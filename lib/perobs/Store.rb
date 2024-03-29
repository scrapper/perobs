# encoding: UTF-8
#
# = Store.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022
# by Chris Schlaeger <chris@taskjuggler.org>
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'set'
require 'monitor'

require 'perobs/Log'
require 'perobs/Handle'
require 'perobs/Cache'
require 'perobs/ClassMap'
require 'perobs/BTreeDB'
require 'perobs/FlatFileDB'
require 'perobs/Object'
require 'perobs/Hash'
require 'perobs/Array'
require 'perobs/BigTree'
require 'perobs/BigHash'
require 'perobs/BigArray'
require 'perobs/ProgressMeter'
require 'perobs/ConsoleProgressMeter'

# PErsistent Ruby OBject Store
module PEROBS

  Statistics = Struct.new(:in_memory_objects, :root_objects,
                          :marked_objects, :swept_objects,
                          :created_objects, :collected_objects)

  # PEROBS::Store is a persistent storage system for Ruby objects. Regular
  # Ruby objects are transparently stored in a back-end storage and retrieved
  # when needed. It features a garbage collector that removes all objects that
  # are no longer in use. A build-in cache keeps access latencies to recently
  # used objects low and lazily flushes modified objects into the persistend
  # back-end. The default back-end is a filesystem based database.
  # Alternatively, an Amazon DynamoDB can be used as well. Adding support for
  # other key/value stores is fairly trivial to do. See PEROBS::DynamoDB for
  # an example
  #
  # Persistent objects must be defined by deriving your class from
  # PEROBS::Object, PERBOS::Array or PEROBS::Hash. Only instance variables
  # that are declared via po_attr will be persistent. It is recommended that
  # references to other objects are all going to persistent objects again. TO
  # create a new persistent object you must call Store.new(). Don't use the
  # constructors of persistent classes directly. Store.new() will return a
  # proxy or delegator object that can be used like the actual object. By
  # using delegators we can disconnect the actual object from the delegator
  # handle.
  #
  # require 'perobs'
  #
  # class Person < PEROBS::Object
  #
  #   attr_persist :name, :mother, :father, :kids
  #
  #   # The contructor is only called for the creation of a new object. It is
  #   # not called when the object is restored from the database. In that case
  #   # only restore() is called.
  #   def initialize(cf, name)
  #     super(cf)
  #     self.name = name
  #     self.kids = @store.new(PEROBS::Array)
  #   end
  #
  #   def restore
  #     # In case you need to do any checks or massaging (e. g. for additional
  #     # attributes) you can provide this method.
  #   end
  #
  #   def to_s
  #     "#{@name} is the child of #{self.mother ? self.mother.name : 'unknown'} " +
  #     "and #{self.father ? self.father.name : 'unknown'}.
  #   end
  #
  # end
  #
  # store = PEROBS::Store.new('family')
  # store['grandpa'] = joe = store.new(Person, 'Joe')
  # store['grandma'] = jane = store.new(Person, 'Jane')
  # jim = store.new(Person, 'Jim')
  # jim.father = joe
  # joe.kids << jim
  # jim.mother = jane
  # jane.kids << jim
  # store.exit
  #
  class Store

    attr_reader :db, :cache, :class_map
    attr_writer :root_objects

    # Create a new Store.
    # @param data_base [String] the name of the database
    # @param options [Hash] various options to affect the operation of the
    #        database. Currently the following options are supported:
    #        :engine     : The class that provides the back-end storage
    #                      engine. By default FlatFileDB is used. A user
    #                      can provide it's own storage engine that must
    #                      conform to the same API exposed by FlatFileDB.
    #        :cache_bits : the number of bits used for cache indexing. The
    #                      cache will hold 2 to the power of bits number of
    #                      objects. We have separate caches for reading and
    #                      writing. The default value is 16. It probably makes
    #                      little sense to use much larger numbers than that.
    #        :serializer : select the format used to serialize the data. There
    #                      are 3 different options:
    #                      :marshal : Native Ruby serializer. Fastest option
    #                      that can handle most Ruby data types. Big
    #                      disadvantate is the stability of the format. Data
    #                      written with one Ruby version may not be readable
    #                      with another version.
    #                      :json : About half as fast as marshal, but the
    #                      format is rock solid and portable between
    #                      languages. It only supports basic Ruby data types
    #                      like String, Integer, Float, Array, Hash. This is
    #                      the default option.
    #                      :yaml : Can also handle most Ruby data types and is
    #                      portable between Ruby versions (1.9 and later).
    #                      Unfortunately, it is 10x slower than marshal.
    #     :progressmeter : reference to a ProgressMeter object that receives
    #                      progress information during longer running tasks.
    #                      It defaults to ProgressMeter which only logs into
    #                      the log. Use ConsoleProgressMeter or a derived
    #                      class for more fancy progress reporting.
    #   :no_root_objects : Create a new store without root objects. This only
    #                      makes sense if you want to copy the objects of
    #                      another store into this store.
    def initialize(data_base, options = {})
      # Create a backing store handler
      @progressmeter = (options[:progressmeter] ||= ProgressMeter.new)
      @db = (options[:engine] || FlatFileDB).new(data_base, options)
      @db.open
      # Create a map that can translate classes to numerical IDs and vice
      # versa.
      @class_map = ClassMap.new(@db)
      @db.register_class_map(@class_map)

      # List of PEROBS objects that are currently available as Ruby objects
      # hashed by their ID.
      @in_memory_objects = {}

      # This objects keeps some counters of interest.
      @stats = Statistics.new
      @stats[:created_objects] = 0
      @stats[:collected_objects] = 0

      # The Cache reduces read and write latencies by keeping a subset of the
      # objects in memory.
      @cache = Cache.new(options[:cache_bits] || 16)

      # Lock to serialize access to the Store and all stored data.
      @lock = Monitor.new

      # The named (global) objects IDs hashed by their name
      unless options[:no_root_objects]
        unless (@root_objects = object_by_id(0))
          PEROBS.log.debug "Initializing the PEROBS store"
          # The root object hash always has the object ID 0.
          @root_objects = _construct_po(Hash, 0)
          # Mark the root_objects object as modified.
          @cache.cache_write(@root_objects)
        end
        unless @root_objects.is_a?(Hash)
          PEROBS.log.fatal "Database corrupted: Root objects must be a Hash " +
            "but is a #{@root_objects.class}"
        end
      end
    end

    # Copy the store content into a new Store. The arguments are identical to
    # Store.new().
    # @param options [Hash] various options to affect the operation of the
    def copy(dir, options = {})
      # Make sure all objects are persisted.
      sync

      # Create a new store with the specified directory and options.
      new_options = options.clone
      new_options[:no_root_objects] = true
      new_db = Store.new(dir, new_options)
      # Clear the cache.
      new_db.sync
      # Copy all objects of the existing store to the new store.
      i = 0
      each do |ref_obj|
        obj = ref_obj._referenced_object
        obj._transfer(new_db)
        obj._sync
        i += 1
      end
      new_db.root_objects = new_db.object_by_id(0)
      PEROBS.log.debug "Copied #{i} objects into new database at #{dir}"
      # Flush the new store and close it.
      new_db.exit

      true
    end

    # Close the store and ensure that all in-memory objects are written out to
    # the storage backend. The Store object is no longer usable after this
    # method was called.
    def exit
      if @cache && @cache.in_transaction?
        @cache.abort_transaction
        @cache.flush
        @db.close if @db
        PEROBS.log.fatal "You cannot call exit() during a transaction: #{Kernel.caller}"
      end
      @cache.flush if @cache
      @db.close if @db

      GC.start
      if @stats
        unless @stats[:created_objects] == @stats[:collected_objects] +
            @in_memory_objects.length
          PEROGS.log.fatal "Created objects count " +
            "(#{@stats[:created_objects]})" +
            " is not equal to the collected count " +
            "(#{@stats[:collected_objects]}) + in_memory_objects count " +
            "(#{@in_memory_objects.length})"
        end
      end

      @db = @class_map = @in_memory_objects = @stats = @cache =
        @root_objects = nil
    end

    # You need to call this method to create new PEROBS objects that belong to
    # this Store.
    # @param klass [Class] The class of the object you want to create. This
    #        must be a derivative of ObjectBase.
    # @param args Optional list of other arguments that are passed to the
    #        constructor of the specified class.
    # @return [POXReference] A reference to the newly created object.
    def new(klass, *args)
      unless klass.is_a?(BasicObject)
        PEROBS.log.fatal "#{klass} is not a BasicObject derivative"
      end

      @lock.synchronize do
        obj = _construct_po(klass, _new_id, *args)
        # Mark the new object as modified so it gets pushed into the database.
        @cache.cache_write(obj)
        # Return a POXReference proxy for the newly created object.
        obj.myself
      end
    end

    # For library internal use only!
    # This method will create a new PEROBS object.
    # @param klass [BasicObject] Class of the object to create
    # @param id [Integer] Requested object ID
    # @param args [Array] Arguments to pass to the object constructor.
    # @return [BasicObject] Newly constructed PEROBS object
    def _construct_po(klass, id, *args)
      klass.new(Handle.new(self, id), *args)
    end

    # Delete the entire store. The store is no longer usable after this
    # method was called. This is an alternative to exit() that additionaly
    # deletes the entire database.
    def delete_store
      @lock.synchronize do
        @db.delete_database
        @db = @class_map = @in_memory_objects = @stats = @cache =
          @root_objects = nil
      end
    end

    # Store the provided object under the given name. Use this to make the
    # object a root or top-level object (think global variable). Each store
    # should have at least one root object. Objects that are not directly or
    # indirectly reachable via any of the root objects are no longer
    # accessible and will be garbage collected.
    # @param name [Symbol] The name to use.
    # @param obj [PEROBS::Object] The object to store
    # @return [PEROBS::Object] The stored object.
    def []=(name, obj)
      @lock.synchronize do
        # If the passed object is nil, we delete the entry if it exists.
        if obj.nil?
          @root_objects.delete(name)
          return nil
        end

        # We only allow derivatives of PEROBS::Object to be stored in the
        # store.
        unless obj.is_a?(ObjectBase)
          PEROBS.log.fatal 'Object must be of class PEROBS::Object but ' +
            "is of class #{obj.class}"
        end

        unless obj.store == self
          PEROBS.log.fatal 'The object does not belong to this store.'
        end

        # Store the name and mark the name list as modified.
        @root_objects[name] = obj._id
      end

      obj
    end

    # Return the object with the provided name.
    # @param name [Symbol] A Symbol specifies the name of the object to be
    #        returned.
    # @return The requested object or nil if it doesn't exist.
    def [](name)
      @lock.synchronize do
        # Return nil if there is no object with that name.
        return nil unless (id = @root_objects[name])

        POXReference.new(self, id)
      end
    end

    # Return a list with all the names of the root objects.
    # @return [Array of Symbols]
    def names
      @lock.synchronize do
        @root_objects.keys
      end
    end

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      @lock.synchronize do
        if @cache.in_transaction?
          @cache.abort_transaction
          @cache.flush
          PEROBS.log.fatal "You cannot call sync() during a transaction: \n" +
            Kernel.caller.join("\n")
        end
        @cache.flush
      end
    end

    # Return the number of object stored in the store. CAVEAT: This method
    # will only return correct values when it is separated from any mutating
    # call by a call to sync().
    # @return [Integer] Number of persistently stored objects in the Store.
    def size
      # We don't include the Hash that stores the root objects into the object
      # count.
      @lock.synchronize do
        @db.item_counter - 1
      end
    end

    # Discard all objects that are not somehow connected to the root objects
    # from the back-end storage. The garbage collector is not invoked
    # automatically. Depending on your usage pattern, you need to call this
    # method periodically.
    # @return [Integer] The number of collected objects
    def gc
      @lock.synchronize do
        sync
        mark
        sweep
      end
    end

    # Return the object with the provided ID. This method is not part of the
    # public API and should never be called by outside users. It's purely
    # intended for internal use.
    def object_by_id(id)
      @lock.synchronize do
        object_by_id_internal(id)
      end
    end

    # This method can be used to check the database and optionally repair it.
    # The repair is a pure structural repair. It cannot ensure that the stored
    # data is still correct. E. g. if a reference to a non-existing or
    # unreadable object is found, the reference will simply be deleted.
    # @param repair [TrueClass/FalseClass] true if a repair attempt should be
    #        made.
    # @return [Integer] The number of references to bad objects found.
    def check(repair = false)
      stats = { :errors => 0, :object_cnt => 0 }

      # All objects must have in-db version.
      sync
      # Run basic consistency checks first.
      stats[:errors] += @db.check_db(repair)

      # We will use the mark to mark all objects that we have checked already.
      # Before we start, we need to clear all marks.
      @db.clear_marks

      @progressmeter.start("Checking object link structure",
                           @db.item_counter) do
        @root_objects.each do |name, id|
          check_object(id, repair, stats)
        end
      end

      # Delete all broken root objects.
      if repair
        @root_objects.delete_if do |name, id|
          unless @db.check(id, repair)
            PEROBS.log.error "Discarding broken root object '#{name}' " +
              "with ID #{id}"
            stats[:errors] += 1
          end
        end
      end

      if stats[:errors] > 0
        if repair
          PEROBS.log.error "#{stats[:errors]} errors found in " +
            "#{stats[:object_cnt]} objects"
        else
          PEROBS.log.fatal "#{stats[:errors]} errors found in " +
            "#{stats[:object_cnt]} objects"
        end
      else
        PEROBS.log.debug "No errors found"
      end

      # Ensure that any fixes are written into the DB.
      sync if repair

      stats[:errors]
    end

    # This method will execute the provided block as an atomic transaction
    # regarding the manipulation of all objects associated with this Store. In
    # case the execution of the block generates an exception, the transaction
    # is aborted and all PEROBS objects are restored to the state at the
    # beginning of the transaction. The exception is passed on to the
    # enclosing scope, so you probably want to handle it accordingly.
    def transaction
      transaction_not_started = true
      while transaction_not_started do
        begin
          @lock.synchronize do
            @cache.begin_transaction
            # If we get to this point, the transaction was successfully
            # started. We can exit the loop.
            transaction_not_started = false
          end
        rescue TransactionInOtherThread
          # sleep up to 50ms
          sleep(rand(50) / 1000.0)
        end
      end

      begin
        yield if block_given?
      rescue => e
        @lock.synchronize { @cache.abort_transaction }
        raise e
      end
      @lock.synchronize { @cache.end_transaction }
    end

    # Calls the given block once for each object, passing that object as a
    # parameter.
    def each
      @lock.synchronize do
        @db.clear_marks
        # Start with the object 0 and the indexes of the root objects. Push them
        # onto the work stack.
        stack = [ 0 ] + @root_objects.values
        while !stack.empty?
          # Get an object index from the stack.
          id = stack.pop
          next if @db.is_marked?(id)

          unless (obj = object_by_id_internal(id))
            PEROBS.log.fatal "Database is corrupted. Object with ID #{id} " +
              "not found."
          end
          # Mark the object so it will never be pushed to the stack again.
          @db.mark(id)
          yield(obj.myself) if block_given?
          # Push the IDs of all unmarked referenced objects onto the stack
          obj._referenced_object_ids.each do |r_id|
            stack << r_id unless @db.is_marked?(r_id)
          end
        end
      end
    end

    # Rename classes of objects stored in the data base.
    # @param rename_map [Hash] Hash that maps the old name to the new name
    def rename_classes(rename_map)
      @lock.synchronize { @class_map.rename(rename_map) }
    end

    # Internal method. Don't use this outside of this library!
    # Generate a new unique ID that is not used by any other object. It uses
    # random numbers between 0 and 2**64 - 1.
    # @return [Integer]
    def _new_id
      @lock.synchronize do
        begin
          # Generate a random number. It's recommended to not store more than
          # 2**62 objects in the same store.
          id = rand(2**64)
          # Ensure that we don't have already another object with this ID.
        end while @in_memory_objects.include?(id) || @db.include?(id)

        id
      end
    end

    # Internal method. Don't use this outside of this library!
    # Add the new object to the in-memory list. We only store a weak
    # reference to the object so it can be garbage collected. When this
    # happens the object finalizer is triggered and calls _forget() to
    # remove the object from this hash again.
    # @param obj [BasicObject] Object to register
    # @param id [Integer] object ID
    def _register_in_memory(obj, id)
      @lock.synchronize do
        unless obj.is_a?(ObjectBase)
          PEROBS.log.fatal "You can only register ObjectBase objects"
        end
        if @in_memory_objects.include?(id)
          PEROBS.log.fatal "The Store::_in_memory_objects list already " +
            "contains an object for ID #{id}"
        end

        @in_memory_objects[id] = obj.object_id
        @stats[:created_objects] += 1
      end
    end

    # Remove the object from the in-memory list. This is an internal method
    # and should never be called from user code. It will be called from a
    # finalizer, so many restrictions apply!
    # @param id [Integer] Object ID of object to remove from the list
    def _collect(id, ruby_object_id)
      # This method should only be called from the Ruby garbage collector.
      # Therefor no locking is needed or even possible. The GC can kick in at
      # any time and we could be anywhere in the code. So there is a small
      # risk for a race here, but it should not have any serious consequences.
      if @in_memory_objects && @in_memory_objects[id] == ruby_object_id
        @in_memory_objects.delete(id)
        @stats[:collected_objects] += 1
      end
    end

    # This method returns a Hash with some statistics about this store.
    def statistics
      @lock.synchronize do
        @stats.in_memory_objects = @in_memory_objects.length
        @stats.root_objects = @root_objects.length
      end

      @stats
    end

    private

    def object_by_id_internal(id)
      if (ruby_object_id = @in_memory_objects[id])
        # We have the object in memory so we can just return it.
        begin
          object = ObjectSpace._id2ref(ruby_object_id)
          # Let's make sure the object is really the object we are looking
          # for. The GC might have recycled it already and the Ruby object ID
          # could now be used for another object.
          if object.is_a?(ObjectBase) && object._id == id
            return object
          end
        rescue RangeError => e
          # Due to a race condition the object can still be in the
          # @in_memory_objects list but has been collected already by the Ruby
          # GC. The _collect() call has not been completed yet. We now have to
          # wait until this has been done. I think the GC lock will prevent a
          # race on @in_memory_objects.
          GC.start
          while @in_memory_objects.include?(id)
            sleep 0.01
          end
        end
      end

      # This is just a safety check. It has never triggered, so we can disable
      # it for now.
      #if (obj = @cache.object_by_id(id))
      #  PEROBS.log.fatal "Object #{id} with Ruby #{obj.object_id} is in " +
      #    "cache but not in_memory"
      #end

      # We don't have the object in memory. Let's find it in the storage.
      if @db.include?(id)
        # Great, object found. Read it into memory and return it.
        obj = ObjectBase::read(self, id)
        # Add the object to the in-memory storage list.
        @cache.cache_read(obj)

        return obj
      end

      # The requested object does not exist. Return nil.
      nil
    end

    # Mark phase of a mark-and-sweep garbage collector. It will mark all
    # objects that are reachable from the root objects.
    def mark
      classes = Set.new
      marked_objects = 0
      @progressmeter.start("Marking linked objects", @db.item_counter) do
        each do |obj|
          classes.add(obj.class)
          @progressmeter.update(marked_objects += 1)
        end
      end
      @class_map.keep(classes.map { |c| c.to_s })

      # The root_objects object is included in the count, but we only want to
      # count user objects here.
      PEROBS.log.debug "#{marked_objects - 1} of #{@db.item_counter} " +
        "objects marked"
      @stats.marked_objects = marked_objects - 1
    end

    # Sweep phase of a mark-and-sweep garbage collector. It will remove all
    # unmarked objects from the store.
    def sweep
      @stats.swept_objects = @db.delete_unmarked_objects do |id|
        @cache.evict(id)
      end
      @db.clear_marks
      GC.start
      PEROBS.log.debug "#{@stats.swept_objects} objects collected"
      @stats.swept_objects
    end

    # Check the object with the given start_id and all other objects that are
    # somehow reachable from the start object.
    # @param start_id [Integer] ID of the top-level object to start
    #        with
    # @param repair [Boolean] Delete refernces to broken objects if true
    # @return [Integer] The number of references to bad objects.
    def check_object(start_id, repair, stats)
      @db.mark(start_id)
      # The todo list holds a touple for each object that still needs to be
      # checked. The first item is the referring object and the second is the
      # ID of the object to check.
      todo_list = [ [ nil, start_id ] ]

      while !todo_list.empty?
        # Get the next PEROBS object to check
        ref_obj, id = todo_list.pop

        begin
          obj = object_by_id(id)
        rescue PEROBS::FatalError
          obj = nil
        end

        if obj
          # The object exists and is OK. Mark is as checked.
          @db.mark(id)
          # Now look at all other objects referenced by this object.
          obj._referenced_object_ids.each do |refd_id|
            # Push them onto the todo list unless they have been marked
            # already.
            todo_list << [ obj, refd_id ] unless @db.is_marked?(refd_id, true)
          end
        else
          # Remove references to bad objects.
          if ref_obj
            if repair
              PEROBS.log.error "Removing reference to " +
                "#{obj ? 'broken' : 'non-existing'} object #{id} from:\n" +
                ref_obj.inspect
              ref_obj._delete_reference_to_id(id)
            else
              PEROBS.log.error "The following object references a " +
                "#{obj ? 'broken' : 'non-existing'} object #{id}:\n" +
                ref_obj.inspect
            end
          end
          stats[:errors] += 1
        end

        @progressmeter.update(stats[:object_cnt] += 1)
      end
    end

  end

end
