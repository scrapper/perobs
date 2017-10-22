# encoding: UTF-8
#
# = Store.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/Log'
require 'perobs/Handle'
require 'perobs/Cache'
require 'perobs/ClassMap'
require 'perobs/BTreeDB'
require 'perobs/FlatFileDB'
require 'perobs/Object'
require 'perobs/Hash'
require 'perobs/Array'

# PErsistent Ruby OBject Store
module PEROBS

  Statistics = Struct.new(:in_memory_objects, :root_objects,
                          :marked_objects, :swept_objects)

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
  #   po_attr :name, :mother, :father, :kids
  #
  #   def initialize(cf, name)
  #     super(cf)
  #     attr_init(:name, name)
  #     attr_init(:kids, @store.new(PEROBS::Array))
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
  # store.sync
  #
  class Store

    attr_reader :db, :cache, :class_map

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
    def initialize(data_base, options = {})
      # Create a backing store handler
      @db = (options[:engine] || BTreeDB).new(data_base, options)
      @db.open
      # Create a map that can translate classes to numerical IDs and vice
      # versa.
      @class_map = ClassMap.new(@db)

      # List of PEROBS objects that are currently available as Ruby objects
      # hashed by their ID.
      @in_memory_objects = {}

      # This objects keeps some counters of interest.
      @stats = Statistics.new

      # The Cache reduces read and write latencies by keeping a subset of the
      # objects in memory.
      @cache = Cache.new(options[:cache_bits] || 16)

      # The named (global) objects IDs hashed by their name
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

    # Copy the store content into a new Store. The arguments are identical to
    # Store.new().
    # @param options [Hash] various options to affect the operation of the
    def copy(dir, options = {})
      # Make sure all objects are persisted.
      sync

      # Create a new store with the specified directory and options.
      new_db = Store.new(dir, options)
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
        PEROBS.log.fatal 'You cannot call exit() during a transaction'
      end
      @cache.flush if @cache
      @db.close if @db
      @db = @class_map = @in_memory_objects = @stats = @cache = @root_objects =
        nil
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

      obj = _construct_po(klass, _new_id, *args)
      # Mark the new object as modified so it gets pushed into the database.
      @cache.cache_write(obj)
      # Return a POXReference proxy for the newly created object.
      obj.myself
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
    # method was called.
    def delete_store
      @db.delete_database
      @db = @class_map = @cache = @root_objects = nil
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

      obj
    end

    # Return the object with the provided name.
    # @param name [Symbol] A Symbol specifies the name of the object to be
    #        returned.
    # @return The requested object or nil if it doesn't exist.
    def [](name)
      # Return nil if there is no object with that name.
      return nil unless (id = @root_objects[name])

      POXReference.new(self, id)
    end

    # Return a list with all the names of the root objects.
    # @return [Array of Symbols]
    def names
      @root_objects.keys
    end

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      if @cache.in_transaction?
        PEROBS.log.fatal 'You cannot call sync() during a transaction'
      end
      @cache.flush
    end

    # Discard all objects that are not somehow connected to the root objects
    # from the back-end storage. The garbage collector is not invoked
    # automatically. Depending on your usage pattern, you need to call this
    # method periodically.
    # @return [Integer] The number of collected objects
    def gc
      if @cache.in_transaction?
        PEROBS.log.fatal 'You cannot call gc() during a transaction'
      end
      sync
      mark
      sweep
    end

    # Return the object with the provided ID. This method is not part of the
    # public API and should never be called by outside users. It's purely
    # intended for internal use.
    def object_by_id(id)
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
          # GC. In that case we need to load it again. In this case the
          # _collect() call will happen much later, potentially after we have
          # registered a new object with the same ID.
          @in_memory_objects.delete(id)
        end
      end

      if (obj = @cache.object_by_id(id))
        PEROBS.log.fatal "Object #{id} with Ruby #{obj.object_id} is in cache but not in_memory"
      end

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

    # This method can be used to check the database and optionally repair it.
    # The repair is a pure structural repair. It cannot ensure that the stored
    # data is still correct. E. g. if a reference to a non-existing or
    # unreadable object is found, the reference will simply be deleted.
    # @param repair [TrueClass/FalseClass] true if a repair attempt should be
    #        made.
    # @return [Integer] The number of references to bad objects found.
    def check(repair = false)
      # All objects must have in-db version.
      sync
      # Run basic consistency checks first.
      errors = @db.check_db(repair)

      # We will use the mark to mark all objects that we have checked already.
      # Before we start, we need to clear all marks.
      @db.clear_marks

      objects = 0
      @root_objects.each do |name, id|
        objects += 1
        errors += check_object(id, repair)
      end

      # Delete all broken root objects.
      if repair
        @root_objects.delete_if do |name, id|
          unless (res = @db.check(id, repair))
            PEROBS.log.error "Discarding broken root object '#{name}' " +
              "with ID #{id}"
            errors += 1
          end
          !res
        end
      end

      if errors > 0
        if repair
          PEROBS.log.error "#{errors} errors found in #{objects} objects"
        else
          PEROBS.log.fatal "#{errors} errors found in #{objects} objects"
        end
      else
        PEROBS.log.debug "No errors found"
      end

      # Ensure that any fixes are written into the DB.
      sync if repair

      errors
    end

    # This method will execute the provided block as an atomic transaction
    # regarding the manipulation of all objects associated with this Store. In
    # case the execution of the block generates an exception, the transaction
    # is aborted and all PEROBS objects are restored to the state at the
    # beginning of the transaction. The exception is passed on to the
    # enclosing scope, so you probably want to handle it accordingly.
    def transaction
      @cache.begin_transaction
      begin
        yield if block_given?
      rescue => e
        @cache.abort_transaction
        raise e
      end
      @cache.end_transaction
    end

    # Calls the given block once for each object, passing that object as a
    # parameter.
    def each
      @db.clear_marks
      # Start with the object 0 and the indexes of the root objects. Push them
      # onto the work stack.
      stack = [ 0 ] + @root_objects.values
      while !stack.empty?
        # Get an object index from the stack.
        unless (obj = object_by_id(id = stack.pop))
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

    # Rename classes of objects stored in the data base.
    # @param rename_map [Hash] Hash that maps the old name to the new name
    def rename_classes(rename_map)
      @class_map.rename(rename_map)
    end

    # Internal method. Don't use this outside of this library!
    # Generate a new unique ID that is not used by any other object. It uses
    # random numbers between 0 and 2**64 - 1.
    # @return [Integer]
    def _new_id
      begin
        # Generate a random number. It's recommended to not store more than
        # 2**62 objects in the same store.
        id = rand(2**64)
        # Ensure that we don't have already another object with this ID.
      end while @in_memory_objects.include?(id) || @db.include?(id)

      id
    end

    # Internal method. Don't use this outside of this library!
    # Add the new object to the in-memory list. We only store a weak
    # reference to the object so it can be garbage collected. When this
    # happens the object finalizer is triggered and calls _forget() to
    # remove the object from this hash again.
    # @param obj [BasicObject] Object to register
    # @param id [Integer] object ID
    def _register_in_memory(obj, id)
      @in_memory_objects[id] = obj.object_id
    end

    # Remove the object from the in-memory list. This is an internal method
    # and should never be called from user code. It will be called from a
    # finalizer, so many restrictions apply!
    # @param id [Integer] Object ID of object to remove from the list
    def _collect(id, ruby_object_id)
      if @in_memory_objects[id] == ruby_object_id
        @in_memory_objects.delete(id)
      end
    end

    # This method returns a Hash with some statistics about this store.
    def statistics
      @stats.in_memory_objects = @in_memory_objects.length
      @stats.root_objects = @root_objects.length

      @stats
    end

    private

    # Mark phase of a mark-and-sweep garbage collector. It will mark all
    # objects that are reachable from the root objects.
    def mark
      classes = Set.new
      marked_objects = 0
      each { |obj| classes.add(obj.class); marked_objects += 1 }
      @class_map.keep(classes.map { |c| c.to_s })

      # The root_objects object is included in the count, but we only want to
      # count user objects here.
      PEROBS.log.debug "#{marked_objects - 1} objects marked"
      @stats.marked_objects = marked_objects - 1
    end

    # Sweep phase of a mark-and-sweep garbage collector. It will remove all
    # unmarked objects from the store.
    def sweep
      @stats.swept_objects = @db.delete_unmarked_objects.length
      @cache.reset
      PEROBS.log.debug "#{@stats.swept_objects} objects collected"
      @stats.swept_objects
    end

    # Check the object with the given start_id and all other objects that are
    # somehow reachable from the start object.
    # @param start_id [Integer] ID of the top-level object to start
    #        with
    # @param repair [Boolean] Delete refernces to broken objects if true
    # @return [Integer] The number of references to bad objects.
    def check_object(start_id, repair)
      errors = 0
      @db.mark(start_id)
      # The todo list holds a touple for each object that still needs to be
      # checked. The first item is the referring object and the second is the
      # ID of the object to check.
      todo_list = [ [ nil, start_id ] ]

      while !todo_list.empty?
        # Get the next PEROBS object to check
        ref_obj, id = todo_list.pop

        if (obj = object_by_id(id))
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
          errors += 1
        end
      end

      errors
    end

  end

end

