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
require 'weakref'

require 'perobs/Cache'
require 'perobs/ClassMap'
require 'perobs/BTreeDB'
require 'perobs/Object'
require 'perobs/Hash'
require 'perobs/Array'

# PErsistent Ruby OBject Store
module PEROBS

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
  #   def initialize(store, name)
  #     super
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

    attr_reader :db, :cache, :class_map, :object_creation_in_progress

    # Create a new Store.
    # @param data_base [String] the name of the database
    # @param options [Hash] various options to affect the operation of the
    #        database. Currently the following options are supported:
    #        :engine     : The class that provides the back-end storage
    #                      engine. By default BTreeDB is used. A user
    #                      can provide it's own storage engine that must
    #                      conform to the same API exposed by BTreeBlobsDB.
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
    #                      like String, Fixnum, Float, Array, Hash. This is
    #                      the default option.
    #                      :yaml : Can also handle most Ruby data types and is
    #                      portable between Ruby versions (1.9 and later).
    #                      Unfortunately, it is 10x slower than marshal.
    def initialize(data_base, options = {})
      # Create a backing store handler
      @db = (options[:engine] || BTreeDB).new(data_base, options)
      # Create a map that can translate classes to numerical IDs and vice
      # versa.
      @class_map = ClassMap.new(@db)
      # This flag is used to check that PEROBS objects are only created via
      # the Store.new() call by PEROBS users.
      @object_creation_in_progress = false

      # List of PEROBS objects that are currently available as Ruby objects
      # hashed by their ID.
      @in_memory_objects = {}

      # The Cache reduces read and write latencies by keeping a subset of the
      # objects in memory.
      @cache = Cache.new(options[:cache_bits] || 16)

      # The named (global) objects IDs hashed by their name
      unless (@root_objects = object_by_id(0))
        # The root object hash always has the object ID 0.
        @root_objects = _construct_po(Hash, 0)
        # The ID change removes it from the write cache. We need to add it
        # again.
        @cache.cache_write(@root_objects)
      end
    end

    # You need to call this method to create new PEROBS objects that belong to
    # this Store.
    # @param klass [Class] The class of the object you want to create. This
    #        must be a derivative of ObjectBase.
    # @param *args Optional list of other arguments that are passed to the
    #        constructor of the specified class.
    # @return [POXReference] A reference to the newly created object.
    def new(klass, *args)
      _construct_po(klass, nil, *args).myself
    end

    # For library internal use only!
    # This method will create a new PEROBS object.
    # @param klass [BasicObject] Class of the object to create
    # @param id [Fixnum, Bignum or nil] Requested object ID or nil
    # @param *args [Array] Arguments to pass to the object constructor.
    # @return [BasicObject] Newly constructed PEROBS object
    def _construct_po(klass, id, *args)
      unless klass.is_a?(BasicObject)
        raise ArgumentError, "#{klass} is not a BasicObject derivative"
      end
      @object_creation_in_progress = true
      obj = klass.new(self, *args)
      @object_creation_in_progress = false
      # If a specific object ID was requested we need to set it now.
      obj._change_id(id) if id
      # Add the new object to the in-memory list. We only store a weak
      # reference to the object so it can be garbage collected. When this
      # happens the object finalizer is triggered and calls _forget() to
      # remove the object from this hash again.
      @in_memory_objects[obj._id] = WeakRef.new(obj)
      obj
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
        raise ArgumentError, 'Object must be of class PEROBS::Object but ' +
                             "is of class #{obj.class}"
      end

      unless obj.store == self
        raise ArgumentError, 'The object does not belong to this store.'
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

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      if @cache.in_transaction?
        raise RuntimeError, 'You cannot call sync during a transaction'
      end
      @cache.flush
    end

    # Discard all objects that are not somehow connected to the root objects
    # from the back-end storage. The garbage collector is not invoked
    # automatically. Depending on your usage pattern, you need to call this
    # method periodically.
    # @return [Fixnum] The number of collected objects
    def gc
      sync
      mark
      sweep
    end

    # Return the object with the provided ID. This method is not part of the
    # public API and should never be called by outside users. It's purely
    # intended for internal use.
    def object_by_id(id)
      if (obj = @in_memory_objects[id])
        # We have the object in memory so we can just return it.
        begin
          return obj.__getobj__
        rescue WeakRef::RefError
          # Due to a race condition the object can still be in the
          # @in_memory_objects list but has been collected already by the Ruby
          # GC. In that case we need to load it again.
        end
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
    # @return [Fixnum] The number of references to bad objects found.
    def check(repair = true)
      # All objects must have in-db version.
      sync
      # Run basic consistency checks first.
      @db.check_db(repair)

      # We will use the mark to mark all objects that we have checked already.
      # Before we start, we need to clear all marks.
      @db.clear_marks

      errors = 0
      @root_objects.each do |name, id|
        errors += check_object(id, repair)
      end
      @root_objects.delete_if { |name, id| !@db.check(id, false) }

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
      stack.each { |id| @db.mark(id) }
      while !stack.empty?
        # Get an object index from the stack.
        obj = object_by_id(id = stack.pop)
        yield(POXReference.new(self, id)) if block_given?
        obj._referenced_object_ids.each do |id|
          unless @db.is_marked?(id)
            @db.mark(id)
            stack << id
          end
        end
      end
    end

    # Rename classes of objects stored in the data base.
    # @param rename_map [Hash] Hash that maps the old name to the new name
    def rename_classes(rename_map)
      @class_map.rename(rename_map)
    end

    # Remove the object from the in-memory list. This is an internal method
    # and should never be called from user code.
    # @param id [Fixnum or Bignum] Object ID of object to remove from the list
    def _collect(id, ignore_errors = false)
      unless ignore_errors || @in_memory_objects.include?(id)
        raise RuntimeError, "Object with id #{id} is currently not in memory"
      end
      @in_memory_objects.delete(id)
    end

    # This method returns a Hash with some statistics about this store.
    def statistics
      {
        :in_memory_objects => @in_memory_objects.length,
        :root_objects => 0 #@root_objects.length
      }
    end

    private

    # Mark phase of a mark-and-sweep garbage collector. It will mark all
    # objects that are reachable from the root objects.
    def mark
      classes = Set.new
      each { |obj| classes.add(obj.class) }
      @class_map.keep(classes.map { |c| c.to_s })
    end

    # Sweep phase of a mark-and-sweep garbage collector. It will remove all
    # unmarked objects from the store.
    def sweep
      cntr = @db.delete_unmarked_objects.length
      @cache.reset
      cntr
    end

    # Check the object with the given start_id and all other objects that are
    # somehow reachable from the start object.
    # @param start_id [Fixnum or Bignum] ID of the top-level object to start
    #        with
    # @param repair [Boolean] Delete refernces to broken objects if true
    # @return [Fixnum] The number of references to bad objects.
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

        if (obj = object_by_id(id)) && @db.check(id, repair)
          # The object exists and is OK. Mark is as checked.
          @db.mark(id)
          # Now look at all other objects referenced by this object.
          obj._referenced_object_ids.each do |refd_id|
            # Push them onto the todo list unless they have been marked
            # already.
            todo_list << [ obj, refd_id ] unless @db.is_marked?(refd_id)
          end
        else
          # Remove references to bad objects.
          ref_obj._delete_reference_to_id(id) if ref_obj && repair
          errors += 1
        end
      end

      errors
    end

  end

end

