require 'perobs/FileSystemDB'
require 'perobs/PersistentObject'

module PEROBS

  # Store is a simple database that keeps objects in memory and flushes them
  # out to disk when asked by the user. It also limits the number of in-memory
  # objects to avoid exponential slowdown due to the Ruby garbage collector or
  # when dealing with more objects than would fit into the memory. The main
  # benefit of PEROBS is the very Ruby-like representation of the stored
  # objects. With very few restrictions they can be used just like any other
  # instance of ordinary Ruby classes.
  class Store

    attr_accessor :max_objects, :flush_count
    attr_reader :db

    def initialize(data_base)
      # Create a backing store handler
      @db = FileSystemDB.new(data_base)

      # The in-memory objects hashed by their ID
      @working_set = {}
      # The named (global) objects IDs hashed by their name
      @root_objects = {}
      # Flag that indicates that the root_object list differs from the
      # on-disk version.
      @root_objects_modified = false
      # The maximum number of objects to store in memory.
      @max_objects = 10000
      # The number of objects to remove from memory when the max_objects limit
      # is reached.
      @flush_count = @max_objects / 10

      @root_objects = @db.get_root_objects
    end

    # Store the provided object under the given name.
    # @param name [Symbol] The name to use.
    # @param obj [PersistentObject] The object to store
    # @return [PersistentObject] The stored object.
    def []=(name, obj)
      unless name.is_a?(Symbol)
        raise ArgumentError, "name '#{name}' must be a Symbol but is a " +
                             "#{name.class}"
      end

      # If the passed object is nil, we delete the entry if it exists.
      if obj.nil?
        @root_objects.delete(name)
        return nil
      end

      # We only allow derivatives of PersistentObject to be stored in the
      # store.
      unless obj.is_a?(PersistentObject)
        raise ArgumentError, "Object must be of class PersistentObject but "
                             "is of class #{obj.class}"
      end

      # Register it with the PersistentRubyObjectStore.
      obj.register(self)
      # Store the name and mark the name list as modified.
      @root_objects[name] = obj.id
      @root_objects_modified = true
      # Add the object to the in-memory storage list.
      add_to_working_set(obj)

      obj
    end

    # Return the object with the provided name.
    # @param name [Symbol] A Symbol specifies the name of the object to be
    #        returned.
    # @return The requested object or nil if it doesn't exist.
    def [](name)
      if name.is_a?(Symbol)
        # Return nil if there is no object with that name.
        return nil unless @root_objects.include?(name)

        # Find the object ID.
        id = @root_objects[name]
      else
        raise ArgumentError, "name '#{name_or_id}' must be a Symbol but is a " +
                             "#{name_or_id.class}"
      end

      object_by_id(id)
    end

    # Return the number of in-memory objects. There is no quick way to
    # determine the number of total objects.
    def length
      @working_set.length
    end

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      # If we have modified the named objects list, write it to disk.
      if @root_objects_modified
        @db.put_root_objects(@root_objects)
        @root_objects_modified = false
      end

      # Write all modified objects to disk
      @working_set.each { |id, obj| obj.sync }

      # Check if we've reached the defined threshold of in-memory objects and
      # delete the @flush_count least recently used ones from memory.
      if @working_set.length > @max_objects
        # Create a Array of PersistentObject items sorted by access time.
        objects_by_atime = @working_set.values.sort do |o1, o2|
          o1.access_time <=> o2.access_time
        end
        # Delete the least recently used objects from @working_set.
        objects_by_atime[0..@flush_count].each do |o|
          @working_set.delete(o.id)
        end
      end
    end

    # Discard all objects that are not somehow connected to the root objects
    # from the database.
    def gc
      sync
      mark
      sweep
    end

    # Return the object with the provided ID. This method is not part of the
    # public API and should never be called by outside users. It's purely
    # intended for internal use.
    def object_by_id(id)
      if @working_set.include?(id)
        # We have the object in memory so we can just return it.
        return @working_set[id]
      else
        # We don't have the object in memory. Let's find it in the storage.
        if @db.include?(id)
          # Great, object found. Read it into memory and return it.
          obj = PersistentObject::read(self, id)
          # Add the object to the in-memory storage list.
          add_to_working_set(obj)

          return obj
        end
      end

      # The requested object does not exist. Return nil.
      nil
    end

    # Ensure that a specific object is part of the working set. This method is
    # only intended for internal use. It should never be used the the users of
    # this library.
    # @param obj [PersistentObject] The object to include
    def add_to_working_set(obj)
      @working_set[obj.id] = obj
      # If the in-memory list has reached the upper limit, flush out the
      # modified objects to disk and shrink the list.
      sync if @working_set.length > @max_objects
    end

    def mark
      @db.clear_marks
      stack = @root_objects.values
      while !stack.empty?
        id = stack.pop
        unless @db.is_marked?(id)
          @db.mark(id)
          obj = object_by_id(id)
          stack += obj.referenced_object_ids
        end
      end
    end

    def sweep
      @db.delete_unmarked_objects
    end

  end

end

