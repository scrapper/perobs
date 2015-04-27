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

    def initialize(data_base)
      # The name of the data base directory
      @db_dir = data_base
      # The in-memory objects hashed by ID
      @working_set = {}
      # The named (global) objects IDs hashed by their name
      @named_objects = {}
      # Flag that indicates that the named_objects list differs from the
      # on-disk version.
      @named_objects_modified = false
      # The maximum number of objects to store in memory.
      @max_objects = 10000
      # The number of objects to remove from memory when the max_objects limit
      # is reached.
      @flush_count = @max_objects / 10

      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)

      # Read the list of named objects from disk if it exists.
      named_objects_file = File.join(@db_dir, 'named_objects.json')
      if File.exists?(named_objects_file)
        JSON::parse(File.read(named_objects_file)).each do |name, id|
          @named_objects[name.to_sym] = id
        end
      end
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

      # The the ID of the object. If we already have a named object, we reuse
      # the ID. Otherwise, we generate a new one.
      id = @named_objects[name] || new_id

      # If the passed object is nil, we delete the entry if it exists.
      if obj.nil?
        @named_objects.delete(id)
        return nil
      end

      # Register it with the PersistentRubyObjectStore.
      obj.register(self, id)
      # Store the name and mark the name list as modified.
      @named_objects[name] = id
      @named_objects_modified = true
      # Add the object to the in-memory storage list.
      add_to_working_set(obj, id)

      obj
    end

    # Return the object with the provided name or object ID.
    # @param name_or_id [Symbol or Fixnum/Bignum] A Symbol specifies the name
    #        of the object to be returned. A Fixnum or Bignum the object ID.
    # @return The requested object or nil if it doesn't exist.
    def [](name_or_id)
      if name_or_id.is_a?(Symbol)
        # Return nil if there is no object with that name.
        return nil unless @named_objects.include?(name_or_id)

        # Find the object ID.
        id = @named_objects[name_or_id]
      elsif name_or_id.is_a?(Bignum) || name_or_id.is_a?(Fixnum)
        # If the argument is a number it's the object ID.
        id = name_or_id
      else
        raise ArgumentError, "name '#{name_or_id}' must be a Symbol but is a " +
                             "#{name_or_id.class}"
      end

      if @working_set.include?(id)
        # We have the object in memory so we can just return it.
        return @working_set[id]
      else
        # We don't have the object in memory. Let's find it on the disk.
        obj_file = object_file_name(id)
        if File.exists?(obj_file)
          # Great, object found. Read it into memory and return it.
          obj = PersistentObject::read(obj_file, self, id)
          @working_set[id] = obj
          return obj
        end
      end

      # The requested object does not exist. Return nil.
      nil
    end

    # Return the number of in-memory objects. There is no quick way to
    # determine the number of total objects.
    def length
      @working_set.length
    end

    def add_to_working_set(obj, id)
      @working_set[id] = obj
      # If the in-memory list has reached the upper limit, flush out the
      # modified objects to disk and shrink the list.
      sync if @working_set.length > @max_objects
    end

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      # If we have modified the named objects list, write it to disk.
      if @named_objects_modified
        File.write(File.join(@db_dir, 'named_objects.json'),
                   @named_objects.to_json)
        @named_objects_modified = false
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

    # Determine the file name to store the object. The object ID determines
    # the directory and file name inside the store.
    # @param id [Fixnum or Bignum] ID of the object
    def object_file_name(id)
      hex_id = "%08X" % id
      dir = hex_id[0..1]
      ensure_dir_exists(File.join(@db_dir, dir))

      File.join(@db_dir, dir, hex_id + '.json')
    end

    private

    def new_id
      begin
        # Generate a random number. It's recommended to not store more than
        # 2**62 objects in the same store.
        id = rand(2**64)
        # Ensure that we don't have already another object with this ID.
      end while File.exists?(object_file_name(id))

      id
    end

    # Ensure that we have a directory to store the DB items.
    def ensure_dir_exists(dir)
      unless Dir.exists?(dir)
        begin
          Dir.mkdir(dir)
        rescue IOError
          raise IOError, "Cannote create DB directory '#{dir}': #{$!}"
        end
      end
    end

  end

end

