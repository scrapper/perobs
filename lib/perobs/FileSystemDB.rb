require 'time'

require 'perobs/PersistentObject'

module PEROBS

  # This class provides a filesytem based database store for objects.
  class FileSystemDB

    @@RootObjectsFile = 'root_objects.json'

    # Create a new FileSystemDB object. This will create a DB with the given
    # name. A database will live in a directory of that name.
    # @param db_name [String] name of the DB directory
    def initialize(db_name)
      @db_dir = db_name

      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)
    end

    # Return true if the object with given ID exists
    # @param id [Fixnum or Bignum]
    def include?(id)
      File.exists?(object_file_name(id))
    end

    # Store the given object into the filesystem.
    # @param obj [Hash] Object as defined by PersistentObject
    def put_object(obj, id)
      File.write(object_file_name(id), obj.to_json)
    end

    # Load the given object from the filesystem.
    # @param id [Fixnum or Bignum] object ID
    # @return [Hash] Object as defined by PersistentObject
    def get_object(id)
      obj = JSON.parse(File.read(object_file_name(id)))
    end

    # Write the list of root objects to the filesytem.
    # @param objects [Hash] Objects IDs hashed by their name [Symbol]
    def put_root_objects(objects)
      File.write(File.join(@db_dir, @@RootObjectsFile), objects.to_json)
    end

    # Read the list of named objects from the filesystem if it exists.
    # @return [Hash] Objects IDs hashed by their name [Symbol]
    def get_root_objects
      root_objects = {}

      root_objects_file = File.join(@db_dir, @@RootObjectsFile)
      if File.exists?(root_objects_file)
        JSON::parse(File.read(root_objects_file)).each do |name, id|
          root_objects[name.to_sym] = id
        end
      end

      root_objects
    end

    # Generate a new unique ID.
    # @return [Fixnum or Bignum]
    def new_id
      begin
        # Generate a random number. It's recommended to not store more than
        # 2**62 objects in the same store.
        id = rand(2**64)
        # Ensure that we don't have already another object with this ID.
      end while include?(id)

      id
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      @mark_start = Time.now
      # The filesystem stores access times with second granularity. We need to
      # wait 1 sec. to ensure that all marks are noticeable.
      sleep(1)
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    def delete_unmarked_objects
      Dir.glob(File.join(@db_dir, '*')) do |dir|
        next unless Dir.exists?(dir)

        Dir.glob(File.join(dir, '*')) do |file|
          if File.atime(file) <= @mark_start
            File.delete(file)
          end
        end
      end
    end

    def mark(id)
      File.read(object_file_name(id))
    end

    def is_marked?(id)
      File.atime(object_file_name(id)) > @mark_start
    end

    private

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

    # Determine the file name to store the object. The object ID determines
    # the directory and file name inside the store.
    # @param id [Fixnum or Bignum] ID of the object
    def object_file_name(id)
      hex_id = "%08X" % id
      dir = hex_id[0..1]
      ensure_dir_exists(File.join(@db_dir, dir))

      File.join(@db_dir, dir, hex_id + '.json')
    end

  end

end

