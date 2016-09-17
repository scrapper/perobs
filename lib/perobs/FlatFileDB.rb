# encoding: UTF-8
#
# = FlatFileDB.rb -- Persistent Ruby Object Store
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

require 'fileutils'
require 'zlib'

require 'perobs/FlatFile'

module PEROBS

  # The FlatFileDB is a storage backend that uses a single flat file to store
  # the value blobs.
  class FlatFileDB < DataBase

    attr_reader :max_blob_size

    # Create a new FlatFileDB object.
    # @param db_name [String] name of the DB directory
    # @param options [Hash] options to customize the behavior. Currently only
    #        the following options are supported:
    #        :serializer    : Can be :marshal, :json, :yaml
    def initialize(db_name, options = {})
      super(options[:serializer] || :json)

      @db_dir = db_name
      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)

      # Read the existing DB config.
      @config = get_hash('config')
      check_option('serializer')

      put_hash('config', @config)
    end

    # Open the FlatFileDB for transactions.
    def open
      @flat_file = FlatFile.new(@db_dir)
      @flat_file.open
    end

    # Close the FlatFileDB.
    def close
      @flat_file.close
      @flat_file = nil
    end

    # Delete the entire database. The database is no longer usable after this
    # method was called.
    def delete_database
      FileUtils.rm_rf(@db_dir)
    end

    def FlatFileDB::delete_db(db_name)
      FileUtils.rm_rf(db_name)
    end

    # Return true if the object with given ID exists
    # @param id [Fixnum or Bignum]
    def include?(id)
      !@flat_file.find_obj_addr_by_id(id).nil?
    end

    # Store a simple Hash as a JSON encoded file into the DB directory.
    # @param name [String] Name of the hash. Will be used as file name.
    # @param hash [Hash] A Hash that maps String objects to strings or
    # numbers.
    def put_hash(name, hash)
      file_name = File.join(@db_dir, name + '.json')
      begin
        File.write(file_name, hash.to_json)
      rescue => e
        raise RuntimeError,
              "Cannot write hash file '#{file_name}': #{e.message}"
      end
    end

    # Load the Hash with the given name.
    # @param name [String] Name of the hash.
    # @return [Hash] A Hash that maps String objects to strings or numbers.
    def get_hash(name)
      file_name = File.join(@db_dir, name + '.json')
      return ::Hash.new unless File.exist?(file_name)

      begin
        json = File.read(file_name)
      rescue => e
        raise RuntimeError,
              "Cannot read hash file '#{file_name}': #{e.message}"
      end
      JSON.parse(json, :create_additions => true)
    end

    # Store the given object into the cluster files.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      @flat_file.delete_obj_by_id(id)
      @flat_file.write_obj_by_id(id, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Fixnum or Bignum] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase or nil if ID does
    #         not exist
    def get_object(id)
      if (raw_obj = @flat_file.read_obj_by_id(id))
        return deserialize(raw_obj)
      else
        nil
      end
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      @flat_file.clear_all_marks
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    # @return [Array] List of IDs that have been removed from the DB.
    def delete_unmarked_objects
      @flat_file.delete_unmarked_objects
    end

    # Mark an object.
    # @param id [Fixnum or Bignum] ID of the object to mark
    def mark(id)
      @flat_file.mark_obj_by_id(id)
    end

    # Check if the object is marked.
    # @param id [Fixnum or Bignum] ID of the object to check
    # @param ignore_errors [Boolean] If set to true no errors will be raised
    #        for non-existing objects.
    def is_marked?(id, ignore_errors = false)
      @flat_file.is_marked_by_id?(id)
    end

    # Basic consistency check.
    # @param repair [TrueClass/FalseClass] True if found errors should be
    #        repaired.
    def check_db(repair = false)
      @flat_file.check(repair)
    end

    # Check if the stored object is syntactically correct.
    # @param id [Fixnum/Bignum] Object ID
    # @param repair [TrueClass/FalseClass] True if an repair attempt should be
    #        made.
    # @return [TrueClass/FalseClass] True if the object is OK, otherwise
    #         false.
    def check(id, repair)
      begin
        get_object(id)
      rescue => e
        $stderr.puts "Cannot read object with ID #{id}: #{e.message}"
        return false
      end

      true
    end

    # Store the given serialized object into the cluster files. This method is
    # for internal use only!
    # @param raw [String] Serialized Object as defined by PEROBS::ObjectBase
    # @param id [Fixnum or Bignum] Object ID
    def put_raw_object(raw, id)
      @flat_file.delete_obj_(id)
      @flat_file.write_obj_by_id(id, raw)
    end

  end

end

