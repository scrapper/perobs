# encoding: UTF-8
#
# = FlatFileDB.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016, 2017, 2018, 2019
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

require 'fileutils'
require 'zlib'

require 'perobs/Log'
require 'perobs/RobustFile'
require 'perobs/DataBase'
require 'perobs/FlatFile'

module PEROBS

  # The FlatFileDB is a storage backend that uses a single flat file to store
  # the value blobs.
  class FlatFileDB < DataBase

    # This version number increases whenever the on-disk format changes in a
    # way that requires conversion actions after an update.
    VERSION = 4

    attr_reader :max_blob_size

    # Create a new FlatFileDB object.
    # @param db_name [String] name of the DB directory
    # @param options [Hash] options to customize the behavior. Currently only
    #        the following options are supported:
    #        :serializer    : Can be :marshal, :json, :yaml
    #        :progressmeter : Reference to a ProgressMeter object
    #        :log           : IO that should be used for logging
    #        :log_level     : Minimum Logger level to log
    def initialize(db_name, options = {})
      super(options)

      @db_dir = db_name
      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)
      PEROBS.log.level = options[:log_level] if options[:log_level]
      PEROBS.log.open(options[:log] || File.join(@db_dir, 'log'))
      check_version_and_upgrade

      # Read the existing DB config.
      @config = get_hash('config')
      check_option('serializer')

      put_hash('config', @config)
    end

    # Open the FlatFileDB for transactions.
    def open
      @flat_file = FlatFile.new(@db_dir, @progressmeter)
      @flat_file.open
      PEROBS.log.info "FlatFile '#{@db_dir}' opened"
    end

    # Close the FlatFileDB.
    def close
      @flat_file.close
      @flat_file = nil
      PEROBS.log.info "FlatFile '#{@db_dir}' closed"
    end

    # Delete the entire database. The database is no longer usable after this
    # method was called.
    def delete_database
      FileUtils.rm_rf(@db_dir)
    end

    def FlatFileDB::delete_db(db_name)
      close
      FileUtils.rm_rf(db_name)
    end

    # Return true if the object with given ID exists
    # @param id [Integer]
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
        RobustFile.write(file_name, hash.to_json)
      rescue IOError => e
        PEROBS.log.fatal "Cannot write hash file '#{file_name}': #{e.message}"
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
        PEROBS.log.fatal "Cannot read hash file '#{file_name}': #{e.message}"
      end
      JSON.parse(json, :create_additions => true)
    end

    # Store the given object into the cluster files.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      @flat_file.write_obj_by_id(id, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Integer] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase or nil if ID does
    #         not exist
    def get_object(id)
      if (raw_obj = @flat_file.read_obj_by_id(id))
        return deserialize(raw_obj)
      else
        nil
      end
    end

    # @return [Integer] Number of objects stored in the DB.
    def item_counter
      @flat_file.item_counter
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      @flat_file.clear_all_marks
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    # @return [Integer] Number of the removed objects from the DB.
    def delete_unmarked_objects(&block)
      @flat_file.delete_unmarked_objects(&block)
    end

    # Mark an object.
    # @param id [Integer] ID of the object to mark
    def mark(id)
      @flat_file.mark_obj_by_id(id)
    end

    # Check if the object is marked.
    # @param id [Integer] ID of the object to check
    # @param ignore_errors [Boolean] If set to true no errors will be raised
    #        for non-existing objects.
    def is_marked?(id, ignore_errors = false)
      @flat_file.is_marked_by_id?(id)
    end

    # Basic consistency check.
    # @param repair [TrueClass/FalseClass] True if found errors should be
    #        repaired.
    # @return number of errors found
    def check_db(repair = false)
      @flat_file.check(repair)
    end

    # Check if the stored object is syntactically correct.
    # @param id [Integer] Object ID
    # @param repair [TrueClass/FalseClass] True if an repair attempt should be
    #        made.
    # @return [TrueClass/FalseClass] True if the object is OK, otherwise
    #         false.
    def check(id, repair)
      begin
        return get_object(id) != nil
      rescue PEROBS::FatalError => e
        PEROBS.log.error "Cannot read object with ID #{id}: #{e.message}"
        if repair
          begin
            PEROBS.log.error "Discarding broken object with ID #{id}"
            @flat_file.delete_obj_by_id(id)
          rescue PEROBS::FatalError
          end
        end
      end

      return false
    end

    # Store the given serialized object into the cluster files. This method is
    # for internal use only!
    # @param raw [String] Serialized Object as defined by PEROBS::ObjectBase
    # @param id [Integer] Object ID
    def put_raw_object(raw, id)
      @flat_file.write_obj_by_id(id, raw)
    end

    private

    def check_version_and_upgrade
      version_file = File.join(@db_dir, 'version')
      version = 1

      if File.exist?(version_file)
        begin
          version = File.read(version_file).to_i
        rescue => e
          PEROBS.log.fatal "Cannot read version number file " +
                           "'#{version_file}': " + e.message
        end
      else
        # The DB is brand new.
        version = VERSION
        write_version_file(version_file)
      end

      if version > VERSION
        PEROBS.log.fatal "Cannot downgrade the FlatFile database from " +
                         "version #{version} to version #{VERSION}"
      end
      if version < 3
        PEROBS.log.fatal "The upgrade of this version of the PEROBS database " +
          "is not supported by this version of PEROBS. Please try an earlier " +
          "version of PEROBS to upgrade the database before using this version."
      end

      # Version upgrades must be done one version number at a time. If the
      # existing DB is multiple versions older than what the current PEROBS
      # version expects than multiple upgrade runs will be needed.
      while version < VERSION
        if version == 3
          PEROBS.log.warn "Updating FlatFileDB #{@db_dir} from version 3 to " +
            "version 4 ..."
          # Version 4 adds checksums for blob file headers. We have to convert
          # the blob file to include the checksums.
          FlatFile.insert_header_checksums(@db_dir)
          open
          @flat_file.regenerate_index_and_spaces
          close
        end

        # After a successful upgrade change the version number in the DB as
        # well.
        write_version_file(version_file)
        PEROBS.log.warn "Update of FlatFileDB '#{@db_dir}' from version " +
          "#{version} to version #{version + 1} completed"

        # Update version variable to new version.
        version += 1
      end
    end

    def write_version_file(version_file)

      begin
        RobustFile.write(version_file, VERSION)
      rescue IOError => e
        PEROBS.log.fatal "Cannot write version number file " +
                         "'#{version_file}': " + e.message
      end
    end

  end

end

