# encoding: UTF-8
#
# = BTreeDB.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'
require 'perobs/RobustFile'
require 'perobs/DataBase'
require 'perobs/BTreeBlob'

module PEROBS

  # This class implements a BTree database using filesystem directories as
  # nodes and blob files as leafs. The BTree grows with the number of stored
  # entries. Each leaf node blob can hold a fixed number of entries. If more
  # entries need to be stored, the blob is replaced by a node with multiple
  # new leafs that store the entries of the previous node. The leafs are
  # implemented by the BTreeBlob class.
  class BTreeDB < DataBase

    attr_reader :max_blob_size

    # Create a new BTreeDB object.
    # @param db_name [String] name of the DB directory
    # @param options [Hash] options to customize the behavior. Currently only
    #        the following options are supported:
    #        :serializer    : Can be :marshal, :json, :yaml
    #        :dir_bits      : The number of bits to use for the BTree nodes.
    #                         The value must be between 4 and 14. The larger
    #                         the number the more back-end directories are
    #                         being used. The default is 12 which results in
    #                         4096 directories per node.
    #        :max_blob_size : The maximum number of entries in the BTree leaf
    #                         nodes. The insert/find/delete time grows
    #                         linearly with the size.
    def initialize(db_name, options = {})
      super(options[:serializer] || :json)

      @db_dir = db_name
      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)

      # Read the existing DB config.
      @config = get_hash('config')
      check_option('serializer')

      # Check and set @dir_bits, the number of bits used for each tree level.
      @dir_bits = options[:dir_bits] || 12
      if @dir_bits < 4 || @dir_bits > 14
        PEROBS.log.fatal "dir_bits option (#{@dir_bits}) must be between 4 " +
          "and 12"
      end
      check_option('dir_bits')

      @max_blob_size = options[:max_blob_size] || 32
      if @max_blob_size < 4 || @max_blob_size > 128
        PEROBS.log.fatal "max_blob_size option (#{@max_blob_size}) must be " +
          "between 4 and 128"
      end
      check_option('max_blob_size')

      put_hash('config', @config)

      # This format string is used to create the directory name.
      @dir_format_string = "%0#{(@dir_bits / 4) +
                                (@dir_bits % 4 == 0 ? 0 : 1)}X"
      # Bit mask to extract the dir_bits LSBs.
      @dir_mask = 2 ** @dir_bits - 1
    end

    # Delete the entire database. The database is no longer usable after this
    # method was called.
    def delete_database
      FileUtils.rm_rf(@db_dir)
    end

    def BTreeDB::delete_db(db_name)
      FileUtils.rm_rf(db_name)
    end

    # Return true if the object with given ID exists
    # @param id [Integer]
    def include?(id)
      !(blob = find_blob(id)).nil? && !blob.find(id).nil?
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
      find_blob(id, true).write_object(id, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Integer] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase or nil if ID does
    #         not exist
    def get_object(id)
      return nil unless (blob = find_blob(id)) && (obj = blob.read_object(id))
      deserialize(obj)
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      each_blob { |blob| blob.clear_marks }
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    # @return [Array] List of IDs that have been removed from the DB.
    def delete_unmarked_objects
      deleted_ids = []
      each_blob { |blob| deleted_ids += blob.delete_unmarked_entries }
      deleted_ids
    end

    # Mark an object.
    # @param id [Integer] ID of the object to mark
    def mark(id)
      (blob = find_blob(id)) && blob.mark(id)
    end

    # Check if the object is marked.
    # @param id [Integer] ID of the object to check
    # @param ignore_errors [Boolean] If set to true no errors will be raised
    #        for non-existing objects.
    def is_marked?(id, ignore_errors = false)
      (blob = find_blob(id)) && blob.is_marked?(id, ignore_errors)
    end

    # Basic consistency check.
    # @param repair [TrueClass/FalseClass] True if found errors should be
    #        repaired.
    def check_db(repair = false)
      each_blob { |blob| blob.check(repair) }
    end

    # Check if the stored object is syntactically correct.
    # @param id [Integer] Object ID
    # @param repair [TrueClass/FalseClass] True if an repair attempt should be
    #        made.
    # @return [TrueClass/FalseClass] True if the object is OK, otherwise
    #         false.
    def check(id, repair)
      begin
        get_object(id)
      rescue => e
        PEROBS.log.error "Cannot read object with ID #{id}: #{e.message}"
        return false
      end

      true
    end

    # Store the given serialized object into the cluster files. This method is
    # for internal use only!
    # @param raw [String] Serialized Object as defined by PEROBS::ObjectBase
    # @param id [Integer] Object ID
    def put_raw_object(raw, id)
      find_blob(id, true).write_object(id, raw)
    end

    private

    def find_blob(id, create_missing_blob = false, dir_name = @db_dir)
      dir_bits = id & @dir_mask
      sub_dir_name = File.join(dir_name, @dir_format_string % dir_bits)

      if Dir.exist?(sub_dir_name)
        if File.exist?(File.join(sub_dir_name, 'index'))
          # The directory is a blob directory and not a BTree node dir.
          return BTreeBlob.new(sub_dir_name, self)
        end
      else
        Dir.glob(File.join(dir_name, '*.index')).each do |fqfn|
          # Extract the 01-part of the filename
          lsb_string = File.basename(fqfn)[0..-6]
          # Convert the lsb_string into a Integer
          lsb = Integer('0b' + lsb_string)
          # Bit mask to match the LSBs
          mask = (2 ** lsb_string.length) - 1
          if (id & mask) == lsb
            return TreeBlob.new(sub_dir_name, lsb_string, self)
          end
        end
        if create_missing_blob
          # Create the new blob directory.
          Dir.mkdir(dir_name)
          # And initialize the blob DB.
          return BTreeBlob.new(dir_name, self)
        else
          return nil
        end
      end

      # Discard the least significant @dir_bits bits and start over again
      # with the directory that matches the @dir_bits LSBs of the new ID.
      id = id >> @dir_bits
    end

    def each_blob(&block)
      each_blob_r(@db_dir, &block)
    end

    def each_blob_r(dir, &block)
      Dir.glob(File.join(dir, '*')) do |dir_name|
        if is_blob_dir?(dir_name)
          block.call(BTreeBlob.new(dir_name, self))
        else
          each_blob_r(dir_name, &block)
        end
      end
    end

    def is_blob_dir?(dir_name)
      # A blob directory contains an 'index' and 'data' file. This is in
      # contrast to BTree node directories that only contain other
      # directories.
      index_file = File.join(dir_name, 'index')
      File.exist?(index_file)
    end

  end

end

