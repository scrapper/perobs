# encoding: UTF-8
#
# = HashedBlocksDB.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'time'
require 'json'
require 'json/add/core'
require 'json/add/struct'
require 'yaml'
require 'fileutils'

require 'perobs/DataBase'
require 'perobs/BlockDB'

module PEROBS

  # This class provides a filesytem based database store for objects.
  class HashedBlocksDB < DataBase

    # Create a new HashedBlocksDB object. This will create a database with the
    # given name. The database will live in a directory of that given name.
    # @param db_name [String] name of the DB directory
    # @param options [Hash] options to customize the behavior. Currently only
    #        the following options are supported:
    #        :serializer  : Can be :marshal, :json, :yaml
    #        :dir_nibbles : The number of nibbles to use for directory names.
    #                       Meaningful values are 1, 2, and 3. The larger the
    #                       number the more back-end files are used. Each
    #                       nibble provides 16 times more directories.
    #        :block_size  : The size of the blocks inside the storage files in
    #                       bytes. This should roughly correspond to the size
    #                       of the smallest serialized objects you want to
    #                       store in quantities. It also should be an fraction
    #                       of 4096, the native storage system block size.
    def initialize(db_name, options = {})
      super(options[:serializer] || :json)
      @db_dir = db_name
      @dir_nibbles = options[:dir_nibbles] || 2
      @block_size = options[:block_size] || 256

      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)
    end

    # Return true if the object with given ID exists
    # @param id [Fixnum or Bignum]
    def include?(id)
      !BlockDB.new(directory(id), @block_size).find(id).nil?
    end

    # Store the given object into the cluster files.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      BlockDB.new(directory(id), @block_size).write_object(id, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Fixnum or Bignum] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase
    def get_object(id)
      deserialize(BlockDB.new(directory(id), @block_size).read_object(id))
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      Dir.glob(File.join(@db_dir, '*')) do |dir|
        BlockDB.new(dir, @block_size).clear_marks
      end
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    def delete_unmarked_objects
      Dir.glob(File.join(@db_dir, '*')) do |dir|
        BlockDB.new(dir, @block_size).delete_unmarked_entries
      end
    end

    # Mark an object.
    # @param id [Fixnum or Bignum] ID of the object to mark
    def mark(id)
      BlockDB.new(directory(id), @block_size).mark(id)
    end

    # Check if the object is marked.
    # @param id [Fixnum or Bignum] ID of the object to check
    def is_marked?(id)
      BlockDB.new(directory(id), @block_size).is_marked?(id)
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

    private

    # Determine the file name to store the object. The object ID determines
    # the directory and file name inside the store.
    # @param id [Fixnum or Bignum] ID of the object
    def directory(id)
      hex_id = "%016X" % id
      dir = hex_id[0..(@dir_nibbles - 1)]
      ensure_dir_exists(dir_name = File.join(@db_dir, dir))

      dir_name
    end

  end

end
