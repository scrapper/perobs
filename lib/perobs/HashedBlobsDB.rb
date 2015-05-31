# encoding: UTF-8
#
# = HashedBlobsDB.rb -- Persistent Ruby Object Store
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
require 'perobs/BlobsDB'

module PEROBS

  # This class provides a filesytem based database store for objects.
  class HashedBlobsDB < DataBase

    # Create a new HashedBlobsDB object. This will create a database with the
    # given name. The database will live in a directory of that given name.
    # @param db_name [String] name of the DB directory
    # @param options [Hash] options to customize the behavior. Currently only
    #        the following options are supported:
    #        :serializer : Can be :marshal, :json, :yaml
    #        :dir_bits   : The number of bits to use for directory names. The
    #                      value must be between 4 and 16. The larger the
    #                      number the more back-end files are being used. The
    #                      more files there are, the fewer entries they have
    #                      and hence the linear search for IDs, free space
    #                      etc. is faster. The default is 12.
    def initialize(db_name, options = {})
      super(options[:serializer] || :json)
      @db_dir = db_name
      @dir_bits = options[:dir_bits] || 12
      if @dir_bits < 4 || @dir_bits > 16
        raise ArgumentError,
              "dir_bits option (#{@dir_bits}) must be between 4 and 16"
      end

      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)
      @blobs_dbs = []
    end

    # Return true if the object with given ID exists
    # @param id [Fixnum or Bignum]
    def include?(id)
      !blobs(id).find(id).nil?
    end

    # Store the given object into the cluster files.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      blobs(id).write_object(id, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Fixnum or Bignum] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase
    def get_object(id)
      deserialize(blobs(id).read_object(id))
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      @blobs_dbs.each { |bdb| bdb.clear_marks if bdb }
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    def delete_unmarked_objects
      @blobs_dbs.each { |bdb| bdb.delete_unmarked_entries if bdb }
    end

    # Mark an object.
    # @param id [Fixnum or Bignum] ID of the object to mark
    def mark(id)
      blobs(id).mark(id)
    end

    # Check if the object is marked.
    # @param id [Fixnum or Bignum] ID of the object to check
    def is_marked?(id)
      blobs(id).is_marked?(id)
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

    # Empty the cache.
    def clear_cache
      @blobs_cache = ::Array.new(2**@cache_bits)
    end

    # This method returns a BlobsDB object that can be used to access the data
    # for the object with the given ID. If the BlobsDB object is already in
    # the cache, we use that one. Otherwise we generate a new one.
    # @param [Fixnum or Bignum] ID of the object
    # @return [BlobsDB] The corresponding BlobsDB for the ID
    def blobs(id)
      dir_id = id >> (64 - @dir_bits)
      unless (bdb = @blobs_dbs[dir_id])
        dir_name = File.join(@db_dir, "%X" % dir_id)
        ensure_dir_exists(dir_name)
        @blobs_dbs[dir_id] = bdb = BlobsDB.new(dir_name)
      end

      bdb
    end

  end

end

