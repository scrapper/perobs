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

require 'perobs/DataBase'
require 'perobs/BTreeBlob'

module PEROBS

  class BTreeDB < DataBase

    def initialize(db_name, options = {})
      super(options[:serializer] || :json)

      @db_dir = db_name
      @dir_bits = options[:dir_bits] || 10
      if @dir_bits < 4 || @dir_bits > 12
        raise ArgumentError,
              "dir_bits option (#{@dir_bits}) must be between 4 and 12"
      end
      @dir_format_string = "%0#{(@dir_bits / 4) +
                                (@dir_bits % 4 == 0 ? 0 : 1)}X"
      @dir_mask = 2 ** @dir_bits - 1

      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)
    end

    # Return true if the object with given ID exists
    # @param id [Fixnum or Bignum]
    def include?(id)
      (blob = find_blob(id)) && blob.find(id)
    end

    # Store the given object into the cluster files.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      find_blob(id, true).write_object(id, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Fixnum or Bignum] object ID
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
    def delete_unmarked_objects
      each_blob { |blob| blob.delete_unmarked_entries }
    end

    # Mark an object.
    # @param id [Fixnum or Bignum] ID of the object to mark
    def mark(id)
      (blob = find_blob(id)) && blob.mark(id)
    end

    # Check if the object is marked.
    # @param id [Fixnum or Bignum] ID of the object to check
    def is_marked?(id)
      (blob = find_blob(id)) && blob.is_marked?(id)
    end

    # Basic consistency check.
    # @param repair [TrueClass/FalseClass] True if found errors should be
    #        repaired.
    def check_db(repair = false)
      each_blob { |blob| blob.check(repair) }
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
      find_blob(id, true).write_object(id, raw)
    end

    private

    def find_blob(id, create_missing_blob = false)
      dir_name = @db_dir
      loop do
        name = @dir_format_string % (id & @dir_mask)
        dir_name = File.join(dir_name, name)
        if is_blob_dir?(dir_name)
          # The directory is a blob directory and not a BTree node dir.
          return BTreeBlob.new(dir_name, self)
        elsif !Dir.exists?(dir_name)
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
      File.exists?(index_file)
    end

  end

end

