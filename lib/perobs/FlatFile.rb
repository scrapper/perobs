# encoding: UTF-8
#
# = FlatFile.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'zlib'

module PEROBS

  # The FlatFile class manages the storage file of the FlatFileDB. It contains
  # a sequence of blobs Each blob consists of a 25 byte header and the actual
  # blob data bytes. The header has the following structure:
  #
  # 1 Byte:  Mark byte.
  #          Bit 0: 0 deleted entry, 1 valid entry
  #          Bit 1: 0 unmarked, 1 marked
  #          Bit 2 - 7: reserved
  # 8 bytes: Length of the data blob in bytes
  # 8 bytes: ID of the value in the data blob
  # 4 bytes: CRC32 checksum of the data blob
  #
  # If the bit 0 of the mark byte is 0, only the length is valid. The blob is
  # empty. Only of bit 0 is set then entry is valid.
  class FlatFile

    # The 'pack()' format of the header.
    BLOB_HEADER_FORMAT = 'CQQL'
    # The length of the header in bytes.
    BLOB_HEADER_LENGTH = 21

    # Create a new FlatFile object for a database in the given path.
    # @param dir [String] Directory path for the data base file
    def initialize(dir)
      @db_dir = dir
      @f = nil
    end

    # Open the flat file for reading and writing.
    def open
      file_name = File.join(@db_dir, 'database.blobs')
      begin
        if File.exists?(file_name)
          @f = File.open(file_name, 'rb+')
        else
          @f = File.open(file_name, 'wb+')
        end
      rescue => e
        raise IOError, "Cannot open flat file database #{file_name}: " +
          e.message
      end
    end

    # Close the flat file. This method must be called to ensure that all data
    # is really written into the filesystem.
    def close
      @f.close
    end

    # Force outstanding data to be written to the filesystem.
    def sync
      @f.flush
    end

    # Delete the blob for the specified ID.
    # @param id [Integer] ID of the object to be deleted
    # @return [Boolean] True if object was deleted, false otherwise
    def delete_obj_by_id(id)
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1) && blob_id == id
          delete_obj_by_address(pos, id)
          return true
        end
      end

      return false
    end

    # Delete the blob that is stored at the specified address.
    # @param addr [Integer] Address of the blob to delete
    # @param id [Integer] ID of the blob to delete
    def delete_obj_by_address(addr, id)
      begin
        @f.seek(addr)
        @f.write([ 0 ].pack('C'))
        @f.flush
      rescue => e
        raise IOError, "Cannot erase blob for ID #{id}: #{e.message}"
      end
    end

    # Delete all unmarked objects.
    def delete_unmarked_objects
      deleted_ids = []
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 3 == 1)
          delete_obj_by_address(pos, blob_id)
          deleted_ids << blob_id
        end
      end

      deleted_ids
    end

    # Write the given object into the file.
    # @param id [Integer] ID of the object
    # @param raw_obj [String] Raw object as String
    def write_obj_by_id(id, raw_obj)
      addr, length = find_free_blob(raw_obj.length)
      begin
        @f.seek(addr)
        @f.write([ 1, raw_obj.length, id, checksum(raw_obj)].
                 pack(BLOB_HEADER_FORMAT))
        @f.write(raw_obj)
        if length > 0 && raw_obj.length != length
          # The new object was not appended and it did not completely fill the
          # free space. So we have to write a new header to mark the remaining
          # empty space.
          @f.write([ 0, length - BLOB_HEADER_LENGTH - raw_obj.length, 0, 0 ].
                   pack(BLOB_HEADER_FORMAT))
        end
        @f.flush
      rescue => e
        raise IOError, "Cannot write blob for ID #{id} to FlatFileDB: " +
          e.message
      end
    end

    # Find the address of the object with the given ID.
    # @param id [Integer] ID of the object
    # @return [Integer] Offset in the flat file or nil if not found
    def find_obj_addr_by_id(id)
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1) && (blob_id == id)
          return pos
        end
      end

      nil
    end

    # Read the object with the given ID.
    # @param id [Integer] ID of the object
    # @return [String or nil] Raw object data if found, otherwise nil
    def read_obj_by_id(id)
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1) && (blob_id == id)
          return read_obj_by_address(pos, length, blob_id, crc)
        end
      end

      nil
    end

    # Read the object at the specified address.
    # @param addr [Integer] Offset in the flat file
    # @param length [Integer] Length of the data blob in bytes
    # @param id [Integer] ID of the data blob
    # @param crc [Fixnum] CRC32 checksum of the data blob
    # @return [String] Raw object data
    def read_obj_by_address(addr, length, id, crc = nil)
      begin
        @f.seek(addr + BLOB_HEADER_LENGTH)
        buf = @f.read(length)
        if crc && checksum(buf) != crc
          raise RuntimeError,
            "Checksum failure while reading blob ID #{id}"
        end
        return buf
      rescue => e
        raise IOError, "Cannot read blob for ID #{id}: #{e.message}"
      end
    end

    # Mark the object with the given ID.
    # @param id [Integer] ID of the object
    def mark_obj_by_id(id)
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1) && (blob_id == id)
          mark_obj_by_address(pos, mark, id)
        end
      end
    end

    # Mark the object at the specified address.
    # @param addr [Integer] Offset in the file
    # @param mark [Fixnum] Current value of the mark byte
    # @param id [Integer] ID of the object
    def mark_obj_by_address(addr, mark, id)
      begin
        @f.seek(addr)
        @f.write([ mark | 2 ].pack('C'))
        @f.flush
      rescue => e
        raise IOError, "Marking of FlatFile blob with ID #{id} " +
          "failed: #{e.message}"
      end
    end

    # Return true if the object with the given ID is marked, false otherwise.
    # @param id [Integer] ID of the object
    def is_marked_by_id?(id)
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1) && (blob_id == id)
          return (mark & 2) == 2
        end
      end

      false
    end

    # Clear alls marks.
    def clear_all_marks
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1)
          begin
            @f.seek(pos)
            @f.write([ mark & 0b11111101 ].pack('C'))
            @f.flush
          rescue => e
            raise IOError, "Unmarking of FlatFile blob with ID #{blob_id} " +
                           "failed: #{e.message}"
          end
        end
      end
    end

    # Eliminate all the holes in the file. This is an in-place
    # implementation. No additional space will be needed on the file system.
    def defragmentize
      distance = 0
      # Iterate over all entries.
      each_blob_header do |pos, mark, length, blob_id, crc|
        # Total size of the current entry
        entry_bytes = BLOB_HEADER_LENGTH + length
        if (mark & 1 == 1)
          # We have found a valid entry.
          if distance > 0
            begin
              # Read current entry into a buffer
              @f.seek(pos)
              buf = @f.read(entry_bytes)
              # Write the buffer right after the end of the previous entry.
              @f.seek(pos - distance)
              @f.write(buf)
              # Mark the space between the relocated current entry and the
              # next valid entry as deleted space.
              @f.write([ 0, distance - BLOB_HEADER_LENGTH, 0, 0 ].
                       pack(BLOB_HEADER_FORMAT))
              @f.flush
            rescue => e
              raise IOError, "Error while moving blob for ID #{blob_id}: " +
                e.message
            end
          end
        else
          distance += entry_bytes
        end
      end

      @f.flush
      @f.truncate(@f.size - distance)
      @f.flush
    end

    def check(repair)
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1)
          begin
            @f.seek(pos + BLOB_HEADER_LENGTH)
            buf = @f.read(length)
            if crc && checksum(buf) != crc
              if repair
                delete_obj_by_address(pos, blob_id)
              else
                raise RuntimeError,
                  "Checksum failure while checking blob with ID #{id}"
              end
            end
          rescue => e
            raise IOError, "Check of blob with ID #{blob_id} failed: " +
              e.message
          end
        end
      end
    end


    private

    def find_free_blob(bytes)
      each_blob_header do |pos, mark, length, id, crc|
        # The unused space must either be exactly 'bytes' long or it must be
        # smaller than 'bytes' plus the new header that marks the smaller
        # empty space.
        if (mark & 1) == 0 &&
           (length == bytes || (bytes + BLOB_HEADER_LENGTH <= length))
          return [ pos, length ]
        end
      end

      [ @f.size, -1 ]
    end

    def checksum(raw_obj)
      Zlib.crc32(raw_obj, 0)
    end

    def each_blob_header(&block)
      pos = 0
      begin
        @f.seek(0)
        while (buf = @f.read(BLOB_HEADER_LENGTH))
          mark, length, id, crc = buf.unpack(BLOB_HEADER_FORMAT)
          yield(pos, mark, length, id, crc)

          pos += BLOB_HEADER_LENGTH + length
          @f.seek(pos)
        end
      rescue => e
        raise IOError, "Cannot read blob in flat file DB: #{e.message}"
      end
    end

  end

end

