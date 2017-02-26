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

require 'perobs/Log'
require 'perobs/IndexTree'
require 'perobs/FreeSpaceManager'

module PEROBS

  # The FlatFile class manages the storage file of the FlatFileDB. It contains
  # a sequence of blobs Each blob consists of a 25 byte header and the actual
  # blob data bytes. The header has the following structure:
  #
  # 1 Byte:  Mark byte.
  #          Bit 0: 0 deleted entry, 1 valid entry
  #          Bit 1: 0 unmarked, 1 marked
  #          Bit 2 - 7: reserved, must be 0
  # 8 bytes: Length of the data blob in bytes
  # 8 bytes: ID of the value in the data blob
  # 4 bytes: CRC32 checksum of the data blob
  #
  # If the bit 0 of the mark byte is 0, only the length is valid. The blob is
  # empty. Only of bit 0 is set then entry is valid.
  class FlatFile

    # Utility class to hold all the data that is stored in a blob header.
    class Header < Struct.new(:mark, :length, :id, :crc)
    end

    # The 'pack()' format of the header.
    BLOB_HEADER_FORMAT = 'CQQL'
    # The length of the header in bytes.
    BLOB_HEADER_LENGTH = 21

    # Create a new FlatFile object for a database in the given path.
    # @param dir [String] Directory path for the data base file
    def initialize(dir)
      @db_dir = dir
      @f = nil
      @index = IndexTree.new(dir)
      @space_list = FreeSpaceManager.new(dir)
    end

    # Open the flat file for reading and writing.
    def open
      file_name = File.join(@db_dir, 'database.blobs')
      begin
        if File.exist?(file_name)
          @f = File.open(file_name, 'rb+')
        else
          PEROBS.log.info 'New database.blobs file created'
          @f = File.open(file_name, 'wb+')
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot open flat file database #{file_name}: " +
          e.message
      end
      @index.open
      @space_list.open
    end

    # Close the flat file. This method must be called to ensure that all data
    # is really written into the filesystem.
    def close
      @space_list.close
      @index.close
      @f.flush
      @f.close
      @f = nil
    end

    # Force outstanding data to be written to the filesystem.
    def sync
      begin
        @f.flush
      rescue IOError => e
        PEROBS.log.fatal "Cannot sync flat file database: #{e.message}"
      end
    end

    # Delete the blob for the specified ID.
    # @param id [Integer] ID of the object to be deleted
    # @return [Boolean] True if object was deleted, false otherwise
    def delete_obj_by_id(id)
      if (pos = find_obj_addr_by_id(id))
        delete_obj_by_address(pos, id)
        return true
      end

      return false
    end

    # Delete the blob that is stored at the specified address.
    # @param addr [Integer] Address of the blob to delete
    # @param id [Integer] ID of the blob to delete
    def delete_obj_by_address(addr, id)
      @index.delete_value(id)
      header = read_blob_header(addr, id)
      begin
        @f.seek(addr)
        @f.write([ 0 ].pack('C'))
        @f.flush
        @space_list.add_space(addr, header.length)
      rescue => e
        PEROBS.log.fatal "Cannot erase blob for ID #{header.id}: #{e.message}"
      end
    end

    # Delete all unmarked objects.
    def delete_unmarked_objects
      PEROBS.log.info "Deleting unmarked objects..."
      t = Time.now

      deleted_ids = []
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 3 == 1)
          delete_obj_by_address(pos, blob_id)
          deleted_ids << blob_id
        end
      end
      defragmentize

      PEROBS.log.info "#{deleted_ids.length} unmarked objects deleted " +
        "in #{Time.now - t} seconds"
      deleted_ids
    end

    # Write the given object into the file. This method assumes that no other
    # entry with the given ID exists already in the file.
    # @param id [Integer] ID of the object
    # @param raw_obj [String] Raw object as String
    # @return [Integer] position of the written blob in the blob file
    def write_obj_by_id(id, raw_obj)
      addr, length = find_free_blob(raw_obj.length)
      begin
        if length != -1
          # Just a safeguard so we don't overwrite current data.
          header = read_blob_header(addr)
          if header.length != length
            PEROBS.log.fatal "Length in free list (#{length}) and header " +
              "(#{header.length}) don't match."
          end
          if raw_obj.length > header.length
            PEROBS.log.fatal "Object (#{raw_obj.length}) is longer than " +
              "blob space (#{header.length})."
          end
          if header.mark != 0
            PEROBS.log.fatal "Mark (#{header.mark}) is not 0."
          end
        end
        @f.seek(addr)
        @f.write([ 1, raw_obj.length, id, checksum(raw_obj)].
                 pack(BLOB_HEADER_FORMAT))
        @f.write(raw_obj)
        if length != -1 && raw_obj.length < length
          # The new object was not appended and it did not completely fill the
          # free space. So we have to write a new header to mark the remaining
          # empty space.
          unless length - raw_obj.length >= BLOB_HEADER_LENGTH
            PEROBS.log.fatal "Not enough space to append the empty space " +
              "header (space: #{length} bytes, object: #{raw_obj.length} " +
              "bytes)."
          end
          space_address = @f.pos
          space_length = length - BLOB_HEADER_LENGTH - raw_obj.length
          @f.write([ 0, space_length, 0, 0 ].pack(BLOB_HEADER_FORMAT))
          # Register the new space with the space list.
          @space_list.add_space(space_address, space_length) if space_length > 0
        end
        @f.flush
        @index.put_value(id, addr)
      rescue IOError => e
        PEROBS.log.fatal "Cannot write blob for ID #{id} to FlatFileDB: " +
          e.message
      end

      addr
    end

    # Find the address of the object with the given ID.
    # @param id [Integer] ID of the object
    # @return [Integer] Offset in the flat file or nil if not found
    def find_obj_addr_by_id(id)
      @index.get_value(id)
    end

    # Read the object with the given ID.
    # @param id [Integer] ID of the object
    # @return [String or nil] Raw object data if found, otherwise nil
    def read_obj_by_id(id)
      if (addr = find_obj_addr_by_id(id))
        return read_obj_by_address(addr, id)
      end

      nil
    end

    # Read the object at the specified address.
    # @param addr [Integer] Offset in the flat file
    # @param id [Integer] ID of the data blob
    # @return [String] Raw object data
    def read_obj_by_address(addr, id)
      header = read_blob_header(addr, id)
      if header.id != id
        PEROBS.log.fatal "Database index corrupted: Index for object " +
          "#{id} points to object with ID #{header.id}"
      end
      begin
        @f.seek(addr + BLOB_HEADER_LENGTH)
        buf = @f.read(header.length)
        if checksum(buf) != header.crc
          PEROBS.log.fatal "Checksum failure while reading blob ID #{id}"
        end
        return buf
      rescue => e
        PEROBS.log.fatal "Cannot read blob for ID #{id}: #{e.message}"
      end
    end

    # Mark the object with the given ID.
    # @param id [Integer] ID of the object
    def mark_obj_by_id(id)
      if (addr = find_obj_addr_by_id(id))
        mark_obj_by_address(addr, id)
      end
    end

    # Mark the object at the specified address.
    # @param addr [Integer] Offset in the file
    # @param id [Integer] ID of the object
    def mark_obj_by_address(addr, id)
      header = read_blob_header(addr, id)
      begin
        @f.seek(addr)
        @f.write([ header.mark | 2 ].pack('C'))
        @f.flush
      rescue => e
        PEROBS.log.fatal "Marking of FlatFile blob with ID #{id} " +
          "failed: #{e.message}"
      end
    end

    # Return true if the object with the given ID is marked, false otherwise.
    # @param id [Integer] ID of the object
    def is_marked_by_id?(id)
      if (addr = find_obj_addr_by_id(id))
        header = read_blob_header(addr, id)
        return (header.mark & 2) == 2
      end

      false
    end

    # Clear alls marks.
    def clear_all_marks
      t = Time.now
      PEROBS.log.info "Clearing all marks..."

      total_blob_count = 0
      marked_blob_count = 0

      each_blob_header do |pos, mark, length, blob_id, crc|
        total_blob_count += 1
        if (mark & 1 == 1)
          marked_blob_count += 1
          begin
            @f.seek(pos)
            @f.write([ mark & 0b11111101 ].pack('C'))
            @f.flush
          rescue => e
            PEROBS.log.fatal "Unmarking of FlatFile blob with ID #{blob_id} " +
              "failed: #{e.message}"
          end
        end
      end
      PEROBS.log.info "#{marked_blob_count} marks in #{total_blob_count} " +
        "objects cleared in #{Time.now - t} seconds"
    end

    # Eliminate all the holes in the file. This is an in-place
    # implementation. No additional space will be needed on the file system.
    def defragmentize
      distance = 0
      t = Time.now
      PEROBS.log.debug "Defragmenting FlatFile"
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
              # Update the index with the new position
              @index.put_value(blob_id, pos - distance)
              # Mark the space between the relocated current entry and the
              # next valid entry as deleted space.
              @f.write([ 0, distance - BLOB_HEADER_LENGTH, 0, 0 ].
                       pack(BLOB_HEADER_FORMAT))
              @f.flush
            rescue => e
              PEROBS.log.fatal "Error while moving blob for ID #{blob_id}: " +
                e.message
            end
          end
        else
          distance += entry_bytes
        end
      end
      PEROBS.log.debug "FlatFile defragmented in #{Time.now - t} seconds"
      PEROBS.log.debug "#{distance} bytes or " +
        "#{'%.1f' % (distance.to_f / @f.size * 100.0)}% reclaimed"

      @f.flush
      @f.truncate(@f.size - distance)
      @f.flush
      @space_list.clear

      sync
    end

    def check(repair = false)
      return unless @f

      t = Time.now
      PEROBS.log.info "Checking FlatFile database..."

      # First check the database blob file. Each entry should be readable and
      # correct.
      each_blob_header do |pos, mark, length, blob_id, crc|
        if (mark & 1 == 1)
          # We have a non-deleted entry.
          begin
            @f.seek(pos + BLOB_HEADER_LENGTH)
            buf = @f.read(length)
            if crc && checksum(buf) != crc
              if repair
                PEROBS.log.error "Checksum failure while checking blob " +
                  "with ID #{id}. Deleting object."
                delete_obj_by_address(pos, blob_id)
              else
                PEROBS.log.fatal "Checksum failure while checking blob " +
                  "with ID #{id}"
              end
            end
          rescue => e
            PEROBS.log.fatal "Check of blob with ID #{blob_id} failed: " +
              e.message
          end
        end
      end

      # Now we check the index data. It must be correct and the entries must
      # match the blob file. All entries in the index must be in the blob file
      # and vise versa.
      begin
        unless @index.check(self) && @space_list.check(self) &&
          cross_check_entries

          regenerate_index_and_spaces if repair
        end
      rescue PEROBS::FatalError
        regenerate_index_and_spaces
      end

      sync if repair
      PEROBS.log.info "check_db completed in #{Time.now - t} seconds"
    end

    # This method clears the index tree and the free space list and
    # regenerates them from the FlatFile.
    def regenerate_index_and_spaces
      PEROBS.log.warn "Re-generating FlatFileDB index and space files"
      @index.clear
      @space_list.clear

      each_blob_header do |pos, mark, length, id, crc|
        if mark == 0
          @space_list.add_space(pos, length) if length > 0
        else
          @index.put_value(id, pos)
        end
      end
    end

    def has_space?(address, size)
      header = read_blob_header(address)
      header.length == size
    end

    def has_id_at?(id, address)
      header = read_blob_header(address)
      header.id == id
    end

    def inspect
      s = '['
      each_blob_header do |pos, mark, length, blob_id, crc|
        s << "{ :pos => #{pos}, :mark => #{mark}, " +
             ":length => #{length}, :id => #{blob_id}, :crc => #{crc}"
        if mark != 0
          s << ", :value => #{@f.read(length)}"
        end
        s << " }\n"
      end
      s + ']'
    end



    private

    def read_blob_header(addr, id = nil)
      buf = nil
      begin
        @f.seek(addr)
        buf = @f.read(BLOB_HEADER_LENGTH)
      rescue => e
        PEROBS.log.fatal "Cannot read blob in flat file DB: #{e.message}"
      end
      if buf.nil? || buf.length != BLOB_HEADER_LENGTH
        PEROBS.log.fatal "Cannot read blob header " +
          "#{id ? "for ID #{id} " : ''}at address " +
          "#{addr}"
      end
      header = Header.new(*buf.unpack(BLOB_HEADER_FORMAT))
      if id && header.id != id
        PEROBS.log.fatal "Mismatch between FlatFile index and blob file " +
          "found for entry with ID #{id}/#{header.id}"
      end

      return header
    end

    def find_free_blob(bytes)
      address, size = @space_list.get_space(bytes)
      unless address
        # We have not found any suitable space. Return the end of the file.
        return [ @f.size, -1 ]
      end
      if size == bytes || size - BLOB_HEADER_LENGTH >= bytes
        return [ address, size ]
      end

      # Return the found space again. It's too small for the new content plus
      # the gap header.
      @space_list.add_space(address, size)

      # We need a space that is large enough to hold the bytes and the gap
      # header.
      @space_list.get_space(bytes + BLOB_HEADER_LENGTH) || [ @f.size, -1 ]
    end

    def checksum(raw_obj)
      Zlib.crc32(raw_obj, 0)
    end

    def cross_check_entries
      each_blob_header do |pos, mark, length, blob_id, crc|
        if mark == 0
          if length > 0
            unless @space_list.has_space?(pos, length)
              PEROBS.log.error "FlatFile has free space " +
                "(addr: #{pos}, len: #{length}) that is not in FreeSpaceManager"
              return false
            end
          end
        else
          unless @index.get_value(blob_id) == pos
            PEROBS.log.error "FlatFile blob at address #{pos} is listed " +
              "in index with address #{@index.get_value(blob_id)}"
            return false
          end
        end
      end

      true
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
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob in flat file DB: #{e.message}"
      end
    end

  end

end

