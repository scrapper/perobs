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
require 'perobs/FlatFileBlobHeader'
require 'perobs/BTree'
require 'perobs/SpaceTree'

module PEROBS

  # The FlatFile class manages the storage file of the FlatFileDB. It contains
  # a sequence of blobs Each blob consists of header and the actual
  # blob data bytes.
  class FlatFile

    # The number of entries in a single BTree node of the index file.
    INDEX_BTREE_ORDER = 65

    # Create a new FlatFile object for a database in the given path.
    # @param dir [String] Directory path for the data base file
    def initialize(dir)
      @db_dir = dir
      @f = nil
      @index = BTree.new(@db_dir, 'index', INDEX_BTREE_ORDER)
      @space_list = SpaceTree.new(@db_dir)
    end

    # Open the flat file for reading and writing.
    def open
      file_name = File.join(@db_dir, 'database.blobs')
      new_db_created = false
      begin
        if File.exist?(file_name)
          @f = File.open(file_name, 'rb+')
        else
          PEROBS.log.info "New FlatFile database '#{file_name}' created"
          @f = File.open(file_name, 'wb+')
          new_db_created = true
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot open FlatFile database #{file_name}: " +
          e.message
      end
      unless @f.flock(File::LOCK_NB | File::LOCK_EX)
        PEROBS.log.fatal "FlatFile database '#{file_name}' is locked by " +
          "another process"
      end

      begin
        @index.open(!new_db_created)
        @space_list.open
      rescue FatalError
        # Ensure that the index is really closed.
        @index.close
        # Erase it completely
        @index.erase
        # Then create it again.
        @index.open

        # Ensure that the spaces list is really closed.
        @space_list.close
        # Erase it completely
        @space_list.erase
        # Then create it again
        @space_list.open

        regenerate_index_and_spaces
      end
    end

    # Close the flat file. This method must be called to ensure that all data
    # is really written into the filesystem.
    def close
      @space_list.close
      @index.close

      if @f
        @f.flush
        @f.flock(File::LOCK_UN)
        @f.close
        @f = nil
      end
    end

    # Force outstanding data to be written to the filesystem.
    def sync
      begin
        @f.flush
      rescue IOError => e
        PEROBS.log.fatal "Cannot sync flat file database: #{e.message}"
      end
      @index.sync
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
      @index.remove(id)
      header = FlatFileBlobHeader.read_at(@f, addr, id)
      begin
        @f.seek(addr)
        @f.write([ 0 ].pack('C'))
        @f.flush
        @space_list.add_space(addr, header.length)
      rescue IOError => e
        PEROBS.log.fatal "Cannot erase blob for ID #{header.id}: #{e.message}"
      end
    end

    # Delete all unmarked objects.
    def delete_unmarked_objects
      PEROBS.log.info "Deleting unmarked objects..."
      t = Time.now

      deleted_ids = []
      each_blob_header do |pos, header|
        if header.is_valid? && !header.is_marked?
          delete_obj_by_address(pos, header.id)
          deleted_ids << header.id
        end
      end
      defragmentize

      PEROBS.log.info "#{deleted_ids.length} unmarked objects deleted " +
        "in #{Time.now - t} seconds"
      deleted_ids
    end

    # Write the given object into the file. This method never uses in-place
    # updates for existing objects. A new copy is inserted first and only when
    # the insert was successful, the old copy is deleted and the index
    # updated.
    # @param id [Integer] ID of the object
    # @param raw_obj [String] Raw object as String
    # @return [Integer] position of the written blob in the blob file
    def write_obj_by_id(id, raw_obj)
      # Check if we have already an object with the given ID. We'll save the
      # address for later use.
      old_addr = find_obj_addr_by_id(id)

      crc = checksum(raw_obj)

      # If the raw_obj is larger then 256 characters we will compress it to
      # safe some space in the database file. For smaller strings the
      # performance impact of compression is not compensated by writing
      # less data to the storage.
      compressed = false
      if raw_obj.length > 256
        raw_obj = Zlib.deflate(raw_obj)
        compressed = true
      end

      addr, length = find_free_blob(raw_obj.length)
      begin
        if length != -1
          # Just a safeguard so we don't overwrite current data.
          header = FlatFileBlobHeader.read_at(@f, addr)
          if header.length != length
            PEROBS.log.fatal "Length in free list (#{length}) and header " +
              "(#{header.length}) don't match."
          end
          if raw_obj.length > header.length
            PEROBS.log.fatal "Object (#{raw_obj.length}) is longer than " +
              "blob space (#{header.length})."
          end
          if header.is_valid?
            PEROBS.log.fatal "Entry (flags: #{header.flags}) is already used."
          end
        end
        @f.seek(addr)
        FlatFileBlobHeader.new(compressed ? (1 << 2) | 1 : 1, raw_obj.length,
                               id, crc).write(@f)
        @f.write(raw_obj)
        if length != -1 && raw_obj.length < length
          # The new object was not appended and it did not completely fill the
          # free space. So we have to write a new header to mark the remaining
          # empty space.
          unless length - raw_obj.length >= FlatFileBlobHeader::LENGTH
            PEROBS.log.fatal "Not enough space to append the empty space " +
              "header (space: #{length} bytes, object: #{raw_obj.length} " +
              "bytes)."
          end
          space_address = @f.pos
          space_length = length - FlatFileBlobHeader::LENGTH - raw_obj.length
          FlatFileBlobHeader.new(0, space_length, 0, 0).write(@f)
          # Register the new space with the space list.
          @space_list.add_space(space_address, space_length) if space_length > 0
        end
        if old_addr
          # If we had an existing object stored for the ID we have to mark
          # this entry as deleted now.
          @f.seek(old_addr)
          @f.write([ 0 ].pack('C'))
        end
        @f.flush
        @index.insert(id, addr)
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
      @index.get(id)
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
      header = FlatFileBlobHeader.read_at(@f, addr, id)
      if header.id != id
        PEROBS.log.fatal "Database index corrupted: Index for object " +
          "#{id} points to object with ID #{header.id}"
      end

      buf = nil

      begin
        @f.seek(addr + FlatFileBlobHeader::LENGTH)
        buf = @f.read(header.length)
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob for ID #{id}: #{e.message}"
      end

      # Uncompress the data if the compression bit is set in the flags byte.
      if header.is_compressed?
        begin
          buf = Zlib.inflate(buf)
        rescue Zlib::BufError, Zlib::DataError
          PEROBS.log.fatal "Corrupted compressed block with ID " +
            "#{header.id} found."
        end
      end

      if checksum(buf) != header.crc
        PEROBS.log.fatal "Checksum failure while reading blob ID #{id}"
      end

      buf
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
      header = FlatFileBlobHeader.read_at(@f, addr, id)
      begin
        @f.seek(addr)
        @f.write([ header.flags | (1 << 1) ].pack('C'))
        @f.flush
      rescue IOError => e
        PEROBS.log.fatal "Marking of FlatFile blob with ID #{id} " +
          "failed: #{e.message}"
      end
    end

    # Return true if the object with the given ID is marked, false otherwise.
    # @param id [Integer] ID of the object
    def is_marked_by_id?(id)
      if (addr = find_obj_addr_by_id(id))
        header = FlatFileBlobHeader.read_at(@f, addr, id)
        return header.is_marked?
      end

      false
    end

    # Clear alls marks.
    def clear_all_marks
      t = Time.now
      PEROBS.log.info "Clearing all marks..."

      total_blob_count = 0
      marked_blob_count = 0

      each_blob_header do |pos, header|
        total_blob_count += 1
        if header.is_valid? && header.is_marked?
          # Clear all valid and marked blocks.
          marked_blob_count += 1
          begin
            @f.seek(pos)
            @f.write([ header.flags & 0b11111101 ].pack('C'))
            @f.flush
          rescue IOError => e
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
      deleted_blobs = 0
      valid_blobs = 0
      t = Time.now
      PEROBS.log.info "Defragmenting FlatFile"
      # Iterate over all entries.
      each_blob_header do |pos, header|
        # Total size of the current entry
        entry_bytes = FlatFileBlobHeader::LENGTH + header.length
        if header.is_valid?
          # We have found a valid entry.
          valid_blobs += 1
          if distance > 0
            begin
              # Read current entry into a buffer
              @f.seek(pos)
              buf = @f.read(entry_bytes)
              # Write the buffer right after the end of the previous entry.
              @f.seek(pos - distance)
              @f.write(buf)
              # Update the index with the new position
              @index.insert(header.id, pos - distance)
              # Mark the space between the relocated current entry and the
              # next valid entry as deleted space.
              FlatFileBlobHeader.new(0, distance - FlatFileBlobHeader::LENGTH,
                                     0, 0).write(@f)
              @f.flush
            rescue IOError => e
              PEROBS.log.fatal "Error while moving blob for ID #{header.id}: " +
                e.message
            end
          end
        else
          deleted_blobs += 1
          distance += entry_bytes
        end
      end
      PEROBS.log.info "FlatFile defragmented in #{Time.now - t} seconds"
      PEROBS.log.info "#{distance / 1000} KiB/#{deleted_blobs} blobs of " +
        "#{@f.size / 1000} KiB/#{valid_blobs} blobs or " +
        "#{'%.1f' % (distance.to_f / @f.size * 100.0)}% reclaimed"

      @f.flush
      @f.truncate(@f.size - distance)
      @f.flush
      @space_list.clear

      sync
    end

    # This method iterates over all entries in the FlatFile and removes the
    # entry and inserts it again. This is useful to update all entries in
    # cased the storage format has changed.
    def refresh
      # This iteration might look scary as we iterate over the entries while
      # while we are rearranging them. Re-inserted items may be inserted
      # before or at the current entry and this is fine. They also may be
      # inserted after the current entry and will be re-read again unless they
      # are inserted after the original file end.
      file_size = @f.size
      PEROBS.log.info "Refreshing the DB..."
      t = Time.now
      each_blob_header do |pos, header|
        if header.is_valid?
          buf = read_obj_by_address(pos, header.id)
          delete_obj_by_address(pos, header.id)
          write_obj_by_id(header.id, buf)
        end

        # Some re-inserted blobs may be inserted after the original file end.
        # No need to process those blobs again.
        break if pos >= file_size
      end
      PEROBS.log.info "DB refresh completed in #{Time.now - t} seconds"

      # Reclaim the space saved by compressing entries.
      defragmentize
    end

    # Check (and repair) the FlatFile.
    # @param repair [Boolean] True if errors should be fixed.
    # @return [Integer] Number of errors found
    def check(repair = false)
      errors = 0
      return errors unless @f

      t = Time.now
      PEROBS.log.info "Checking FlatFile database" +
        "#{repair ? ' in repair mode' : ''}..."

      # First check the database blob file. Each entry should be readable and
      # correct and all IDs must be unique. We use a shadow index to keep
      # track of the already found IDs.
      new_index = BTree.new(@db_dir, 'new-index', INDEX_BTREE_ORDER)
      new_index.erase
      new_index.open

      each_blob_header do |pos, header|
        if header.is_valid?
          # We have a non-deleted entry.
          begin
            @f.seek(pos + FlatFileBlobHeader::LENGTH)
            buf = @f.read(header.length)
            if buf.length != header.length
              PEROBS.log.error "Premature end of file in blob with ID " +
                "#{header.id}."
              discard_damaged_blob(pos, header.id) if repair
              errors += 1
            end

            # Uncompress the data if the compression bit is set in the mark
            # byte.
            if header.is_compressed?
              begin
                buf = Zlib.inflate(buf)
              rescue Zlib::BufError, Zlib::DataError
                PEROBS.log.error "Corrupted compressed block with ID " +
                  "#{header.id} found."
                discard_damaged_blob(pos, header.id) if repair
                errors += 1
              end
            end

            if header.crc && checksum(buf) != header.crc
              PEROBS.log.error "Checksum failure while checking blob " +
                "with ID #{header.id}"
              discard_damaged_blob(pos, header.id) if repair
              errors += 1
            end
          rescue IOError => e
            PEROBS.log.fatal "Check of blob with ID #{header.id} failed: " +
              e.message
          end

          # Check if the ID has already been found in the file.
          if (previous_address = new_index.get(header.id))
            PEROBS.log.error "Multiple blobs for ID #{header.id} found. " +
              "Addresses: #{previous_address}, #{pos}"
            @f.seek(previous_address)
            previous_header = FlatFileBlobHeader.read(@f)
            # We have two blobs with the same ID and we must discard one of
            # them. As we haven't checked the index file yet, we must rely on
            # other information to make a choice. For now, we discard the
            # smaller one in the hope to minimize data loss.
            discard_damaged_blob(header.length < previous_header.length ?
                                 pos : previous_address, header.id) if repair
          else
            # ID is unique so far. Add it to the shadow index.
            new_index.insert(header.id, pos)
          end

        end
      end
      # We no longer need the new index.
      new_index.close
      new_index.erase

      # Now we check the index data. It must be correct and the entries must
      # match the blob file. All entries in the index must be in the blob file
      # and vise versa.
      begin
        index_ok = @index.check do |id, address|
          has_id_at?(id, address)
        end
        unless index_ok && @space_list.check(self) && cross_check_entries
          regenerate_index_and_spaces if repair
        end
      rescue PEROBS::FatalError
        errors += 1
        regenerate_index_and_spaces if repair
      end

      sync if repair
      PEROBS.log.info "check_db completed in #{Time.now - t} seconds. " +
        "#{errors} errors found."

      errors
    end

    # This method clears the index tree and the free space list and
    # regenerates them from the FlatFile.
    def regenerate_index_and_spaces
      PEROBS.log.warn "Re-generating FlatFileDB index and space files"
      @index.clear
      @space_list.clear

      each_blob_header do |pos, header|
        if header.is_valid?
          @index.insert(header.id, pos)
        else
          @space_list.add_space(pos, header.length) if header.length > 0
        end
      end
    end

    def has_space?(address, size)
      header = FlatFileBlobHeader.read_at(@f, address)
      header.length == size
    end

    def has_id_at?(id, address)
      header = FlatFileBlobHeader.read_at(@f, address)
      header.id == id
    end

    def inspect
      s = '['
      each_blob_header do |pos, header|
        s << "{ :pos => #{pos}, :flags => #{header.flags}, " +
             ":length => #{header.length}, :id => #{header.id}, " +
             ":crc => #{header.crc}"
        if header.is_valid?
          s << ", :value => #{@f.read(header.length)}"
        end
        s << " }\n"
      end
      s + ']'
    end

    private

    def each_blob_header(&block)
      pos = 0
      begin
        @f.seek(0)
        while (header = FlatFileBlobHeader.read(@f))
          yield(pos, header)

          pos += FlatFileBlobHeader::LENGTH + header.length
          @f.seek(pos)
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob in flat file DB: #{e.message}"
      end
    end

    def find_free_blob(bytes)
      address, size = @space_list.get_space(bytes)
      unless address
        # We have not found any suitable space. Return the end of the file.
        return [ @f.size, -1 ]
      end
      if size == bytes || size - FlatFileBlobHeader::LENGTH >= bytes
        return [ address, size ]
      end

      # Return the found space again. It's too small for the new content plus
      # the gap header.
      @space_list.add_space(address, size)

      # We need a space that is large enough to hold the bytes and the gap
      # header.
      @space_list.get_space(bytes + FlatFileBlobHeader::LENGTH) ||
        [ @f.size, -1 ]
    end

    def checksum(raw_obj)
      Zlib.crc32(raw_obj, 0)
    end

    def cross_check_entries
      errors = 0

      each_blob_header do |pos, header|
        if !header.is_valid?
          if header.length > 0
            unless @space_list.has_space?(pos, header.length)
              PEROBS.log.error "FlatFile has free space " +
                "(addr: #{pos}, len: #{header.length}) that is not in " +
                "FreeSpaceManager"
              errors += 1
            end
          end
        else
          unless @index.get(header.id) == pos
            PEROBS.log.error "FlatFile blob at address #{pos} is listed " +
              "in index with address #{@index.get(header.id)}"
            errors += 1
          end
        end
      end

      errors == 0
    end

    def discard_damaged_blob(addr, id)
      begin
        PEROBS.log.error "Discarding corrupted data blob for ID #{id}"
        @f.seek(addr)
        @f.write([ 0 ].pack('C'))
        @f.flush
      rescue IOError => e
        PEROBS.log.fatal "Cannot discard blob for ID #{id}: #{e.message}"
      end
    end

  end

end

