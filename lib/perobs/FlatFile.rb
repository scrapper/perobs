# encoding: UTF-8
#
# = FlatFile.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2018 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/IDList'

module PEROBS

  # The FlatFile class manages the storage file of the FlatFileDB. It contains
  # a sequence of blobs Each blob consists of header and the actual
  # blob data bytes.
  class FlatFile

    # The number of entries in a single BTree node of the index file.
    INDEX_BTREE_ORDER = 65

    # Create a new FlatFile object for a database in the given path.
    # @param dir [String] Directory path for the data base file
    def initialize(dir, progressmeter)
      @db_dir = dir
      @progressmeter = progressmeter
      @f = nil
      @index = BTree.new(@db_dir, 'index', INDEX_BTREE_ORDER, @progressmeter)
      @marks = nil
      @space_list = SpaceTree.new(@db_dir, @progressmeter)
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
      @f.sync = true

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
      if @marks
        @marks.erase
        @marks = nil
      end

      if @f
        @f.flush
        @f.flock(File::LOCK_UN)
        @f.fsync
        @f.close
        @f = nil
      end
    end

    # Force outstanding data to be written to the filesystem.
    def sync
      begin
        @f.flush
        @f.fsync
      rescue IOError => e
        PEROBS.log.fatal "Cannot sync flat file database: #{e.message}"
      end
      @index.sync
      @space_list.sync
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
      header = FlatFileBlobHeader.read(@f, addr, id)
      header.clear_flags
      @space_list.add_space(addr, header.length)
    end

    # Delete all unmarked objects.
    def delete_unmarked_objects
      deleted_objects_count = 0
      @progressmeter.start('Sweeping unmarked objects', @f.size) do |pm|
        each_blob_header do |pos, header|
          if header.is_valid? && !@marks.include?(header.id)
            delete_obj_by_address(pos, header.id)
            deleted_objects_count += 1
          end

          pm.update(pos)
        end
      end
      defragmentize

      deleted_objects_count
    end

    # Write the given object into the file. This method never uses in-place
    # updates for existing objects. A new copy is inserted first and only when
    # the insert was successful, the old copy is deleted and the index
    # updated.
    # @param id [Integer] ID of the object
    # @param raw_obj [String] Raw object as String
    # @return [Integer] position of the written blob in the blob file
    def write_obj_by_id(id, raw_obj)
      # Check if we have already an object with the given ID. We'll mark it as
      # outdated and save the header for later deletion. In case this
      # operation is aborted or interrupted we ensure that we either have the
      # old or the new version available.
      if (old_addr = find_obj_addr_by_id(id))
        old_header = FlatFileBlobHeader.read(@f, old_addr)
        old_header.set_outdated_flag
      end

      crc = checksum(raw_obj)

      # If the raw_obj is larger then 256 characters we will compress it to
      # safe some space in the database file. For smaller strings the
      # performance impact of compression is not compensated by writing
      # less data to the storage.
      compressed = false
      if raw_obj.bytesize > 256
        raw_obj = Zlib.deflate(raw_obj)
        compressed = true
      end

      addr, length = find_free_blob(raw_obj.bytesize)
      begin
        if length != -1
          # Just a safeguard so we don't overwrite current data.
          header = FlatFileBlobHeader.read(@f, addr)
          if header.length != length
            PEROBS.log.fatal "Length in free list (#{length}) and header " +
              "(#{header.length}) for address #{addr} don't match."
          end
          if raw_obj.bytesize > header.length
            PEROBS.log.fatal "Object (#{raw_obj.bytesize}) is longer than " +
              "blob space (#{header.length})."
          end
          if header.is_valid?
            PEROBS.log.fatal "Entry at address #{addr} with flags: " +
              "#{header.flags} is already used for ID #{header.id}."
          end
        end
        flags = 1 << FlatFileBlobHeader::VALID_FLAG_BIT
        flags |= (1 << FlatFileBlobHeader::COMPRESSED_FLAG_BIT) if compressed
        FlatFileBlobHeader.new(@f, addr, flags, raw_obj.bytesize, id, crc).write
        @f.write(raw_obj)
        if length != -1 && raw_obj.bytesize < length
          # The new object was not appended and it did not completely fill the
          # free space. So we have to write a new header to mark the remaining
          # empty space.
          unless length - raw_obj.bytesize >= FlatFileBlobHeader::LENGTH
            PEROBS.log.fatal "Not enough space to append the empty space " +
              "header (space: #{length} bytes, object: #{raw_obj.bytesize} " +
              "bytes)."
          end
          space_address = @f.pos
          space_length = length - FlatFileBlobHeader::LENGTH - raw_obj.bytesize
          FlatFileBlobHeader.new(@f, space_address, 0, space_length,
                                 0, 0).write
          # Register the new space with the space list.
          @space_list.add_space(space_address, space_length) if space_length > 0
        end

        # Once the blob has been written we can update the index as well.
        @index.insert(id, addr)

        if old_addr
          # If we had an existing object stored for the ID we have to mark
          # this entry as deleted now.
          old_header.clear_flags
          # And register the newly freed space with the space list.
          @space_list.add_space(old_addr, old_header.length)
        else
          @f.flush
        end
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

    # @return [Integer] Number of items stored in the DB.
    def item_counter
      @index.entries_count
    end

    # Read the object at the specified address.
    # @param addr [Integer] Offset in the flat file
    # @param id [Integer] ID of the data blob
    # @return [String] Raw object data
    def read_obj_by_address(addr, id)
      header = FlatFileBlobHeader.read(@f, addr, id)
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
      @marks.insert(id)
    end

    # Return true if the object with the given ID is marked, false otherwise.
    # @param id [Integer] ID of the object
    def is_marked_by_id?(id)
      @marks.include?(id)
    end

    # Clear alls marks.
    def clear_all_marks
      if @marks
        @marks.clear
      else
        @marks = IDList.new(@db_dir, 'marks', 8)
      end
    end

    # Eliminate all the holes in the file. This is an in-place
    # implementation. No additional space will be needed on the file system.
    def defragmentize
      distance = 0
      new_file_size = 0
      deleted_blobs = 0
      valid_blobs = 0

      # Iterate over all entries.
      @progressmeter.start('Defragmentizing FlatFile', @f.size) do |pm|
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
                FlatFileBlobHeader.new(@f, @f.pos, 0,
                                       distance - FlatFileBlobHeader::LENGTH,
                                       0, 0).write
                @f.flush
              rescue IOError => e
                PEROBS.log.fatal "Error while moving blob for ID " +
                  "#{header.id}: #{e.message}"
              end
            end
            new_file_size = pos + FlatFileBlobHeader::LENGTH + header.length
          else
            deleted_blobs += 1
            distance += entry_bytes
          end

          pm.update(pos)
        end
      end

      PEROBS.log.info "#{distance / 1000} KiB/#{deleted_blobs} blobs of " +
        "#{@f.size / 1000} KiB/#{valid_blobs} blobs or " +
        "#{'%.1f' % (distance.to_f / @f.size * 100.0)}% reclaimed"

      @f.flush
      @f.truncate(new_file_size)
      @f.flush
      @space_list.clear

      sync
    end

    # This method iterates over all entries in the FlatFile and removes the
    # entry and inserts it again. This is useful to update all entries in
    # case the storage format has changed.
    def refresh
      # This iteration might look scary as we iterate over the entries while
      # while we are rearranging them. Re-inserted items may be inserted
      # before or at the current entry and this is fine. They also may be
      # inserted after the current entry and will be re-read again unless they
      # are inserted after the original file end.
      file_size = @f.size
      @progressmeter.start('Refreshing objects', @f.size) do |pm|
        each_blob_header do |pos, header|
          if header.is_valid?
            buf = read_obj_by_address(pos, header.id)
            delete_obj_by_address(pos, header.id)
            write_obj_by_id(header.id, buf)
          end

          # Some re-inserted blobs may be inserted after the original file end.
          # No need to process those blobs again.
          break if pos >= file_size

          pm.update(pos)
        end
      end

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
      new_index = BTree.new(@db_dir, 'new-index', INDEX_BTREE_ORDER,
                            @progressmeter)
      new_index.erase
      new_index.open

      @progressmeter.start('Checking FlatFile blobs', @f.size) do |pm|
        each_blob_header do |pos, header|
          if header.is_valid?
            # We have a non-deleted entry.
            begin
              @f.seek(pos + FlatFileBlobHeader::LENGTH)
              buf = @f.read(header.length)
              if buf.bytesize != header.length
                PEROBS.log.error "Premature end of file in blob with ID " +
                  "#{header.id}."
                discard_damaged_blob(header) if repair
                errors += 1
                next
              end

              # Uncompress the data if the compression bit is set in the mark
              # byte.
              if header.is_compressed?
                begin
                  buf = Zlib.inflate(buf)
                rescue Zlib::BufError, Zlib::DataError
                  PEROBS.log.error "Corrupted compressed block with ID " +
                    "#{header.id} found."
                  discard_damaged_blob(header) if repair
                  errors += 1
                  next
                end
              end

              if header.crc && checksum(buf) != header.crc
                PEROBS.log.error "Checksum failure while checking blob " +
                  "with ID #{header.id}"
                discard_damaged_blob(header) if repair
                errors += 1
                next
              end
            rescue IOError => e
              PEROBS.log.fatal "Check of blob with ID #{header.id} failed: " +
                e.message
            end

            # Check if the ID has already been found in the file.
            if (previous_address = new_index.get(header.id))
              PEROBS.log.error "Multiple blobs for ID #{header.id} found. " +
                "Addresses: #{previous_address}, #{pos}"
              previous_header = FlatFileBlobHeader.read(@f, previous_address,
                                                        header.id)
              if repair
                # We have two blobs with the same ID and we must discard one of
                # them.
                if header.is_outdated?
                  discard_damaged_blob(header)
                elsif previous_header.is_outdated?
                  discard_damaged_blob(previous_header)
                else
                  PEROBS.log.error "None of the blobs with same ID have " +
                    "the outdated flag set. Deleting the smaller one."
                  discard_damaged_blob(header.length < previous_header.length ?
                                       header : previous_header)
                end
                next
              end
            else
              # ID is unique so far. Add it to the shadow index.
              new_index.insert(header.id, pos)
            end

          end

          pm.update(pos)
        end
      end
      # We no longer need the new index.
      new_index.close
      new_index.erase

      # Now we check the index data. It must be correct and the entries must
      # match the blob file. All entries in the index must be in the blob file
      # and vise versa.
      begin
        nodes = 0
        index_ok = false
        @progressmeter.start('Checking index', @index.entries_count) do |pm|
          index_ok = @index.check do |id, address|
            has_id_at?(id, address)
            pm.update(nodes += 1)
          end
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

      @progressmeter.start('Re-generating FlatFileDB index', @f.size) do |pm|
        each_blob_header do |pos, header|
          if header.is_valid?
            if (duplicate_pos = @index.get(header.id))
              PEROBS.log.error "FlatFile contains multiple blobs for ID " +
                "#{header.id}. First blob is at address #{duplicate_pos}. " +
                "Other blob found at address #{pos}."
              @space_list.add_space(pos, header.length) if header.length > 0
              discard_damaged_blob(header)
            else
              @index.insert(header.id, pos)
            end
          else
            @space_list.add_space(pos, header.length) if header.length > 0
          end

          pm.update(pos)
        end
      end

      sync
    end

    def has_space?(address, size)
      header = FlatFileBlobHeader.read(@f, address)
      !header.is_valid? && header.length == size
    end

    def has_id_at?(id, address)
      header = FlatFileBlobHeader.read(@f, address)
      header.is_valid? && header.id == id
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

    def FlatFile::insert_header_checksums(db_dir)
      old_file_name = File.join(db_dir, 'database.blobs')
      new_file_name = File.join(db_dir, 'database_v4.blobs')
      bak_file_name = File.join(db_dir, 'database_v3.blobs')

      old_file = File.open(old_file_name, 'rb')
      new_file = File.open(new_file_name, 'wb')

      entries = 0
      while (buf = old_file.read(21))
        flags, length, id, crc = *buf.unpack('CQQL')
        blob_data = old_file.read(length)

        # Some basic sanity checking to ensure all reserved bits are 0. Older
        # versions of PEROBS used to set bit 1 despite it being reserved now.
        unless flags & 0xF0 == 0
          PEROBS.log.fatal "Blob file #{old_file_name} contains illegal " +
            "flag byte #{'%02x' % flags} at #{old_file.pos - 21}"
        end

        # Check if the blob is valid and current.
        if flags & 0x1 == 1 && flags & 0x8 == 0
          # Make sure the bit 1 is not set anymore.
          flags = flags & 0x05
          header_str = [ flags, length, id, crc ].pack('CQQL')
          header_crc = Zlib.crc32(header_str, 0)
          header_str += [ header_crc ].pack('L')

          new_file.write(header_str + blob_data)
          entries += 1
        end
      end
      PEROBS.log.info "Header checksum added to #{entries} entries"

      old_file.close
      new_file.close

      File.rename(old_file_name, bak_file_name)
      File.rename(new_file_name, old_file_name)
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

      @progressmeter.start('Cross checking FlatFileDB', @f.size) do |pm|
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

          pm.update(pos)
        end
      end

      errors == 0
    end

    def discard_damaged_blob(header)
      PEROBS.log.error "Discarding corrupted data blob for ID #{header.id} " +
        "at offset #{header.addr}"
      header.clear_flags
    end

  end

end

