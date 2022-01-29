# encoding: UTF-8
#
# = FlatFile.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2018, 2019 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/SpaceManager'
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
      @marks = nil
      @index = BTree.new(@db_dir, 'index', INDEX_BTREE_ORDER, @progressmeter)
      old_spaces_file = File.join(@db_dir, 'database_spaces.blobs')
      if File.exist?(old_spaces_file)
        # PEROBS version 4.1.0 and earlier used this space list format. It is
        # deprecated now. Newly created DBs use the SpaceManager format.
        @space_list = SpaceTree.new(@db_dir, @progressmeter)
      else
        @space_list = SpaceManager.new(@db_dir, @progressmeter)
      end
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

      open_index_files(!new_db_created)
    end

    # Close the flat file. This method must be called to ensure that all data
    # is really written into the filesystem.
    def close
      @space_list.close if @space_list.is_open?
      @index.close if @index.is_open?

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
      @index.remove(id) if @index.is_open?
      header = FlatFileBlobHeader.read(@f, addr, id)
      header.clear_flags
      @space_list.add_space(addr, header.length) if @space_list.is_open?
    end

    # Delete all unmarked objects.
    def delete_unmarked_objects(&block)
      # We don't update the index and the space list during this operation as
      # we defragmentize the blob file at the end. We'll end the operation
      # with an empty space list.
      clear_index_files

      deleted_objects_count = 0
      @progressmeter.start('Sweeping unmarked objects', @f.size) do |pm|
        each_blob_header do |header|
          if header.is_valid? && !@marks.include?(header.id)
            delete_obj_by_address(header.addr, header.id)
            yield(header.id) if block_given?
            deleted_objects_count += 1
          end

          pm.update(header.addr)
        end
      end
      defragmentize

      # Update the index file and create a new, empty space list.
      regenerate_index_and_spaces

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
      raw_obj_bytesize = raw_obj.bytesize
      if raw_obj_bytesize > 256
        raw_obj = Zlib.deflate(raw_obj)
        raw_obj_bytesize = raw_obj.bytesize
        compressed = true
      end

      addr, length = find_free_blob(raw_obj_bytesize)
      begin
        if length != -1
          # Just a safeguard so we don't overwrite current data.
          header = FlatFileBlobHeader.read(@f, addr)
          if header.length != length
            PEROBS.log.fatal "Length in free list (#{length}) and header " +
              "(#{header.length}) for address #{addr} don't match."
          end
          if raw_obj_bytesize > header.length
            PEROBS.log.fatal "Object (#{raw_obj_bytesize}) is longer than " +
              "blob space (#{header.length})."
          end
          if header.is_valid?
            PEROBS.log.fatal "Entry at address #{addr} with flags: " +
              "#{header.flags} is already used for ID #{header.id}."
          end
        end
        flags = 1 << FlatFileBlobHeader::VALID_FLAG_BIT
        flags |= (1 << FlatFileBlobHeader::COMPRESSED_FLAG_BIT) if compressed
        FlatFileBlobHeader.new(@f, addr, flags, raw_obj_bytesize, id, crc).write
        @f.write(raw_obj)
        @f.flush
        if length != -1 && raw_obj_bytesize < length
          # The new object was not appended and it did not completely fill the
          # free space. So we have to write a new header to mark the remaining
          # empty space.
          unless length - raw_obj_bytesize >= FlatFileBlobHeader::LENGTH
            PEROBS.log.fatal "Not enough space to append the empty space " +
              "header (space: #{length} bytes, object: #{raw_obj_bytesize} " +
              "bytes)."
          end
          space_address = @f.pos
          space_length = length - FlatFileBlobHeader::LENGTH - raw_obj_bytesize
          FlatFileBlobHeader.new(@f, space_address, 0, space_length,
                                 0, 0).write
          # Register the new space with the space list.
          if @space_list.is_open? && space_length > 0
            @space_list.add_space(space_address, space_length)
          end
        end

        # Once the blob has been written we can update the index as well.
        @index.insert(id, addr) if @index.is_open?

        if old_addr
          # If we had an existing object stored for the ID we have to mark
          # this entry as deleted now.
          old_header.clear_flags
          @f.flush
          # And register the newly freed space with the space list.
          if @space_list.is_open?
            @space_list.add_space(old_addr, old_header.length)
          end
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
          "#{id} points to object with ID #{header.id} at address #{addr}"
      end

      buf = nil

      begin
        @f.seek(addr + FlatFileBlobHeader::LENGTH)
        buf = @f.read(header.length)
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob for ID #{id} at address #{addr}: " +
          e.message
      end

      # Uncompress the data if the compression bit is set in the flags byte.
      if header.is_compressed?
        begin
          buf = Zlib.inflate(buf)
        rescue Zlib::BufError, Zlib::DataError
          PEROBS.log.fatal "Corrupted compressed block with ID " +
            "#{id} found at address #{addr}."
        end
      end

      if checksum(buf) != header.crc
        PEROBS.log.fatal "Checksum failure while reading blob ID #{id} " +
          "at address #{addr}"
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
        @marks = IDList.new(@db_dir, 'marks', item_counter)
      end
    end

    # Eliminate all the holes in the file. This is an in-place
    # implementation. No additional space will be needed on the file system.
    def defragmentize
      distance = 0
      new_file_size = 0
      deleted_blobs = 0
      corrupted_blobs = 0
      valid_blobs = 0

      # Iterate over all entries.
      @progressmeter.start('Defragmenting blobs file', @f.size) do |pm|
        each_blob_header do |header|
          # If we have stumbled over a corrupted blob we treat it similar to a
          # deleted blob and reuse the space.
          if header.corruption_start
            distance += header.addr - header.corruption_start
            corrupted_blobs += 1
          end

          # Total size of the current entry
          entry_bytes = FlatFileBlobHeader::LENGTH + header.length
          if header.is_valid?
            # We have found a valid entry.
            valid_blobs += 1
            if distance > 0
              begin
                # Read current entry into a buffer
                @f.seek(header.addr)
                buf = @f.read(entry_bytes)
                # Write the buffer right after the end of the previous entry.
                @f.seek(header.addr - distance)
                @f.write(buf)
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
            new_file_size = header.addr - distance +
              FlatFileBlobHeader::LENGTH + header.length
          else
            deleted_blobs += 1
            distance += entry_bytes
          end

          pm.update(header.addr)
        end
      end

      PEROBS.log.info "#{distance / 1000} KiB/#{deleted_blobs} blobs of " +
        "#{@f.size / 1000} KiB/#{valid_blobs} blobs or " +
        "#{'%.1f' % (distance.to_f / @f.size * 100.0)}% reclaimed"
      if corrupted_blobs > 0
        PEROBS.log.info "#{corrupted_blobs} corrupted blob(s) found. Space " +
          "was recycled."
      end

      @f.flush
      @f.truncate(new_file_size)
      @f.flush

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

      # We don't update the index and the space list during this operation as
      # we defragmentize the blob file at the end. We'll end the operation
      # with an empty space list.
      clear_index_files

      @progressmeter.start('Converting objects to new storage format',
                           @f.size) do |pm|
        each_blob_header do |header|
          if header.is_valid?
            buf = read_obj_by_address(header.addr, header.id)
            delete_obj_by_address(header.addr, header.id)
            write_obj_by_id(header.id, buf)
          end

          # Some re-inserted blobs may be inserted after the original file end.
          # No need to process those blobs again.
          break if header.addr >= file_size

          pm.update(header.addr)
        end
      end

      # Reclaim the space saved by compressing entries.
      defragmentize

      # Recreate the index file and create an empty space list.
      regenerate_index_and_spaces
    end

    # Check the FlatFile.
    # @return [Integer] Number of errors found
    def check()
      errors = 0
      return errors unless @f

      t = Time.now
      PEROBS.log.info "Checking FlatFile database..."

      # First check the database blob file. Each entry should be readable and
      # correct and all IDs must be unique. We use a shadow index to keep
      # track of the already found IDs.
      new_index = BTree.new(@db_dir, 'new-index', INDEX_BTREE_ORDER,
                            @progressmeter)
      new_index.erase
      new_index.open

      corrupted_blobs = 0
      end_of_last_healthy_blob = nil
      @progressmeter.start('Checking blobs file', @f.size) do |pm|
        corrupted_blobs = each_blob_header do |header|
          if header.is_valid?
            # We have a non-deleted entry.
            begin
              @f.seek(header.addr + FlatFileBlobHeader::LENGTH)
              buf = @f.read(header.length)
              if buf.bytesize != header.length
                PEROBS.log.error "Premature end of file in blob with ID " +
                  "#{header.id}."
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
                  errors += 1
                  next
                end
              end

              if header.crc && checksum(buf) != header.crc
                PEROBS.log.error "Checksum failure while checking blob " +
                  "with ID #{header.id}"
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
                "Addresses: #{previous_address}, #{header.addr}"
              errors += 1
              previous_header = FlatFileBlobHeader.read(@f, previous_address,
                                                        header.id)
            else
              # ID is unique so far. Add it to the shadow index.
              new_index.insert(header.id, header.addr)
            end
          end
          end_of_last_healthy_blob = header.addr +
            FlatFileBlobHeader::LENGTH + header.length

          pm.update(header.addr)
        end

        if end_of_last_healthy_blob && end_of_last_healthy_blob != @f.size
          # The blob file ends with a corrupted blob header.
          PEROBS.log.error "#{@f.size - end_of_last_healthy_blob} corrupted " +
            'bytes found at the end of FlatFile.'
          corrupted_blobs += 1
        end

        errors += corrupted_blobs
      end

      # We no longer need the new index.
      new_index.close
      new_index.erase

      if corrupted_blobs == 0
        # Now we check the index data. It must be correct and the entries must
        # match the blob file. All entries in the index must be in the blob file
        # and vise versa.
        begin
          index_ok = @index.check do |id, address|
            unless has_id_at?(id, address)
              PEROBS.log.error "Index contains an entry for " +
                "ID #{id} at address #{address} that is not in FlatFile"
              false
            else
              true
            end
          end
          x_check_errs = 0
          space_check_ok = true
          unless index_ok && (space_check_ok = @space_list.check(self)) &&
            (x_check_errs = cross_check_entries) == 0
            errors += 1 unless index_ok && space_check_ok
            errors += x_check_errs
          end
        rescue PEROBS::FatalError
          errors += 1
        end
      end

      PEROBS.log.info "FlatFile check completed in #{Time.now - t} seconds. " +
        "#{errors} errors found."

      errors
    end

    # Repair the FlatFile. In contrast to the repair functionality in the
    # check() method this method is much faster. It simply re-creates the
    # index and space list from the blob file.
    # @return [Integer] Number of errors found
    def repair
      errors = 0
      return errors unless @f

      t = Time.now
      PEROBS.log.info "Repairing FlatFile database"

      # Erase and re-open the index and space list files. We purposely don't
      # close the files at it would trigger needless flushing.
      clear_index_files(true)

      # Now we scan the blob file and re-index all blobs and spaces. Corrupted
      # blobs will be skipped.
      corrupted_blobs = 0
      end_of_last_healthy_blob = nil
      @progressmeter.start('Re-indexing blobs file', @f.size) do |pm|
        corrupted_blobs = each_blob_header do |header|
          if header.corruption_start
            # The blob is preceeded by a corrupted area. We create a new
            # header of a deleted blob for this area and write the new blob
            # over it.
            if (data_length = header.addr - header.corruption_start -
                FlatFileBlobHeader::LENGTH) <= 0
              PEROBS.log.error "Found a corrupted blob that is too small to " +
                "fit a header (#{data_length}). File must be defragmented."
            else
              new_header = FlatFileBlobHeader.new(@f, header.corruption_start,
                                                  0, data_length, 0, 0)
              new_header.write
              @space_list.add_space(header.corruption_start, data_length)
            end
          end

          if header.is_valid?
            # We have a non-deleted entry.
            begin
              @f.seek(header.addr + FlatFileBlobHeader::LENGTH)
              buf = @f.read(header.length)
              if buf.bytesize != header.length
                PEROBS.log.error "Premature end of file in blob with ID " +
                  "#{header.id}."
                discard_damaged_blob(header)
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
                  discard_damaged_blob(header)
                  errors += 1
                  next
                end
              end

              if header.crc && checksum(buf) != header.crc
                PEROBS.log.error "Checksum failure while checking blob " +
                  "with ID #{header.id}"
                discard_damaged_blob(header)
                errors += 1
                next
              end
            rescue IOError => e
              PEROBS.log.fatal "Check of blob with ID #{header.id} failed: " +
                e.message
            end

            # Check if the ID has already been found in the file.
            if (previous_address = @index.get(header.id))
              PEROBS.log.error "Multiple blobs for ID #{header.id} found. " +
                "Addresses: #{previous_address}, #{header.addr}"
              errors += 1
              previous_header = FlatFileBlobHeader.read(@f, previous_address,
                                                        header.id)
              # We have two blobs with the same ID and we must discard one of
              # them.
              discard_duplicate_blobs(header, previous_header)
            else
              # ID is unique so far. Add it to the shadow index.
              @index.insert(header.id, header.addr)
            end

          else
            if header.length > 0
              @space_list.add_space(header.addr, header.length)
            end
          end
          end_of_last_healthy_blob = header.addr +
            FlatFileBlobHeader::LENGTH + header.length

          pm.update(header.addr)
        end

        if end_of_last_healthy_blob && end_of_last_healthy_blob != @f.size
          # The blob file ends with a corrupted blob header.
          PEROBS.log.error "#{@f.size - end_of_last_healthy_blob} corrupted " +
            'bytes found at the end of FlatFile.'
          corrupted_blobs += 1

          PEROBS.log.error "Truncating FlatFile to " +
            "#{end_of_last_healthy_blob} bytes by discarding " +
            "#{@f.size - end_of_last_healthy_blob} bytes"
          @f.truncate(end_of_last_healthy_blob)
        end

        errors += corrupted_blobs
      end

      sync
      PEROBS.log.info "FlatFile repair completed in #{Time.now - t} seconds. " +
        "#{errors} errors found."

      errors
    end

    # This method clears the index tree and the free space list and
    # regenerates them from the FlatFile.
    def regenerate_index_and_spaces
      PEROBS.log.warn "Re-generating FlatFileDB index and space files"
      @index.open unless @index.is_open?
      @index.clear
      @space_list.open unless @space_list.is_open?
      @space_list.clear

      @progressmeter.start('Re-generating database index', @f.size) do |pm|
        each_blob_header do |header|
          if header.is_valid?
            if (duplicate_pos = @index.get(header.id))
              PEROBS.log.error "FlatFile contains multiple blobs for ID " +
                "#{header.id}. First blob is at address #{duplicate_pos}. " +
                "Other blob found at address #{header.addr}."
              if header.length > 0
                @space_list.add_space(header.addr, header.length)
              end
              discard_damaged_blob(header)
            else
              @index.insert(header.id, header.addr)
            end
          else
            if header.length > 0
              @space_list.add_space(header.addr, header.length)
            end
          end

          pm.update(header.addr)
        end
      end

      sync
    end

    def has_space?(address, size)
      header = FlatFileBlobHeader.read(@f, address)
      !header.is_valid? && header.length == size
    end

    def has_id_at?(id, address)
      begin
        header = FlatFileBlobHeader.read(@f, address)
      rescue PEROBS::FatalError
        return false
      end
      header.is_valid? && header.id == id
    end

    def inspect
      s = '['
      each_blob_header do |header|
        s << "{ :pos => #{header.addr}, :flags => #{header.flags}, " +
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
      corrupted_blobs = 0

      begin
        @f.seek(0)
        while (header = FlatFileBlobHeader.read(@f))
          if header.corruption_start
            corrupted_blobs += 1
          end

          yield(header)

          @f.seek(header.addr + FlatFileBlobHeader::LENGTH + header.length)
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob in flat file DB: #{e.message}"
      end

      corrupted_blobs
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

      @progressmeter.start('Cross checking blobs and index', @f.size) do |pm|
        each_blob_header do |header|
          if !header.is_valid?
            if header.length > 0
              unless @space_list.has_space?(header.addr, header.length)
                PEROBS.log.error "FlatFile has free space " +
                  "(addr: #{header.addr}, len: #{header.length}) that is " +
                  "not in SpaceManager"
                errors += 1
              end
            end
          else
            if (index_address = @index.get(header.id)).nil?
              PEROBS.log.error "FlatFile blob at address #{header.addr} " +
                "is not listed in the index"
              errors +=1
            elsif index_address != header.addr
                PEROBS.log.error "FlatFile blob at address #{header.addr} " +
                  "is listed in index with address #{index_address}"
                errors += 1
            end
          end

          pm.update(header.addr)
        end
      end

      errors
    end

    def discard_damaged_blob(header)
      PEROBS.log.error "Discarding corrupted data blob for ID #{header.id} " +
        "at offset #{header.addr}"
      header.clear_flags
    end

    def discard_duplicate_blobs(header, previous_header)
      if header.is_outdated?
        discard_damaged_blob(header)
      elsif previous_header.is_outdated?
        discard_damaged_blob(previous_header)
      else
        smaller, larger = header.length < previous_header.length ?
          [ header, previous_header ] : [ previous_header, header ]
        PEROBS.log.error "None of the blobs with same ID have " +
          "the outdated flag set. Deleting the smaller one " +
          "at address #{smaller.addr}"
        discard_damaged_blob(smaller)
        @space_list.add_space(smaller.addr, smaller.length)
        @index.insert(larger.id, larger.addr)
      end
    end

    def open_index_files(abort_on_missing_files = false)
      begin
        @index.open(abort_on_missing_files)
        @space_list.open
      rescue FatalError
        clear_index_files
        regenerate_index_and_spaces
      end
    end

    def erase_index_files(dont_close_files = false)
      # Ensure that the index is really closed.
      @index.close unless dont_close_files
      # Erase it completely
      @index.erase

      # Ensure that the spaces list is really closed.
      @space_list.close unless dont_close_files
      # Erase it completely
      @space_list.erase

      if @space_list.is_a?(SpaceTree)
        # If we still use the old SpaceTree format, this is the moment to
        # convert it to the new SpaceManager format.
        @space_list = SpaceManager.new(@db_dir, @progressmeter)
        PEROBS.log.warn "Converting space list from SpaceTree format " +
          "to SpaceManager format"
      end
    end

    def clear_index_files(dont_close_files = false)
      erase_index_files(dont_close_files)

      # Then create them again.
      @index.open
      @space_list.open
    end

  end

end

