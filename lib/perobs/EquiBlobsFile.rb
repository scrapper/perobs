# encoding: UTF-8
#
# = EquiBlobsFile.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2017, 2018 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/Log'
require 'perobs/ProgressMeter'

module PEROBS

  # This class implements persistent storage space for same size data blobs.
  # The blobs can be stored and retrieved and can be deleted again. The
  # EquiBlobsFile manages the storage of the blobs and free storage
  # spaces. The files grows and shrinks as needed. A blob is referenced by its
  # address. The address is an Integer that must be larger than 0. The value 0
  # is used to represent an undefined address or nil. The file has a 4 * 8
  # bytes long header that stores the total entry count, the total space
  # count, the offset of the first entry and the offset of the first space.
  class EquiBlobsFile

    TOTAL_ENTRIES_OFFSET = 0
    TOTAL_SPACES_OFFSET = 8
    FIRST_ENTRY_OFFSET = 2 * 8
    FIRST_SPACE_OFFSET = 3 * 8
    HEADER_SIZE = 4 * 8

    attr_reader :total_entries, :total_spaces, :file_name, :first_entry

    # Create a new stack file in the given directory with the given file name.
    # @param dir [String] Directory
    # @param name [String] File name
    # @param entry_bytes [Integer] Number of bytes each entry must have
    # @param first_entry_default [Integer] Default address of the first blob
    def initialize(dir, name, entry_bytes, first_entry_default = 0)
      @file_name = File.join(dir, name + '.blobs')
      if entry_bytes < 8
        PEROBS.log.fatal "EquiBlobsFile entry size must be at least 8"
      end
      @entry_bytes = entry_bytes
      @first_entry_default = first_entry_default
      clear_custom_data
      reset_counters

      # The File handle.
      @f = nil
    end

    # Open the blob file.
    def open
      begin
        if File.exist?(@file_name)
          # Open an existing file.
          @f = File.open(@file_name, 'rb+')
          read_header
        else
          # Create a new file by writing a new header.
          @f = File.open(@file_name, 'wb+')
          write_header
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot open blob file #{@file_name}: #{e.message}"
      end
      unless @f.flock(File::LOCK_NB | File::LOCK_EX)
        PEROBS.log.fatal 'Database blob file is locked by another process'
      end
      @f.sync = true
    end

    # Close the blob file. This method must be called before the program is
    # terminated to avoid data loss.
    def close
      begin
        if @f
          @f.flush
          @f.flock(File::LOCK_UN)
          @f.fsync
          @f.close
          @f = nil
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot close blob file #{@file_name}: #{e.message}"
      end
    end

    # In addition to the standard offsets for the first entry and the first
    # space any number of additional data fields can be registered. This must be
    # done right after the object is instanciated and before the open() method
    # is called. Each field represents a 64 bit unsigned integer.
    # @param name [String] The label for this offset
    # @param default_value [Integer] The default value for the offset
    def register_custom_data(name, default_value = 0)
      if @custom_data_labels.include?(name)
        PEROBS.log.fatal "Custom data field #{name} has already been registered"
      end

      @custom_data_labels << name
      @custom_data_values << default_value
      @custom_data_defaults << default_value
    end

    # Reset (delete) all custom data labels that have been registered.
    def clear_custom_data
      unless @f.nil?
        PEROBS.log.fatal "clear_custom_data should only be called when " +
          "the file is not opened"
      end

      @custom_data_labels = []
      @custom_data_values = []
      @custom_data_defaults = []
    end

    # Set the registered custom data field to the given value.
    # @param name [String] Label of the offset
    # @param value [Integer] Value
    def set_custom_data(name, value)
      unless @custom_data_labels.include?(name)
        PEROBS.log.fatal "Unknown custom data field #{name}"
      end

      @custom_data_values[@custom_data_labels.index(name)] = value
      write_header if @f
    end

    # Get the registered custom data field value.
    # @param name [String] Label of the offset
    # @return [Integer] Value of the custom data field
    def get_custom_data(name)
      unless @custom_data_labels.include?(name)
        PEROBS.log.fatal "Unknown custom data field #{name}"
      end

      @custom_data_values[@custom_data_labels.index(name)]
    end

    # Erase the backing store. This method should only be called when the file
    # is not currently open.
    def erase
      @f = nil
      File.delete(@file_name) if File.exist?(@file_name)
      reset_counters
    end

    # Flush out all unwritten data.
    def sync
      begin
        if @f
          @f.flush
          @f.fsync
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot sync blob file #{@file_name}: #{e.message}"
      end
    end

    # Delete all data.
    def clear
      @f.truncate(0)
      @f.flush
      reset_counters
      write_header
    end

    # Change the address of the first blob.
    # @param address [Integer] New address
    def first_entry=(address)
      @first_entry = address
      write_header
    end

    # Return the address of a free blob storage space. Addresses start at 0
    # and increase linearly.
    # @return [Integer] address of a free blob space
    def free_address
      if @first_space == 0
        # There is currently no free entry. Create a new reserved entry at the
        # end of the file.
        begin
          offset = @f.size
          @f.seek(offset)
          write_n_bytes([1] + ::Array.new(@entry_bytes, 0))
          write_header
          return offset_to_address(offset)
        rescue IOError => e
          PEROBS.log.fatal "Cannot create reserved space at #{@first_space} " +
            "in EquiBlobsFile #{@file_name}: #{e.message}"
        end
      else
        begin
          free_space_address = offset_to_address(@first_space)
          @f.seek(@first_space)
          marker = read_char
          @first_space = read_unsigned_int
          unless marker == 0
            PEROBS.log.fatal "Free space list of EquiBlobsFile #{@file_name} " +
              "points to non-empty entry at address #{@first_space}"
          end
          # Mark entry as reserved by setting the mark byte to 1.
          @f.seek(-(1 + 8), IO::SEEK_CUR)
          write_char(1)

          # Update the file header
          @total_spaces -= 1
          write_header
          return free_space_address
        rescue IOError => e
          PEROBS.log.fatal "Cannot mark reserved space at " +
            "#{free_space_address} in EquiBlobsFile #{@file_name}: " +
            "#{e.message}"
        end
      end
    end

    # Store the given byte blob at the specified address. If the blob space is
    # already in use the content will be overwritten.
    # @param address [Integer] Address to store the blob
    # @param bytes [String] bytes to store
    def store_blob(address, bytes)
      unless address >= 0
        PEROBS.log.fatal "Blob storage address must be larger than 0, " +
          "not #{address}"
      end
      if bytes.length != @entry_bytes
        PEROBS.log.fatal "All stack entries must be #{@entry_bytes} " +
          "long. This entry is #{bytes.length} bytes long."
      end

      marker = 1
      begin
        offset = address_to_offset(address)
        if offset > (file_size = @f.size)
          PEROBS.log.fatal "Cannot store blob at address #{address} in " +
            "EquiBlobsFile #{@file_name}. Address is larger than file size. " +
            "Offset: #{offset}  File size: #{file_size}"
        end

        @f.seek(offset)
        # The first byte is the marker byte. It's set to 2 for cells that hold
        # a blob. 1 for reserved cells and 0 for empty cells. The cell must be
        # either already be in use or be reserved. It must not be 0.
        if file_size > offset &&
           (marker = read_char) != 1 && marker != 2
          PEROBS.log.fatal "Marker for entry at address #{address} of " +
            "EquiBlobsFile #{@file_name} must be 1 or 2 but is #{marker}"
        end
        @f.seek(offset)
        write_char(2)
        @f.write(bytes)
        @f.flush
      rescue IOError => e
        PEROBS.log.fatal "Cannot store blob at address #{address} in " +
          "EquiBlobsFile #{@file_name}: #{e.message}"
      end

      # Update the entries counter if we inserted a new blob.
      if marker == 1
        @total_entries += 1
        write_header
      end
    end

    # Retrieve a blob from the given address.
    # @param address [Integer] Address to store the blob
    # @return [String] blob bytes
    def retrieve_blob(address)
      unless address > 0
        PEROBS.log.fatal "Blob retrieval address must be larger than 0, " +
          "not #{address}"
      end

      begin
        if (offset = address_to_offset(address)) >= @f.size
          PEROBS.log.fatal "Cannot retrieve blob at address #{address} " +
            "of EquiBlobsFile #{@file_name}. Address is beyond end of file."
        end

        @f.seek(address_to_offset(address))
        if (marker = read_char) != 2
          PEROBS.log.fatal "Cannot retrieve blob at address #{address} " +
            "of EquiBlobsFile #{@file_name}. Blob is " +
            (marker == 0 ? 'empty' : marker == 1 ? 'reserved' : 'corrupted') +
            '.'
        end
        bytes = @f.read(@entry_bytes)
      rescue IOError => e
        PEROBS.log.fatal "Cannot retrieve blob at adress #{address} " +
          "of EquiBlobsFile #{@file_name}: " + e.message
      end

      bytes
    end

    # Delete the blob at the given address.
    # @param address [Integer] Address of blob to delete
    def delete_blob(address)
      unless address >= 0
        PEROBS.log.fatal "Blob address must be larger than 0, " +
          "not #{address}"
      end

      offset = address_to_offset(address)
      begin
        @f.seek(offset)
        if (marker = read_char) != 1 && marker != 2
          PEROBS.log.fatal "Cannot delete blob stored at address #{address} " +
            "of EquiBlobsFile #{@file_name}. Blob is " +
            (marker == 0 ? 'empty' : 'corrupted') + '.'
        end
        @f.seek(address_to_offset(address))
        write_char(0)
        write_unsigned_int(@first_space)
      rescue IOError => e
        PEROBS.log.fatal "Cannot delete blob at address #{address}: " +
          e.message
      end

      @first_space = offset
      @total_spaces += 1
      @total_entries -= 1
      write_header

      if offset == @f.size - 1 - @entry_bytes
        # We have deleted the last entry in the file. Make sure that all empty
        # entries are removed up to the now new last used entry.
        trim_file
      end
    end

    # Check the file for logical errors.
    # @return [Boolean] true of file has no errors, false otherwise.
    def check
      return false unless check_spaces
      return false unless check_entries

      expected_size = address_to_offset(@total_entries + @total_spaces + 1)
      actual_size = @f.size
      if actual_size != expected_size
        PEROBS.log.error "Size mismatch in EquiBlobsFile #{@file_name}. " +
          "Expected #{expected_size} bytes but found #{actual_size} bytes."
        return false
      end

      true
    end

    # Check if the file exists and is larger than 0.
    def file_exist?
      File.exist?(@file_name) && File.size(@file_name) > 0
    end

    private

    def reset_counters
      # The total number of entries stored in the file.
      @total_entries = 0
      # The total number of spaces (empty entries) in the file.
      @total_spaces = 0
      # The address of the first entry.
      @first_entry = @first_entry_default
      # The file offset of the first empty entry.
      @first_space = 0

      # Copy default custom values
      @custom_data_values = @custom_data_defaults.dup
    end

    def read_header
      begin
        @f.seek(0)
        @total_entries, @total_spaces, @first_entry, @first_space =
          @f.read(HEADER_SIZE).unpack('QQQQ')
        custom_labels_count = @custom_data_labels.length
        if custom_labels_count > 0
          @custom_data_values =
            @f.read(custom_labels_count * 8).unpack("Q#{custom_labels_count}")
        end

      rescue IOError => e
        PEROBS.log.fatal "Cannot read EquiBlobsFile header: #{e.message}"
      end
    end

    def write_header
      header_ary = [ @total_entries, @total_spaces, @first_entry, @first_space ]
      begin
        @f.seek(0)
        @f.write(header_ary.pack('QQQQ'))
        unless @custom_data_values.empty?
          @f.write(@custom_data_values.
                   pack("Q#{@custom_data_values.length}"))
        end
        @f.flush
      end
    end

    def check_spaces
      begin
        # Read and check total space count
        @f.seek(TOTAL_SPACES_OFFSET)
        total_spaces = read_unsigned_int
        unless total_spaces == @total_spaces
          PEROBS.log.error "Mismatch in total space count in EquiBlobsFile " +
            "#{@file_name}. Memory: #{@total_spaces}  File: #{total_spaces}"
          return false
        end

        # Read offset of first empty space
        @f.seek(FIRST_SPACE_OFFSET)
        next_offset = read_unsigned_int
      rescue IOError => e
        PEROBS.log.error "Cannot check header of EquiBlobsFile " +
          "#{@file_name}: #{e.message}"
        return false
      end

      return true if next_offset == 0

      total_spaces = 0
      ProgressMeter.new('Checking EquiBlobsFile spaces list',
                        @total_spaces) do |pm|
        begin
          while next_offset != 0
            # Check that the marker byte is 0
            @f.seek(next_offset)
            if (marker = read_char) != 0
              PEROBS.log.error "Marker byte at address " +
                "#{offset_to_address(next_offset)} is #{marker} instead of 0."
              return false
            end
            # Read offset of next empty space
            next_offset = read_unsigned_int

            total_spaces += 1
            pm.update(total_spaces)
          end
        rescue IOError => e
          PEROBS.log.error "Cannot check space list of EquiBlobsFile " +
            "#{@file_name}: #{e.message}"
          return false
        end
      end

      unless total_spaces == @total_spaces
        PEROBS.log.error "Mismatch between space counter and entries in " +
          "EquiBlobsFile #{@file_name}. Counter: #{@total_spaces}  " +
          "Entries: #{total_spaces}"
        return false
      end

      true
    end

    def check_entries
      begin
        # Read total entry count
        @f.seek(TOTAL_ENTRIES_OFFSET)
        total_entries = read_unsigned_int
        unless total_entries == @total_entries
          PEROBS.log.error "Mismatch in total entry count in EquiBlobsFile " +
            "#{@file_name}. Memory: #{@total_entries}  File: #{total_entries}"
          return false
        end
      rescue IOError => e
        PEROBS.log.error "Cannot check header of EquiBlobsFile " +
          "#{@file_name}: #{e.message}"
        return false
      end

      next_offset = address_to_offset(1)
      total_entries = 0
      total_spaces = 0
      ProgressMeter.new('Checking EquiBlobsFile entries',
                        @total_spaces + @total_entries) do |pm|
        begin
          @f.seek(next_offset)
          while !@f.eof
            marker, bytes = @f.read(1 + @entry_bytes).
              unpack("C#{1 + @entry_bytes}")
            case marker
            when 0
              total_spaces += 1
            when 1
              PEROBS.log.error "Entry at address " +
                "#{offset_to_address(next_offset)} in EquiBlobsFile " +
                "#{@file_name} has reserved marker"
              return false
            when 2
              total_entries += 1
            else
              PEROBS.log.error "Entry at address " +
                "#{offset_to_address(next_offset)} in EquiBlobsFile " +
                "#{@file_name} has illegal marker #{marker}"
              return false
            end
            next_offset += 1 + @entry_bytes
          end

          pm.update(total_spaces + total_entries)
        rescue
          PEROBS.log.error "Cannot check entries of EquiBlobsFile " +
            "#{@file_name}: #{e.message}"
          return false
        end
      end

      unless total_spaces == @total_spaces
        PEROBS.log.error "Mismatch between space counter and spaces in " +
          "EquiBlobsFile #{@file_name}. Counter: #{@total_spaces}  " +
          "Found spaces: #{total_spaces}"
        return false
      end
      unless total_entries == @total_entries
        PEROBS.log.error "Mismatch between entries counter and entries in " +
          "EquiBlobsFile #{@file_name}. Counter: #{@total_entries}  " +
          "Found entries: #{total_entries}"
        return false
      end

      true
    end

    def trim_file
      offset = @f.size - 1 - @entry_bytes
      while offset >= address_to_offset(1)
        @f.seek(offset)
        begin
          if (marker = read_char) == 0
            # This entry is a deleted entry
            unlink_space(offset)
          else
            # No more empty entries at the end of the file to trim.
            return
          end
          @f.truncate(offset)
          @f.flush
        rescue IOError => e
          PEROBS.log.fatal "Error while trimming EquiBlobsFile " +
            "#{@file_name}: #{e.message}"
        end

        # Push offset to the previous entry
        offset -= 1 + @entry_bytes
      end
    end

    def unlink_space(offset_to_unlink)
      # The space list entry that points to offset_to_unlink will be
      # eliminated.
      #
      # This is the offset in the file that potentially holds the offset to
      # the entry that we want to eliminate. If so, it will be overwritten
      # with the offset that the eliminated entry was pointing to.
      offset_to_modify = FIRST_SPACE_OFFSET

      begin
        # Read the offset that potentially points to the entry to be
        # eliminated.
        @f.seek(offset_to_modify)
        if (next_entry_offset = read_unsigned_int) == 0
          PEROBS.log.fatal "Cannot delete space from an empty space list"
        end

        loop do
          # Get the offset of the successor in the chain.
          @f.seek(next_entry_offset)
          # Check that it really is a empty space entry.
          unless (marker = read_char) == 0
            PEROBS.log.fatal "Marker of space must be 0"
          end
          offset_to_insert = read_unsigned_int

          if next_entry_offset == offset_to_unlink
            # We've found the entry that we want to eliminate.
            # Rewind to the location of the offset that pointed to the entry
            # to eliminate.
            @f.seek(offset_to_modify)
            # Write the offset of the successor to this offset.
            write_unsigned_int(offset_to_insert)
            if offset_to_modify == FIRST_SPACE_OFFSET
              # When we modify the first space entry we also have to update
              # the in-memory value.
              @first_space = offset_to_insert
            end

            # Reduce the space counter.
            @total_spaces -= 1
            write_header

            return
          end

          # Check if we've reached the end of the chain.
          return if offset_to_insert == 0

          # The next_entry_offset did not match the offset to unlink. Shift
          # offset_to_modify and next_entry_offset to their successor entries.
          offset_to_modify = next_entry_offset + 1
          next_entry_offset = offset_to_insert
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot unlink space of EquiBlobsFile " +
          "#{@file_name}: #{e.message}"
      end
    end

    # Translate a blob address to the actual offset in the file.
    def address_to_offset(address)
      # Since address 0 is illegal, we can use address - 1 as index here.
      HEADER_SIZE + @custom_data_labels.length * 8 +
        (address - 1) * (1 + @entry_bytes)
    end

    # Translate the file offset to the address of a blob.
    def offset_to_address(offset)
      (offset - HEADER_SIZE - @custom_data_labels.length * 8) /
        (1 + @entry_bytes) + 1
    end

    def write_char(c)
      @f.write([ c ].pack('C'))
    end

    def read_char
      @f.read(1).unpack('C')[0]
    end

    def write_unsigned_int(uint)
      @f.write([ uint ].pack('Q'))
    end

    def write_n_bytes(bytes)
      @f.write(bytes.pack("C#{bytes.size}"))
    end

    def read_unsigned_int
      @f.read(8).unpack('Q')[0]
    end

  end

end

