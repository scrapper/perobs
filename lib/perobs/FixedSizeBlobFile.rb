# encoding: UTF-8
#
# = FixedSizeBlobFile.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'
require 'perobs/StackFile'

module PEROBS

  # This class implements persistent storage space for fixed size data blobs.
  # The blobs can be stored and retrieved and can be deleted again. The
  # FixedSizeBlobFile manages the storage of the blobs and free storage
  # spaces. The files grows and shrinks as needed. A blob is referenced by its
  # address.
  class FixedSizeBlobFile

    # Create a new stack file in the given directory with the given file name.
    # @param dir [String] Directory
    # @param name [String] File name
    # @param entry_bytes [Fixnum] Number of bytes each entry must have
    def initialize(dir, name, entry_bytes)
      @file_name = File.join(dir, name + '.blobs')
      @entry_bytes = entry_bytes
      @free_list = StackFile.new(dir, name + '-freelist', 8)
      @f = nil
    end

    # Open the blob file.
    def open
      begin
        if File.exist?(@file_name)
          @f = File.open(@file_name, 'rb+')
        else
          @f = File.open(@file_name, 'wb+')
        end
      rescue IOError => e
        PEROBS.log.fatal "Cannot open blob file #{@file_name}: #{e.message}"
      end
      @free_list.open
    end

    # Close the blob file. This method must be called before the program is
    # terminated to avoid data loss.
    def close
      @free_list.close
      begin
        @f.flush
        @f.close
      rescue IOError => e
        PEROBS.log.fatal "Cannot close blob file #{@file_name}: #{e.message}"
      end
    end

    # Flush out all unwritten data.
    def sync
      @free_list.sync
      begin
        @f.sync
      rescue IOError => e
        PEROBS.log.fatal "Cannot sync blob file #{@file_name}: #{e.message}"
      end
    end

    # Delete all data.
    def clear
      @f.truncate(0)
      @f.flush
      @free_list.clear
    end

    # Return the address of a free blob storage space. Addresses start at 0
    # and increase linearly.
    # @return [Fixnum] address of a free blob space
    def free_address
      if (bytes = @free_list.pop)
        # Return an entry from the free list.
        return bytes.unpack('Q')[0]
      else
        # There is currently no free entry. Return the address at the end of
        # the file.
        offset_to_address(@f.size)
      end
    end

    # Store the given byte blob at the specified address. If the blob space is
    # already in use the content will be overwritten.
    # @param address [Fixnum] Address to store the blob
    # @param bytes [String] bytes to store
    def store_blob(address, bytes)
      if bytes.length != @entry_bytes
        PEROBS.log.fatal "All stack entries must be #{@entry_bytes} " +
          "long. This entry is #{bytes.length} bytes long."
      end
      begin
        @f.seek(address_to_offset(address))
        # The first byte is tha flag byte. It's set to 1 for cells that hold a
        # blob. 0 for empty cells.
        @f.write([ 1 ].pack('C'))
        @f.write(bytes)
        @f.flush
      rescue IOError => e
        PEROBS.log.fatal "Cannot store blob at address #{address}: #{e.message}"
      end
    end

    # Retrieve a blob from the given address.
    # @param address [Fixnum] Address to store the blob
    # @return [String] blob bytes
    def retrieve_blob(address)
      begin
        if (offset = address_to_offset(address)) >= @f.size
          return nil
        end

        @f.seek(address_to_offset(address))
        if (@f.read(1).unpack('C')[0] != 1)
          return nil
        end
        bytes = @f.read(@entry_bytes)
      rescue IOError => e
        PEROBS.log.fatal "Cannot retrieve blob at adress #{address}: " +
          e.message
      end

      bytes
    end

    # Delete the blob at the given address.
    # @param address [Fixnum] Address of blob to delete
    def delete_blob(address)
      begin
        @f.seek(address_to_offset(address))
        if (@f.read(1).unpack('C')[0] != 1)
          PEROBS.log.fatal "There is no blob stored at address #{address}"
        end
        @f.seek(address_to_offset(address))
        @f.write([ 0 ].pack('C'))
      rescue IOError => e
        PEROBS.log.fatal "Cannot delete blob at address #{address}: " +
          e.message
      end
      # Add the address to the free list.
      @free_list.push([ address ].pack('Q'))
    end

    private

    # Translate a blob address to the actual offset in the file.
    def address_to_offset(address)
      address * (1 + @entry_bytes)
    end

    # Translate the file offset to the address of a blob.
    def offset_to_address(offset)
      offset / (1 + @entry_bytes)
    end

  end

end

