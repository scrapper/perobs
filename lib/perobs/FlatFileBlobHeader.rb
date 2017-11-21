# encoding: UTF-8
#
# = FlatFileBlobHeader.rb -- Persistent Ruby Object Store
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

module PEROBS

  # The FlatFile blob header has the following structure:
  #
  # 1 Byte:  Flags byte.
  #          Bit 0: 0 deleted entry, 1 valid entry
  #          Bit 1: 0 reserved, must be 0
  #          Bit 2: 0 uncompressed data, 1 compressed data
  #          Bit 3: 0 current entry, 1 outdated entry
  #          Bit 4 - 7: reserved, must be 0
  # 8 bytes: Length of the data blob in bytes
  # 8 bytes: ID of the value in the data blob
  # 4 bytes: CRC32 checksum of the data blob
  #
  # If the bit 0 of the flags byte is 0, only the length is valid. The blob is
  # empty. Only of bit 0 is set then entry is valid.
  class FlatFileBlobHeader

    # The 'pack()' format of the header.
    FORMAT = 'CQQL'
    # The length of the header in bytes.
    LENGTH = 21
    VALID_FLAG_BIT = 0
    COMPRESSED_FLAG_BIT = 2
    OUTDATED_FLAG_BIT = 3

    attr_reader :addr, :flags, :length, :id, :crc

    # Create a new FlatFileBlobHeader with the given flags, length, id and crc.
    # @param file [File] the FlatFile that contains the header
    # @param addr [Integer] the offset address of the header in the file
    # @param flags [Integer] 8 bit number, see above
    # @param length [Integer] length of the header in bytes
    # @param id [Integer] ID of the blob entry
    # @param crc [Integer] CRC32 checksum of the blob entry
    def initialize(file, addr, flags, length, id, crc)
      @file = file
      @addr = addr
      @flags = flags
      @length = length
      @id = id
      @crc = crc
    end

    # Read the header from the given File.
    # @param file [File]
    # @return FlatFileBlobHeader
    def FlatFileBlobHeader::read(file)
      begin
        addr = file.pos
        buf = file.read(LENGTH)
      rescue IOError => e
        PEROBS.log.error "Cannot read blob header in flat file DB: #{e.message}"
        return nil
      end

      return nil unless buf

      if buf.length != LENGTH
        PEROBS.log.error "Incomplete FlatFileBlobHeader: Only #{buf.length} " +
          "bytes of #{LENGTH} could be read"
        return nil
      end

      FlatFileBlobHeader.new(file, addr, *buf.unpack(FORMAT))
    end

    # Read the header from the given File.
    # @param file [File]
    # @param addr [Integer] address in the file to start reading
    # @param id [Integer] Optional ID that the header should have
    # @return FlatFileBlobHeader
    def FlatFileBlobHeader::read_at(file, addr, id = nil)
      buf = nil
      begin
        file.seek(addr)
        buf = file.read(LENGTH)
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob in flat file DB: #{e.message}"
      end
      if buf.nil? || buf.length != LENGTH
        PEROBS.log.fatal "Cannot read blob header " +
          "#{id ? "for ID #{id} " : ''}at address " +
          "#{addr}"
      end
      header = FlatFileBlobHeader.new(file, addr, *buf.unpack(FORMAT))
      if id && header.id != id
        PEROBS.log.fatal "Mismatch between FlatFile index and blob file " +
          "found. FlatFile has entry with ID #{header.id} at address " +
          "#{addr}. Index has ID #{id} for this address."
      end

      return header
    end

    # Write the header to a given File.
    def write
      begin
        @file.seek(@addr)
        @file.write([ @flags, @length, @id, @crc].pack(FORMAT))
      rescue IOError => e
        PEROBS.log.fatal "Cannot write blob header into flat file DB: " +
          e.message
      end
    end

    # Reset all the flags bit to 0. This marks the blob as invalid.
    def clear_flags
      @flags = 0
      write_flags
    end

    # Return true if the header is for a non-empty blob.
    def is_valid?
      bit_set?(VALID_FLAG_BIT)
    end

    # Return true if the blob contains compressed data.
    def is_compressed?
      bit_set?(COMPRESSED_FLAG_BIT)
    end

    # Set the outdated bit. The entry will be invalid as soon as the current
    # transaction has been completed.
    def set_outdated_flag
      set_flag(OUTDATED_FLAG_BIT)
      write_flags
    end

    # Return true if the blob contains outdated data.
    def is_outdated?
      bit_set?(OUTDATED_FLAG_BIT)
    end

    private

    def write_flags
      begin
        @file.seek(@addr)
        @file.write([ @flags ].pack('C'))
        @file.flush
      rescue IOError => e
        PEROBS.log.fatal "Writing flags of FlatFileBlobHeader with ID #{@id} " +
          "failed: #{e.message}"
      end
    end

    def bit_set?(n)
      mask = 1 << n
      @flags & mask == mask
    end

    def set_flag(n)
      @flags |= (1 << n)
    end

    def clear_flag(n)
      @flags &= ~(1 << n) & 0xFF
    end

  end

end

