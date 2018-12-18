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
    LENGTH = 25
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
    # @param addr [Integer] address in the file to start reading. If no
    #        address is specified use the current position in the file.
    # @param id [Integer] Optional ID that the header should have. If no id is
    #        specified there is no check against the actual ID done.
    # @return FlatFileBlobHeader or nil if there are no more blobs to read in
    #         the file.
    def FlatFileBlobHeader::read(file, addr = nil, id = nil)
      # If an address was specified we expect the read to always succeed. If
      # no address is specified and we can't read the header we generate an
      # error message but it is not fatal.
      errors_are_fatal = !addr.nil?

      mode = :searching_next_header
      addr = file.pos unless addr
      buf = nil
      corruption_start = nil

      loop do
        buf_with_crc = nil
        begin
          file.seek(addr)
          buf_with_crc = file.read(LENGTH)
        rescue IOError => e
          if errors_are_fatal
            PEROBS.log.fatal "Cannot read blob header in flat file DB at " +
              "address #{addr}: #{e.message}"
          else
            PEROBS.log.error "Cannot read blob header in flat file DB: " +
              e.message
            return nil
          end
        end

        # Did we read anything?
        if buf_with_crc.nil?
          if errors_are_fatal
            PEROBS.log.fatal "Cannot read blob header " +
              "#{id ? "for ID #{id} " : ''}at address #{addr}"
          else
            # We have reached the end of the file.
            return nil
          end
        end

        # Did we get the full header?
        if buf_with_crc.length != LENGTH
          PEROBS.log.error "Incomplete FlatFileBlobHeader: Only " +
            "#{buf_with_crc.length} " +
            "bytes of #{LENGTH} could be read "
          "#{id ? "for ID #{id} " : ''}at address #{addr}"
          return nil
        end

        # Check the CRC of the header
        buf = buf_with_crc[0..-5]
        crc = buf_with_crc[-4..-1].unpack('L')[0]

        if (read_crc = Zlib.crc32(buf, 0)) == crc
          # We have found a valid header.
          if corruption_start
            PEROBS.log.error "FlatFile corruption ends at #{addr}. " +
              "#{addr - corruption_start} bytes skipped. Some data may " +
              "not be recoverable."
          end
          break
        else
          if errors_are_fatal
            PEROBS.log.fatal "FlatFile Header CRC mismatch at address " +
              "#{addr}. Header CRC is #{'%08x' % read_crc} but should be " +
              "#{'%08x' % crc}."
          else
            if corruption_start.nil?
              PEROBS.log.error "FlatFile corruption found. The FlatFile " +
                "Header CRC mismatch at address #{addr}. Header CRC is " +
                "#{'%08x' % read_crc} but should be #{'%08x' % crc}. Trying " +
                "to find the next header."
              corruption_start = addr
            end
            # The blob file is corrupted. There is no valid header at the
            # current position in the file. We now try to find the next valid
            # header by iterating over the remainder of the file advanding one
            # byte with each step until we hit the end of the file or find the
            # next valid header.
            addr += 1
          end
        end
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
        buf = [ @flags, @length, @id, @crc].pack(FORMAT)
        crc = Zlib.crc32(buf, 0)
        @file.seek(@addr)
        @file.write(buf + [ crc ].pack('L'))
      rescue IOError => e
        PEROBS.log.fatal "Cannot write blob header into flat file DB: " +
          e.message
      end
    end

    # Reset all the flags bit to 0. This marks the blob as invalid.
    def clear_flags
      @flags = 0
      write
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
      write
    end

    # Return true if the blob contains outdated data.
    def is_outdated?
      bit_set?(OUTDATED_FLAG_BIT)
    end

    private

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

