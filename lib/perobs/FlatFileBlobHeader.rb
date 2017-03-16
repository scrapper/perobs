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
  # 1 Byte:  Mark byte.
  #          Bit 0: 0 deleted entry, 1 valid entry
  #          Bit 1: 0 unmarked, 1 marked
  #          Bit 2: 0 uncompressed data, 1 compressed data
  #          Bit 3 - 7: reserved, must be 0
  # 8 bytes: Length of the data blob in bytes
  # 8 bytes: ID of the value in the data blob
  # 4 bytes: CRC32 checksum of the data blob
  #
  # If the bit 0 of the mark byte is 0, only the length is valid. The blob is
  # empty. Only of bit 0 is set then entry is valid.
  class FlatFileBlobHeader

    # The 'pack()' format of the header.
    FORMAT = 'CQQL'
    # The length of the header in bytes.
    LENGTH = 21

    attr_reader :mark, :length, :id, :crc

    # Create a new FlatFileBlobHeader with the given mark, length, id and crc.
    # @param mark [Fixnum] 8 bit number, see above
    # @param length [Fixnum] length of the header in bytes
    # @param id [Integer] ID of the blob entry
    # @param crc [Fixnum] CRC32 checksum of the blob entry
    def initialize(mark, length, id, crc)
      @mark = mark
      @length = length
      @id = id
      @crc = crc
    end

    # Read the header from the given File.
    # @param file [File]
    # @return FlatFileBlobHeader
    def FlatFileBlobHeader::read(file)
      begin
        buf = file.read(LENGTH)
      rescue IOError => e
        PEROBS.log.fatal "Cannot read blob header in flat file DB: #{e.message}"
      end

      return nil unless buf

      FlatFileBlobHeader.new(*buf.unpack(FORMAT))
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
      header = FlatFileBlobHeader.new(*buf.unpack(FORMAT))
      if id && header.id != id
        PEROBS.log.fatal "Mismatch between FlatFile index and blob file " +
          "found for entry with ID #{id}/#{header.id}"
      end

      return header
    end

    # Write the header to a given File.
    # @param file [File]
    def write(file)
      begin
        file.write([ @mark, @length, @id, @crc].pack(FORMAT))
      rescue IOError => e
        PEROBS.log.fatal "Cannot write blob header into flat file DB: " +
          e.message
      end
    end

    # Return true if the header is for a non-empty blob.
    def is_valid?
      bit_set?(0)
    end

    # Return true if the blob has been marked.
    def is_marked?
      bit_set?(1)
    end

    # Return true if the blob contains compressed data.
    def is_compressed?
      bit_set?(2)
    end

    private

    def bit_set?(n)
      mask = 1 << n
      @mark & mask == mask
    end

  end

end

