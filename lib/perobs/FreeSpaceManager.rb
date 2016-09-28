# encoding: UTF-8
#
# = FreeSpaceManager.rb -- Persistent Ruby Object Store
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

  # The FreeSpaceManager keeps a list of the free spaces in the FlatFile. Each
  # space is stored with address and size. The data is persisted in the file
  # system. Internally the free spaces are stored in different pools. Each
  # pool holds spaces that are at least of a given size and not as big as the
  # next pool up. Pool entry minimum sizes increase by a factor of 2 from
  # pool to pool.
  class FreeSpaceManager

    # Create a new FreeSpaceManager object in the specified directory.
    # @param dir [String] directory path
    def initialize(dir)
      @dir = dir
      @pools = []
    end

    # Open the pool files.
    def open
      Dir.glob(File.join(@dir, 'free_list_*.stack')).each do |file|
        basename = File.basename(file)
        # Cut out the pool index from the file name.
        index = basename[10..-7].to_i
        @pools[index] = StackFile.new(@dir, basename[0..-7], 2 * 8)
      end
    end

    # Close all pool files.
    def close
      @pools = []
    end

    # Add a new space with a given address and size.
    # @param address [Integer] Starting address of the space
    # @param size [Integer] size of the space in bytes
    def add_space(address, size)
      if size <= 0
        PEROBS.log.fatal "Size (#{size}) must be larger than 0."
      end
      pool_index = msb(size)
      new_pool(pool_index) unless @pools[pool_index]
      push_pool(pool_index, [ address, size ].pack('QQ'))
    end

    # Get a space that has at least the requested size.
    # @param size [Integer] Required size in bytes
    # @return [Array] Touple with address and actual size of the space.
    def get_space(size)
      if size <= 0
        PEROBS.log.fatal "Size (#{size}) must be larger than 0."
      end
      # When we search for a free space we need to search the pool that
      # corresponds to (size - 1) * 2. It is the pool that has the spaces that
      # are at least as big as size.
      pool_index = size == 1 ? 0 : msb(size - 1) + 1
      unless @pools[pool_index]
        return nil
      else
        return nil unless (entry = pop_pool(pool_index))
        sp_address, sp_size = entry.unpack('QQ')
        if sp_size < size
          PEROBS.log.fatal "Space at address #{sp_address} is too small. " +
            "Must be at least #{size} bytes but is only #{sp_size} bytes."
        end
        [ sp_address, sp_size ]
      end
    end

    # Clear all pools and forget any registered spaces.
    def clear
      @pools.each do |pool|
        if pool
          pool.open
          pool.clear
          pool.close
        end
      end
      close
    end

    # Check if there is a space in the free space lists that matches the
    # address and the size.
    # @param [Integer] address Address of the space
    # @param [Integer] size Length of the space in bytes
    # @return [Boolean] True if space is found, false otherwise
    def has_space?(address, size)
      unless (pool = @pools[msb(size)])
        return false
      end

      pool.open
      pool.each do |entry|
        sp_address, sp_size = entry.unpack('QQ')
        if address == sp_address
          if size != sp_size
            PEROBS.log.fatal "FreeSpaceManager has space with different " +
              "size"
          end
          pool.close
          return true
        end
      end

      pool.close
      false
    end

    def check(flat_file)
      @pools.each do |pool|
        next unless pool

        pool.open
        pool.each do |entry|
          address, size = entry.unpack('QQ')
          unless flat_file.has_space?(address, size)
            PEROBS.log.error "FreeSpaceManager has space that isn't " +
              "available in the FlatFile."
            return false
          end
        end
        pool.close
      end

      true
    end

    def inspect
      '[' + @pools.map do |p|
        if p
          p.open
          r = p.to_ary.map { |bs| bs.unpack('QQ')}.inspect
          p.close
          r
        else
          'nil'
        end
      end.join(', ') + ']'
    end

    private

    def new_pool(index)
      # The file name pattern for the pool files.
      filename = "free_list_#{index}"
      @pools[index] = sf = StackFile.new(@dir, filename, 2 * 8)
    end

    def push_pool(index, value)
      pool = @pools[index]
      pool.open
      pool.push(value)
      pool.close
    end

    def pop_pool(index)
      pool = @pools[index]
      pool.open
      value = pool.pop
      pool.close

      value
    end

    def msb(i)
      unless i > 0
        PEROBS.log.fatal "i must be larger than 0"
      end
      i.to_s(2).length - 1
    end

  end

end

