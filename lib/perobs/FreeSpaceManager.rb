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
        @pools[index].open
      end
    end

    # Close all pool files.
    def close
      @pools.each do |pool|
        next if pool.nil?
        pool.close
      end
      @pools = []
    end

    # Add a new space with a given address and size.
    # @param address [Integer] Starting address of the space
    # @param size [Integer] size of the space in bytes
    def add_space(address, size)
      if size <= 0
        raise RuntimeError, "Size (#{size}) must be larger than 0."
      end
      pool_index = Math.log(size, 2).to_i
      new_pool(pool_index) unless @pools[pool_index]
      @pools[pool_index].push([ address, size ].pack('QQ'))
    end

    # Get a space that has at least the requested size.
    # @param size [Integer] Required size in bytes
    # @return [Array] Touple with address and actual size of the space.
    def get_space(size)
      if size <= 0
        raise RuntimeError, "Size (#{size}) must be larger than 0."
      end
      # When we search for a free space we need to search the pool that
      # corresponds to (size - 1) * 2. It is the pool that has the spaces that
      # are at least as big as size.
      if (pool = @pools[size == 1 ? 0 : Math.log((size - 1) * 2, 2)]).nil?
        return nil
      else
        return nil unless (entry = pool.pop)
        sp_address, sp_size = entry.unpack('QQ')
        if sp_size < size
          raise RuntimeError, "Space at address #{sp_address} is too small. " +
            "Must be at least #{size} bytes but is only #{sp_size} bytes."
        end
        [ sp_address, sp_size ]
      end
    end

    # Clear all pools and forget any registered spaces.
    def clear
      close
      Dir.glob(File.join(@dir, 'free_list_*.stack')).each do |file|
        File.delete(file)
      end
    end

    def inspect
      '[' + @pools.map{ |p| p.inspect { |bs| bs.unpack('QQ').inspect} }.join(', ') + ']'
    end

    private

    def new_pool(index)
      # The file name pattern for the pool files.
      filename = "free_list_#{index}"
      @pools[index] = sf = StackFile.new(@dir, filename, 2 * 8)
      sf.open
    end

    def size_to_pool(size)
      idx = Math.log(size, 2).to_i
      # It makes no sense to have separate buckets for spaces smaller than 8
      # bytes. We enforce 8 as the smallest size.
      idx = 8 if idx < 8
      idx
    end

  end

end

