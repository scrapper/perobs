# encoding: UTF-8
#
# = BlockDB.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'json'
require 'json/add/core'
require 'json/add/struct'

module PEROBS

  # This class manages the usage of the data blocks in the corresponding
  # HashedBlocks object.
  class BlockDB

    # Create a new BlockDB object.
    def initialize(dir, block_size)
      @dir = dir
      @block_size = block_size

      @index_file_name = File.join(dir, 'index.json')
      @block_file_name = File.join(dir, 'data')
      read_index
    end

    # Write the given bytes with the given ID into the DB.
    # @param id [Fixnum or Bignum] ID
    # @param raw [String] sequence of bytes
    def write_object(id, raw)
      bytes = raw.bytesize
      start_address = reserve_blocks(id, bytes)
      if write_to_block_file(raw, start_address) != bytes
        raise RuntimeError, 'Object length does not match written bytes'
      end
      write_index
    end

    # Read the entry for the given ID and return it as bytes.
    # @param id [Fixnum or Bignum] ID
    # @return [String] sequence of bytes
    def read_object(id)
      read_from_block_file(*find(id))
    end


    # Find the data for the object with given id.
    # @param id [Fixnum or Bignum] Object ID
    # @return [Array] Returns an Array with two Fixnum entries. The first is
    #         the number of bytes and the second is the starting offset in the
    #         block storage file.
    def find(id)
      @entries.each do |entry|
        if entry['id'] == id
          return [ entry['bytes'], entry['first_block'] * @block_size ]
        end
      end

      nil
    end

    # Write a string of bytes into the file at the given address.
    # @param raw [String] bytes to write
    # @param address [Fixnum] offset in the file
    # @return [Fixnum] number of bytes written
    def write_to_block_file(raw, address)
      begin
        File.write(@block_file_name, raw, address)
      rescue => e
        raise IOError,
              "Cannot write block file #{@block_file_name}: #{e.message}"
      end
    end

    # Read _bytes_ bytes from the file starting at offset _address_.
    # @param bytes [Fixnum] number of bytes to read
    # @param address [Fixnum] offset in the file
    def read_from_block_file(bytes, address)
      begin
        File.read(@block_file_name, bytes, address)
      rescue => e
        raise IOError,
              "Cannot read block file #{@block_file_name}: #{e.message}"
      end
    end

    # Clear the mark on all entries in the index.
    def clear_marks
      @entries.each { |e| e['marked'] = false}
      write_index
    end

    # Set a mark on the entry with the given ID.
    # @param id [Fixnum or Bignum] ID of the entry
    def mark(id)
      found = false
      @entries.each do |entry|
        if entry['id'] == id
          entry['marked'] = true
          found = true
          break
        end
      end

      unless found
        raise ArgumentError, "Cannot find an entry for ID #{id} to mark"
      end

      write_index
    end

    # Check if the entry for a given ID is marked.
    # @param id [Fixnum or Bignum] ID of the entry
    # @return [TrueClass or FalseClass] true if marked, false otherwise
    def is_marked?(id)
      @entries.each do |entry|
        return entry['marked'] if entry['id'] == id
      end

      raise ArgumentError, "Cannot find an entry for ID #{id} to check"
    end

    # Remove all entries from the index that have not been marked.
    def delete_unmarked_entries
      @entries.delete_if { |e| e['marked'] == false }
      write_index
    end

    private

    # Reserve the blocks needed for the specified number of bytes with the
    # given ID.
    # @param id [Fixnum or Bignum] ID of the entry
    # @param bytes [Fixnum] number of bytes for this entry
    # @return [Fixnum] the start address of the reserved block
    def reserve_blocks(id, bytes)
      # size of the entry in blocks
      blocks = size_in_blocks(bytes)
      # index of first block after the last seen entry
      end_of_last_entry = 0
      # block index of best fit segment
      best_fit_start = nil
      # best fir segment size in blocks
      best_fit_blocks = nil
      # If there is already an entry for an object with the _id_, we mark it
      # for deletion.
      entry_to_delete = nil

      @entries.each do |entry|
        if entry['id'] == id
          # We've found an old entry for this ID.
          if entry['blocks'] >= blocks
            # The old entry still fits. Let's just reuse it.
            entry['bytes'] = bytes
            entry['blocks'] = blocks
            return entry['first_block'] * @block_size
          end
          # It does not fit. Ignore the entry and mark it for deletion.
          entry_to_delete = entry
          next
        end

        gap = entry['first_block'] - end_of_last_entry
        if gap >= blocks &&
          (best_fit_blocks.nil? || gap < best_fit_blocks)
          # We've found a segment that fits the requested bytes and fits
          # better than any previous find.
          best_fit_start = end_of_last_entry
          best_fit_blocks = gap
        end
        end_of_last_entry = entry['first_block'] + entry['blocks']
      end

      # Delete the old entry if requested.
      @entries.delete(entry_to_delete) if entry_to_delete

      # Create a new entry and insert it.
      entry = {
        'id' => id,
        'bytes' => bytes,
        'first_block' => best_fit_start || end_of_last_entry,
        'blocks' => blocks,
        'marked' => false
      }
      @entries << entry
      @entries.sort! { |e1, e2| e1['first_block'] <=> e2['first_block'] }

      entry['first_block'] * @block_size
    end

    def read_index
      if File.exists?(@index_file_name)
        begin
          @entries = JSON.parse(File.read(@index_file_name))
        rescue => e
          raise RuntimeError,
                "BlockDB file #{@index_file_name} corrupted: #{e.message}"
        end
      else
        @entries = []
      end
    end

    def write_index
      begin
        File.write(@index_file_name, @entries.to_json)
      rescue => e
        raise RuntimeError,
              "Cannot write BlockDB index file #{@index_file_name}: " +
              e.message
      end
    end

    def size_in_blocks(bytes)
      bytes / @block_size + (bytes % @block_size != 0 ? 1 : 0)
    end

  end

end

