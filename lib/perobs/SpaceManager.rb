# encoding: UTF-8
#
# = SpaceManager.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2020 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/BTree'
require 'perobs/EquiBlobsFile'
require 'perobs/FlatFile'
require 'perobs/FlatFileBlobHeader'

module PEROBS

  # The SpaceManager is used to keep a list of all the empty spaces in a
  # FlatFileDB file. An empty space is described by its starting address and
  # its length in bytes. The SpaceManager keeps a list of all the spaces and
  # can find the best fit space when a new blob needs to be added to the
  # FlatFileDB.
  #
  # The SpaceManager uses two files to store the list. The first is a file
  # with the actual addresses. This is a set of linked address lists. Each
  # list holds the addresses for spaces that have exactly the same size. The
  # second file is a BTree file that serves as the index. It is used to map
  # the length of a space to the address of the linked list for that
  # particular length. The linked list consists of elements that only hold 2
  # items. The actual address in the FlatFileDB and the address of the next
  # entry in the linked list in the list file.
  class SpaceManager

    attr_reader :added_spaces, :recycled_spaces, :failed_requests

    def initialize(db_dir, progressmeter, btree_order = 65)
      @db_dir = db_dir
      @progressmeter = progressmeter

      @index = BTree.new(@db_dir, 'space_index', btree_order, @progressmeter)
      # The space list contains blobs that have each 2 entries. The address of
      # the space in the FlatFile and the address of the next blob in the
      # space list file that is an entry for the same space size. An address
      # of 0 marks the end of the list.
      @list = EquiBlobsFile.new(@db_dir, 'space_list', @progressmeter, 2 * 8, 1)
    end

    def open
      @index.open
      @list.open
      reset_stats
    end

    def close
      if @index.is_open?
        PEROBS.log.info "SpaceManager has currently #{@list.total_entries} " +
          "used blobs and #{@list.total_spaces} unused blobs in list " +
          "EquiBlobsFile"
        PEROBS.log.info "#{@added_spaces} were added, #{@recycled_spaces} " +
          "spaces were recycled and #{@failed_requests} requests failed"

        @list.close
        @index.close
      end
    end

    def is_open?
      @index.is_open?
    end

    def sync
      @list.sync
      @index.sync
    end

    def add_space(address, length)
      if (list_entry_addr = @index.get(length))
        # There is already at least one move entry for this length.
        new_list_entry_addr = insert_space_in_list(address, list_entry_addr)
      else
        new_list_entry_addr = insert_space_in_list(address, 0)
      end
      @index.insert(length, new_list_entry_addr)
      @added_spaces += 1
    end

    def has_space?(address, length)
      if (list_entry_addr = @index.get(length))
        while list_entry_addr > 0
          blob = @list.retrieve_blob(list_entry_addr)
          space_address, next_entry_addr = blob.unpack('QQ')
          return true if space_address == address
          list_entry_addr = next_entry_addr
        end
      end

      false
    end

    def get_space(length)
      # We use a simple exact fit strategy. All attempts to use a more
      # elaborate scheme were actually less efficient. Non-exact matches
      # generate new spaces for the remainder and fragment the blob file with
      # lots of unusable small spaces. Most applications seem to have
      # clustered their blob sizes around a number of popular sizes. So exact
      # match is very efficient to implement and results in the highest
      # probability that a space will be reused soon.
      list_entry_addr = @index.get(length)

      if list_entry_addr
        blob = @list.retrieve_blob(list_entry_addr)
        space_address, next_entry_addr = blob.unpack('QQ')
        @list.delete_blob(list_entry_addr)

        if next_entry_addr > 0
          # Update the index entry for the length to point to the
          # following space list entry.
          @index.insert(length, next_entry_addr)
        else
          # The space list for this length is empty. Remove the entry
          # from the index.
          @index.remove(length)
        end
        @recycled_spaces += 1

        # We return the length to remain compatible with the old SpaceTree
        # API.
        return [ space_address, length ]
      end

      @failed_requests += 1
      nil
    end

    def clear
      @list.clear
      @index.clear
      reset_stats
    end

    def erase
      @list.erase
      @index.erase
    end

    def check(flat_file = nil)
      sync
      return false unless @index.check
      return false unless @list.check

      smallest_space = nil
      largest_space = nil
      total_space_bytes = 0
      space_distribution = ::Hash.new(0)

      @index.each do |length, list_entry_addr|
        if list_entry_addr <= 0
          PEROBS.log.error "list_entry_addr (#{list_entry_addr}) " +
            "must be positive"
          return false
        end

        # Detect smallest and largest space
        if smallest_space.nil? || length < smallest_space
          smallest_space = length
        end
        if largest_space.nil? || length > largest_space
          largest_space = length
        end

        known_addresses = [ list_entry_addr ]
        entries = 0
        while list_entry_addr > 0
          entries += 1
          unless (blob = @list.retrieve_blob(list_entry_addr))
            PEROBS.log.error "SpaceManager points to non-existing " +
              "space list entry at address #{list_entry_addr}"
            return false
          end
          space_address, next_entry_addr = blob.unpack('QQ')

          if known_addresses.include?(next_entry_addr)
            PEROBS.log.error "Space list is cyclic: "
              "#{known_addresses + next_entry_addr}"
            return false
          end
          if flat_file &&
              !flat_file.has_space?(space_address, length)
            PEROBS.log.error "SpaceManager has space at offset " +
              "#{space_address} of size #{length} that isn't " +
              "available in the FlatFile."
            return false
          end
          list_entry_addr = next_entry_addr
        end

        total_space_bytes += length * entries
        space_distribution[msb(length)] += entries
      end

      PEROBS.log.info "SpaceManager stats: smallest: #{smallest_space}; " +
        "largest: #{largest_space}; total bytes: #{total_space_bytes}; " +
        "distribution: " +
        "#{space_distribution.map { |l, c| "#{2 ** (l - 1)}-#{2 ** l - 1}:#{c}; " }}"

      true
    end

    def to_a
      a = []

      @index.each do |length, list_entry_addr|
        while list_entry_addr > 0
          blob = @list.retrieve_blob(list_entry_addr)
          space_address, next_entry_addr = blob.unpack('QQ')

          a << [ space_address, length ]

          list_entry_addr = next_entry_addr
        end
      end

      a.sort { |a, b| a[0] <=> b[0] }
    end

    private

    def insert_space_in_list(next_element_addr, space_address)
      blob = [ next_element_addr, space_address ].pack('QQ')
      @list.store_blob(blob_addr = @list.free_address, blob)

      blob_addr
    end

    def msb(i)
      return 63 if i < 0

      bit = 0
      while (i > 0)
        bit += 1
        i = i >> 1
      end

      bit
    end

    def reset_stats
      @added_spaces = 0
      @recycled_spaces = 0
      @failed_requests = 0
    end

  end

end

