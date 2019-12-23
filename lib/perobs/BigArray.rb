# encoding: UTF-8
#
# = BigArray.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2017, 2018, 2019
# by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/Object'
require 'perobs/BigArrayNode'

module PEROBS

  # The BigArray class implements an Array that stores the data in segments. It
  # only loads the currently needed parts of the Array into memory. To provide
  # an efficient access to the data by index a B+Tree like data structure is
  # used. Each segment is stored in a leaf node of the B+Tree.
  class BigArray < PEROBS::Object

    class Stats < Struct.new(:leaf_nodes, :branch_nodes, :min_depth,
                             :max_depth)
    end

    attr_persist :node_size, :root, :first_leaf, :last_leaf, :entry_counter

    # Internal constructor. Use Store.new() instead.
    # @param p [Handle]
    # @param node_size [Integer] The size of the tree nodes. This determines
    #        how many entries must be read/written for each operation. The
    #        default of 150 was emperically found to be a performance sweet
    #        spot. Smaller values will improve write operations. Larger
    #        values will improve read operations. 20 - 500 is a reasonable
    #        range to try.
    def initialize(p, node_size = 150)
      super(p)
      unless node_size > 3
        PEROBS.log.fatal "Node size (#{node_size}) must be larger than 3"
      end
      unless node_size % 2 == 0
        PEROBS.log.fatal "Node size (#{node_size}) must be an even number"
      end

      self.node_size = node_size
      clear
    end

    # Remove all entries from the BigArray.
    def clear
      self.root = self.first_leaf = self.last_leaf =
        @store.new(BigArrayNode, myself, true)
      self.entry_counter = 0
    end

    # Store the value at the given index. If the index already exists the old
    # value will be overwritten.
    # @param index [Integer] Position in the array
    # @param value [Integer] value
    def []=(index, value)
      index = validate_index_range(index)

      @store.transaction do
        if index < @entry_counter
          # Overwrite of an existing element
          @root.set(index, value)
        elsif index == @entry_counter
          # Append right at the end
          @root.insert(index, value)
          self.entry_counter += 1
        else
          # Append with nil padding
          @entry_counter.upto(index - 1) do |i|
            @root.insert(i, nil)
          end
          @root.insert(index, value)
          self.entry_counter = index + 1
        end
      end
    end

    def <<(value)
      self[@entry_counter] = value
    end

    # Insert the value at the given index. If the index already exists the old
    # value will be overwritten.
    # @param index [Integer] Position in the array
    # @param value [Integer] value
    def insert(index, value)
      index = validate_index_range(index)

      if index < @entry_counter
        # Insert in between existing elements
        @store.transaction do
          @root.insert(index, value)
          self.entry_counter += 1
        end
      else
        self[index] = value
      end
    end

    # Return the value stored at the given index.
    # @param index [Integer] Position in the array
    # @return [Integer or nil] found value or nil
    def [](index)
      index = validate_index_range(index)

      return nil if index >= @entry_counter

      @root.get(index)
    end

    # Check if there is an entry for the given key.
    # @param key [Integer] Unique key
    # @return [Boolean] True if key is present, false otherwise.
    def has_key?(key)
      @root.has_key?(key)
    end

    # Delete the element at the specified index, returning that element, or
    # nil if the index is out of range.
    # @param index [Integer] Index in the BigArray
    # @return [Object] found value or nil
    def delete_at(index)
      if index < 0
        index = @entry_counter + index
      end

      return nil if index < 0 || index >= @entry_counter

      deleted_value = nil
      @store.transaction do
        deleted_value = @root.delete_at(index)
        self.entry_counter -= 1

        # Eliminate single entry nodes at the top.
        while !@root.is_leaf? && @root.size == 1
          @root = @root.children.first
          @root.parent = nil
        end
      end

      deleted_value
    end

    # Delete all entries for which the passed block yields true. The
    # implementation is optimized for large bulk deletes. It rebuilds a new
    # BTree for the elements to keep. If only few elements are deleted the
    # overhead of rebuilding the BTree is rather high.
    # @yield [key, value]
    def delete_if
      old_root = @root
      clear
      old_root.each do |k, v|
        if !yield(k, v)
          insert(k, v)
        end
      end
    end

    # @return [Integer] The number of entries stored in the tree.
    def length
      @entry_counter
    end

    alias size length

    # Return true if the BigArray has no stored entries.
    def empty?
      @entry_counter == 0
    end

    # Return the first entry of the Array.
    def first
      return nil unless @first_leaf

      @first_leaf.values.first
    end

    # Return the last entry of the Array.
    def last
      return nil unless @last_leaf

      @last_leaf.values.last
    end

    # Iterate over all entries in the tree. Entries are always sorted by the
    # key.
    # @yield [key, value]
    def each(&block)
      node = @first_leaf
      while node
        break unless node.each(&block)
        node = node.next_sibling
      end
    end

    # Iterate over all entries in the tree in reverse order. Entries are
    # always sorted by the key.
    # @yield [key, value]
    def reverse_each(&block)
      node = @last_leaf
      while node
        break unless node.reverse_each(&block)
        node = node.prev_sibling
      end
    end

    # Convert the BigArray into a Ruby Array. This is primarily intended for
    # debugging as real-world BigArray objects are likely too big to fit into
    # memory.
    def to_a
      ary = []
      node = @first_leaf
      while node do
        ary += node.values
        node = node.next_sibling
      end

      ary
    end

    # @return [String] Human reable form of the tree. This is only intended
    # for debugging and should only be used with small BigArray objects.
    def to_s
      @root.to_s
    end

    # Check if the tree file contains any errors.
    # @return [Boolean] true if no erros were found, false otherwise
    def check(&block)
      @root.check(&block)
    end

    # Gather some statistics regarding the tree structure.
    # @return [Stats] Structs with gathered data
    def statistics
      stats = Stats.new(0, 0, nil, nil)
      @root.statistics(stats)
      stats
    end

    private

    def validate_index_range(index)
      if index < 0
        if -index > @entry_counter
          raise IndexError, "index #{index} too small for array; " +
            "minimum #{-@entry_counter}"
        end

        index = @entry_counter + index
      end

      index
    end

  end

end

