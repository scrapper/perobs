# frozen_string_literal: true
#
# = BigTree.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2017 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/BigTreeNode'

module PEROBS

  # The BigTree class implements a BTree as a PEROBS object. It allows to
  # manage huge amounts of data in a reasonably efficient way. The number of
  # entries is limited by the space on the backing store, not the main
  # memory. Entries are addressed by a Integer key.
  class BigTree < PEROBS::Object
    Stats = Struct.new(:leaf_nodes, :branch_nodes, :min_depth, :max_depth)

    attr_persist :node_size, :root, :first_leaf, :last_leaf, :entry_counter

    # Internal constructor. Use Store.new() instead.
    # @param p [Handle]
    # @param node_size [Integer] The size of the tree nodes. This determines
    #        how many entries must be read/written for each operation.
    def initialize(p, node_size = 127)
      super(p)
      unless node_size > 2
        PEROBS.log.fatal "Node size (#{node_size}) must be larger than 2"
      end
      attr_init(:node_size, node_size)
      clear unless instance_variable_defined?('@root')
    end

    # Remove all entries from the BigTree.
    def clear
      self.root = self.first_leaf = self.last_leaf =
        @store.new(BigTreeNode, myself, true)
      self.entry_counter = 0
    end

    # Insert a new value into the tree using the key as a unique index. If the
    # key already exists the old value will be overwritten.
    # @param key [Integer] Unique key
    # @param value [Integer] value
    def insert(key, value)
      @store.transaction do
        @root.insert(key, value)
      end
    end

    # Retrieve the value associated with the given key. If no entry was found,
    # return nil.
    # @param key [Integer] Unique key
    # @return [Integer or nil] found value or nil
    def get(key)
      @root.get(key)
    end

    # Return the node chain from the root to the leaf node storing the
    # key/value pair.
    # @param key [Integer] key to search for
    # @return [Array of BigTreeNode] node list (may be empty)
    def node_chain(key)
      @root.node_chain(key)
    end

    # Check if there is an entry for the given key.
    # @param key [Integer] Unique key
    # @return [Boolean] True if key is present, false otherwise.
    def has_key?(key)
      @root.has_key?(key)
    end

    # Find and remove the value associated with the given key. If no entry was
    # found, return nil, otherwise the found value.
    # @param key [Integer] Unique key
    # @return [Integer or nil] found value or nil
    def remove(key)
      removed_value = nil

      @store.transaction do
        removed_value = @root.remove(key)
      end

      removed_value
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
        insert(k, v) unless yield(k, v)
      end
    end

    # @return [Integer] The number of entries stored in the tree.
    def length
      @entry_counter
    end

    # Return true if the BigTree has no stored entries.
    def empty?
      @entry_counter.zero?
    end

    # Iterate over all entries in the tree. Entries are always sorted by the
    # key.
    # @yield [key, value]
    def each(&)
      node = @first_leaf
      while node
        break if node.each_element(&).nil?

        node = node.next_sibling
      end
    end

    # Iterate over all entries in the tree in reverse order. Entries are
    # always sorted by the key.
    # @yield [key, value]
    def reverse_each(&)
      node = @last_leaf
      while node
        node.reverse_each_element(&)
        node = node.prev_sibling
      end
    end

    # @return [String] Human reable form of the tree.
    def to_s
      @root.to_s
    end

    # Check if the tree file contains any errors.
    # @return [Boolean] true if no erros were found, false otherwise
    def check(&)
      @root.check(&)

      i = 0
      each do |k, v|
        i += 1
      end

      unless @entry_counter == i
        PEROBS.log.error "BigTree contains #{i} values but entry counter " \
                         "is #{@entry_counter}"
        return false
      end

      true
    end

    # Gather some statistics regarding the tree structure.
    # @return [Stats] Structs with gathered data
    def statistics
      stats = Stats.new(0, 0, nil, nil)
      @root.statistics(stats)
      stats
    end
  end
end
