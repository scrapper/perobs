# encoding: UTF-8
#
# = BTreeNode.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2017, 2018 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/LockFile'
require 'perobs/EquiBlobsFile'
require 'perobs/PersistentObjectCache'
require 'perobs/BTreeNode'

module PEROBS

  # This BTree class is very similar to a classic B+Tree implementation. It
  # manages a tree that is always balanced. The BTree is stored in the
  # specified directory and partially kept in memory to speed up operations.
  # The order of the tree specifies how many keys each node will be able to
  # hold. Leaf nodes will have a value associated with each key. Branch nodes
  # have N + 1 references to child nodes instead.
  class BTree

    attr_reader :order, :nodes, :node_cache, :first_leaf, :last_leaf, :size

    # Create a new BTree object.
    # @param dir [String] Directory to store the tree file
    # @param name [String] Base name of the BTree related files in 'dir'
    # @param order [Integer] The maximum number of keys per node. This number
    #        must be odd and larger than 2 and smaller than 2**16 - 1.
    # @param progressmeter [ProgressMeter] reference to a ProgressMeter object
    def initialize(dir, name, order, progressmeter)
      @dir = dir
      @name = name
      @progressmeter = progressmeter

      unless order > 2
        PEROBS.log.fatal "BTree order must be larger than 2, not #{order}"
      end
      unless order % 2 == 1
        PEROBS.log.fatal "BTree order must be an uneven number, not #{order}"
      end
      unless order < 2 ** 16 - 1
        PEROBS.log.fatal "BTree order must be smaller than #{2**16 - 1}"
      end
      @order = order

      # This EquiBlobsFile contains the nodes of the BTree.
      @nodes = EquiBlobsFile.new(@dir, @name, @progressmeter,
                                 BTreeNode::node_bytes(@order))
      @nodes.register_custom_data('first_leaf')
      @nodes.register_custom_data('last_leaf')
      @nodes.register_custom_data('btree_size')
      @node_cache = PersistentObjectCache.new(16384, 5000, BTreeNode, self)
      @root = @first_leaf = @last_leaf = nil
      @size = 0

      # This BTree implementation uses a write cache to improve write
      # performance of multiple successive read/write operations. This also
      # means that data may not be written on the backing store until the
      # sync() or close() methods have been called. A bug in the program or a
      # premature program termination can lead to data loss. To detect such
      # situations, we use a lock file whenever there are pending writes.
      @is_dirty = false
      @dirty_flag = LockFile.new(File.join(@dir, name + '.dirty'),
                                 { :timeout_secs => 0 })
    end

    # Open the tree file.
    def open(file_must_exist = false)
      if @dirty_flag.is_locked?
        PEROBS.log.fatal "Index file #{@nodes.file_name} is already " +
          "locked"
      end
      if file_must_exist && !@nodes.file_exist?
        PEROBS.log.fatal "Index file #{@nodes.file_name} does not exist"
      end

      @node_cache.clear
      @nodes.open

      if @nodes.total_entries == 0
        # We've created a new nodes file
        node = BTreeNode::create(self)
      else
        # We are loading an existing tree.
        node = BTreeNode::load_and_link(self, @nodes.first_entry)
        @first_leaf = BTreeNode::load_and_link(
          self, @nodes.get_custom_data('first_leaf'))
        @last_leaf = BTreeNode::load_and_link(
          self, @nodes.get_custom_data('last_leaf'))
      end
      set_root(node)

      # Get the total number of entries that are stored in the tree.
      @size = @nodes.get_custom_data('btree_size')
    end

    # Close the tree file.
    def close

      def val_perc(value, total)
        "#{value} (#{(value.to_f / total*100.0).to_i}%)"
      end

      sync
      @nodes.close
      @root = nil
    end

    # Clear all pools and forget any registered spaces.
    def clear
      @node_cache.clear
      @nodes.clear
      @size = 0
      set_root(BTreeNode::create(self))
    end

    # Erase the backing store of the BTree. This method should only be called
    # when not having the BTree open. And it obviously and permanently erases
    # all stored data from the BTree.
    def erase
      @nodes.erase
      @size = 0
      @root = nil
      @dirty_flag.forced_unlock
    end

    # Flush all pending modifications into the tree file.
    def sync
      @node_cache.flush(true)
      @nodes.set_custom_data('btree_size', @size)
      @nodes.sync
      @dirty_flag.unlock if @dirty_flag.is_locked?
    end

    # Check if the tree file contains any errors.
    # @return [Boolean] true if no erros were found, false otherwise
    def check(&block)
      sync
      return false unless @nodes.check

      entries = 0
      res = true
      @progressmeter.start('Checking index structure', @size) do |pm|
        res = @root.check do |k, v|
          pm.update(entries += 1)
          block_given? ? yield(k, v) : true
        end
      end

      unless entries == @size
        PEROBS.log.error "The BTree size (#{@size}) and the number of " +
          "found entries (#{entries}) don't match"
        return false
      end

      res
    end

    # Register a new node as root node of the tree.
    # @param node [BTreeNode]
    def set_root(node)
      @root = node
      @nodes.first_entry = node.node_address
    end

    # Set the address of the first leaf node.
    # @param node [BTreeNode]
    def set_first_leaf(node)
      @first_leaf = node
      @nodes.set_custom_data('first_leaf', node.node_address)
    end

    # Set the address of the last leaf node.
    # @param node [BTreeNode]
    def set_last_leaf(node)
      @last_leaf = node
      @nodes.set_custom_data('last_leaf', node.node_address)
    end

    # Insert a new value into the tree using the key as a unique index. If the
    # key already exists the old value will be overwritten.
    # @param key [Integer] Unique key
    # @param value [Integer] value
    def insert(key, value)
      @size += 1 if @root.insert(key, value)
      @node_cache.flush
    end

    # Retrieve the value associated with the given key. If no entry was found,
    # return nil.
    # @param key [Integer] Unique key
    # @return [Integer or nil] found value or nil
    def get(key)
      @root.get(key)
    end

    # Find and remove the value associated with the given key. If no entry was
    # found, return nil, otherwise the found value.
    def remove(key)
      @size -= 1 unless (removed_value = @root.remove(key)).nil?

      # Check if the root node only contains one child link after the delete
      # operation. Then we can delete that node and pull the tree one level
      # up. This could happen for a sequence of nodes that all got merged to
      # single child nodes.
      while !@root.is_leaf && @root.children.size == 1
        old_root = @root
        set_root(@root.children.first)
        @root.parent = nil
        delete_node(old_root.node_address)
      end

      @node_cache.flush
      removed_value
    end

    # Iterate over all key/value pairs that are stored in the tree.
    # @yield [key, value]
    def each(&block)
      @root.each(&block)
    end

    # Delete the node at the given address in the BTree file.
    # @param address [Integer] address in file
    def delete_node(address)
      @node_cache.delete(address)
      @nodes.delete_blob(address)
    end

    # @return [Integer] The number of entries stored in the tree.
    def entries_count
      @size
    end

    # @return [String] Human reable form of the tree.
    def to_s
      @root.to_s
    end

  end

end

