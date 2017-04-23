# encoding: UTF-8
#
# = BTreeNode.rb -- Persistent Ruby Object Store
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

require 'perobs/EquiBlobsFile'
require 'perobs/BTreeNodeCache'
require 'perobs/BTreeNode'

module PEROBS

  class BTree

    attr_reader :order, :nodes

    def initialize(dir, order)
      @dir = dir
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
      @nodes = EquiBlobsFile.new(@dir, 'index',
                                 BTreeNode::node_bytes(@order))
      @node_cache = BTreeNodeCache.new(8)
    end

    def open
      @node_cache.flush
      @nodes.open
      set_root(new_node(nil, @nodes.total_entries == 0 ?
                             nil : @nodes.first_entry))
    end

    def close
      @node_cache.flush
      @nodes.close
      @root = nil
    end

    def sync
      @node_cache.flush
      @nodes.sync
    end

    def check(foo = nil)
      @root.check
    end

    def set_root(node)
      @root = node
      @nodes.first_entry = node.node_address
    end

    # Insert a new value into the tree using the key as a unique index. If the
    # key already exists the old value will be overwritten.
    # @param key [Integer] Unique key
    # @param value [Integer] value
    def insert(key, value)
      @root.insert(key, value)
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
      removed_value = @root.remove(key)

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

    def mark_node_as_modified(node)
      @node_cache.mark_as_modified(node)
    end

    # Delete the node at the given address in the BTree file.
    # @param address [Integer] address in file
    def delete_node(address)
      @node_cache.delete(address)
      @nodes.delete_blob(address)
    end

    # Clear all pools and forget any registered spaces.
    def clear
      @node_cache.flush
      @nodes.clear
      set_root(new_node(nil))
    end

    def to_s
      @root.to_s
    end

    # Create a new BTreeNode. If the node_address is not nil, the node data is
    # read from the backing store. The parent and is_leaf arguments are
    # ignored in this case.
    # @param parent [BTreeNode] parent node
    # @param node_address [Integer or nil] address of the node to create
    # @param is_leaf[Boolean] True if node is a leaf node, false otherweise
    def new_node(parent, node_address = nil, is_leaf = true)
      node = BTreeNode.new(self, parent, node_address, is_leaf)
      @node_cache.insert(node)

      node
    end

    # Return the BTreeNode that matches the given node address. If a blob
    # address and size are given, a new node is created instead of being read
    # from the file.
    # @param node_address [Integer] Address of the node in the BTree file
    # @return [BTreeNode]
    def get_node(node_address)
      if (node = @node_cache[node_address])
        return node
      end

      new_node(nil, node_address)
    end

  end

end

