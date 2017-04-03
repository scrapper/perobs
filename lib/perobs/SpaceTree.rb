# encoding: UTF-8
#
# = SpaceTree.rb -- Persistent Ruby Object Store
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
require 'perobs/FixedSizeBlobFile'
require 'perobs/SpaceTreeNode'
require 'perobs/FlatFile'

module PEROBS

  # The SpaceTree keeps a complete list of all empty spaces in the FlatFile.
  # Spaces are stored with size and address. The Tree is Tenerary Tree. The
  # nodes can link to other nodes with smaller spaces, same spaces and bigger
  # spaces.
  class SpaceTree

    attr_reader :nodes

    # Manage the free spaces tree in the specified directory
    # @param dir [String] directory path of an existing directory
    def initialize(dir)
      @dir = dir

      # This FixedSizeBlobFile contains the nodes of the SpaceTree.
      @nodes = FixedSizeBlobFile.new(@dir, 'database_spaces',
                                     SpaceTreeNode::NODE_BYTES)

      @node_cache = {}
    end

    # Open the SpaceTree file.
    def open
      @nodes.open
      @node_cache = {}
      @root = @node_cache[0] =
        SpaceTreeNode.new(self, nil, @nodes.empty? ? nil : 0)
    end

    # Close the SpaceTree file.
    def close
      @nodes.close
      @root = nil
      @node_cache = {}
    end

    # Add a new space with a given address and size.
    # @param address [Integer] Starting address of the space
    # @param size [Integer] size of the space in bytes
    def add_space(address, size)
      if size <= 0
        PEROBS.log.fatal "Size (#{size}) must be larger than 0."
      end
      @root.add_space(address, size)
    end

    # Get a space that has at least the requested size.
    # @param size [Integer] Required size in bytes
    # @return [Array] Touple with address and actual size of the space.
    def get_space(size)
      if size <= 0
        PEROBS.log.fatal "Size (#{size}) must be larger than 0."
      end

      if (address_size = @root.find_matching_space(size))
        # First we try to find an exact match.
        return address_size
      elsif (address_size = @root.find_equal_or_larger_space(size))
        return address_size
      else
        return nil
      end
    end

    # Delete the node at the given address in the SpaceTree file.
    # @param address [Integer] address in file
    def delete_node(address)
      @node_cache.delete(address)
      @nodes.delete_blob(address)
    end

    # Clear all pools and forget any registered spaces.
    def clear
      @nodes.clear
      @node_cache = {}
      @root = @node_cache[0] = SpaceTreeNode.new(self)
    end

    def new_node(parent, blob_address, size)
      node = SpaceTreeNode.new(self, parent, nil, blob_address, size)
      @node_cache[node.node_address] = node
    end

    # Return the SpaceTreeNode that matches the given node address. If a blob
    # address and size are given, a new node is created instead of being read
    # from the file.
    # @param node_address [Integer] Address of the node in the SpaceTree file
    # @return [SpaceTreeNode]
    def get_node(node_address)
      if @node_cache.include?(node_address)
        return @node_cache[node_address]
      end

      @node_cache[node_address] =
        SpaceTreeNode.new(self, nil, node_address)
    end

    # Check if there is a space in the free space lists that matches the
    # address and the size.
    # @param [Integer] address Address of the space
    # @param [Integer] size Length of the space in bytes
    # @return [Boolean] True if space is found, false otherwise
    def has_space?(address, size)
      @root.has_space?(address, size)
    end

    # Check if the index is OK and matches the flat_file data (if given).
    # @param flat_file [FlatFile] Flat file to compare with
    # @return True if space list matches, flase otherwise
    def check(flat_file = nil)
      @root.check(flat_file)
    end

    # @return Complete internal tree data structure as textual tree.
    def text_tree
      @root.text_tree
    end

    # Convert the tree into a human readable form.
    # @return [String]
    def inspect
      @root.gather_addresses_and_sizes.inspect
    end

  end

end

