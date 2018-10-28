# encoding: UTF-8
#
# = SpaceTree.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'
require 'perobs/EquiBlobsFile'
require 'perobs/PersistentObjectCache'
require 'perobs/SpaceTreeNode'
require 'perobs/FlatFile'

module PEROBS

  # The SpaceTree keeps a complete list of all empty spaces in the FlatFile.
  # Spaces are stored with size and address. The Tree is Tenary Tree. The
  # nodes can link to other nodes with smaller spaces, same spaces and bigger
  # spaces. The advantage of the ternary tree is that all nodes have equal
  # size which drastically simplifies the backing store operation.
  class SpaceTree

    attr_reader :nodes, :cache

    # Manage the free spaces tree in the specified directory
    # @param dir [String] directory path of an existing directory
    def initialize(dir, progressmeter)
      @dir = dir
      @progressmeter = progressmeter

      # This EquiBlobsFile contains the nodes of the SpaceTree.
      @nodes = EquiBlobsFile.new(@dir, 'database_spaces', progressmeter,
                                 SpaceTreeNode::NODE_BYTES, 1)

      # Benchmark runs showed a cache size of 128 to be a good compromise
      # between read and write performance trade-offs and memory consumption.
      @cache = PersistentObjectCache.new(128, SpaceTreeNode, self)
    end

    # Open the SpaceTree file.
    def open
      @nodes.open
      @cache.clear
      node = @nodes.total_entries == 0 ?
        SpaceTreeNode::create(self) :
        SpaceTreeNode::load(self, @nodes.first_entry)
      @root_address = node.node_address
    end

    # Close the SpaceTree file.
    def close
      @cache.flush(true)
      @nodes.close
      @root_address = nil
      @cache.clear
    end

    # Flush all pending writes to the file system.
    def sync
      @cache.flush
      @nodes.sync
    end

    # Set a new root node for the SpaceTree
    # @param node [SpaceTreeNode]
    def set_root(node)
      @root_address = node.node_address
      @nodes.first_entry = node.node_address
    end

    # Return the SpaceTreeNode that is the root of the SpaceTree.
    def root
      @root_address ? @cache.get(@root_address) : nil
    end

    # Erase the SpaceTree file. This method cannot be called while the file is
    # open.
    def erase
      @nodes.erase
    end

    # Add a new space with a given address and size.
    # @param address [Integer] Starting address of the space
    # @param size [Integer] size of the space in bytes
    def add_space(address, size)
      if size <= 0
        PEROBS.log.fatal "Size (#{size}) must be larger than 0."
      end
      if has_space?(address, size)
        PEROBS.log.fatal "The space with address #{address} and size #{size} " +
          "can't be added twice."
      end
      root.add_space(address, size)
    end

    # Get a space that has at least the requested size.
    # @param size [Integer] Required size in bytes
    # @return [Array] Touple with address and actual size of the space.
    def get_space(size)
      if size <= 0
        PEROBS.log.fatal "Size (#{size}) must be larger than 0."
      end

      if (address_size = root.find_matching_space(size))
        # First we try to find an exact match.
        return address_size
      elsif (address_size = root.find_equal_or_larger_space(size))
        return address_size
      else
        return nil
      end
    end

    # Delete the node at the given address in the SpaceTree file.
    # @param address [Integer] address in file
    def delete_node(address)
      @cache.delete(address)
      @nodes.delete_blob(address)
    end

    # Clear all pools and forget any registered spaces.
    def clear
      @nodes.clear
      @cache.clear
      @root_address = SpaceTreeNode::create(self).node_address
    end

    # Check if there is a space in the free space lists that matches the
    # address and the size.
    # @param [Integer] address Address of the space
    # @param [Integer] size Length of the space in bytes
    # @return [Boolean] True if space is found, false otherwise
    def has_space?(address, size)
      root.has_space?(address, size)
    end

    # Check if the index is OK and matches the flat_file data (if given).
    # @param flat_file [FlatFile] Flat file to compare with
    # @return True if space list matches, flase otherwise
    def check(flat_file = nil)
      @nodes.check
      root.check(flat_file)
    end

    # Iterate over all entries and yield address and size.
    def each
      root.each do |node, mode, stack|
        if mode == :on_enter
          yield(node.blob_address, node.size)
        end
      end
    end

    # Complete internal tree data structure as textual tree.
    # @return [String]
    def to_s
      root.to_tree_s
    end

    # Convert the tree into an Array of [address, size] touples.
    # @return [Array]
    def to_a
      root.to_a
    end

  end

end

