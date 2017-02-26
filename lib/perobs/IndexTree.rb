# encoding: UTF-8
#
# = IndexTree.rb -- Persistent Ruby Object Store
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
require 'perobs/IndexTreeNode'

module PEROBS

  # The IndexTree maps the object ID to the address in the FlatFile. The
  # search in the tree is much faster than the linear search in the FlatFile.
  class IndexTree

    # Determines how many levels of the IndexTree will be kept in memory to
    # accerlerate the access. A number of 7 will keep up to 21845 entries in
    # the cache but will accelerate the access to the FlatFile address.
    MAX_CACHED_LEVEL = 7

    attr_reader :nodes, :ids

    def initialize(db_dir)
      # Directory path used to store the files.
      @db_dir = db_dir

      # This FixedSizeBlobFile contains the nodes of the IndexTree.
      @nodes = FixedSizeBlobFile.new(db_dir, 'database_index',
                                     IndexTreeNode::NODE_BYTES)

      # The node sequence usually only reveals a partial match with the
      # requested ID. So, the leaves of the tree point to the object_id_index
      # file which contains the full object ID and the address of the
      # corresponding object in the FlatFile.
      @ids = FixedSizeBlobFile.new(db_dir, 'object_id_index', 2 * 8)

      # The first MAX_CACHED_LEVEL levels of nodes will be cached in memory to
      # improve access times.
      @node_cache = {}
    end

    # Open the tree files.
    def open
      @nodes.open
      @ids.open
      @root = IndexTreeNode.new(self, 0, 0)
    end

    # Close the tree files.
    def close
      @ids.close
      @nodes.close
    end

    # Flush out all unwritten data
    def sync
      @ids.sync
      @nodes.sync
    end

    # Delete all data from the tree.
    def clear
      @nodes.clear
      @ids.clear
      @node_cache = {}
      @root = IndexTreeNode.new(self, 0, 0)
    end

    # Return an IndexTreeNode object that corresponds to the given address.
    # @param nibble [Fixnum] Index of the nibble the node should correspond to
    # @param address [Integer] Address of the node in @nodes or nil
    def get_node(nibble, address = nil)
      if nibble >= 16
        # We only support 64 bit keys, so nibble cannot be larger than 15.
        PEROBS.log.fatal "Nibble must be within 0 - 15 but is #{nibble}"
      end
      # Generate a mask for the least significant bits up to and including the
      # nibble.
      mask = (2 ** ((1 + nibble) * 4)) - 1
      if address && (node = @node_cache[address & mask])
        # We have an address and have found the node in the node cache.
        return node
      else
        # We don't have a IndexTreeNode object yet for this node. Create it
        # with the data from the 'database_index' file.
        node = IndexTreeNode.new(self, nibble, address)
        # Add the node to the node cache if it's up to MAX_CACHED_LEVEL levels
        # down from the root.
        @node_cache[address & mask] = node if nibble <= MAX_CACHED_LEVEL
        return node
      end
    end

    # Delete a node from the tree that corresponds to the address.
    # @param nibble [Fixnum] The corresponding nibble for the node
    # @param address [Integer] The address of the node in @nodes
    def delete_node(nibble, address)
      if nibble >= 16
        # We only support 64 bit keys, so nibble cannot be larger than 15.
        PEROBS.log.fatal "Nibble must be within 0 - 15 but is #{nibble}"
      end
      # First delete the node from the node cache.
      mask = (2 ** ((1 + nibble) * 4)) - 1
      @node_cache.delete(address & mask)
      # Then delete it from the 'database_index' file.
      @nodes.delete_blob(address)
    end

    # Store a ID/value touple into the tree. The value can later be retrieved
    # by the ID again. IDs are always unique in the tree. If the ID already
    # exists in the tree, the value will be overwritten.
    # @param id [Integer] ID or key
    # @param value [Integer] value to store
    def put_value(id, value)
      #MAX_CACHED_LEVEL.downto(0) do |i|
      #  mask = (2 ** ((1 + i) * 4)) - 1
      #  if (node = @node_cache[value & mask])
      #    return node.put_value(id, value)
      #  end
      #end
      @root.put_value(id, value)
    end

    # Retrieve the value that was stored with the given ID.
    # @param id [Integer] ID of the value to retrieve
    # @return [Fixnum] value
    def get_value(id)
      @root.get_value(id)
    end

    # Delete the value with the given ID.
    # @param [Integer] id
    def delete_value(id)
      @root.delete_value(id)
    end

    # Check if the index is OK and matches the flat_file data.
    def check(flat_file)
      @root.check(flat_file, 0)
    end

    # Convert the tree into a human readable form.
    # @return [String]
    def inspect
      @root.inspect
    end

  end

end

