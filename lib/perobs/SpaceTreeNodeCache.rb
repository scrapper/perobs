# encoding: UTF-8
#
# = SpaceTreeNodeCache.rb -- Persistent Ruby Object Store
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

require 'perobs/SpaceTreeNode'

module PEROBS

  class SpaceTreeNodeCache

    # Utility class to store SpaceTreeNode objects and their
    # modified/not-modified state.
    class Entry < Struct.new(:node, :modified)
    end

    # Simple cache that can hold up to size SpaceTreeNode entries. Entries are
    # hashed with a simple node_address % size function. This keeps the
    # overhead for managing the cache extremely low yet giving an OK
    # probability to have cache hits. The cache also keeps track if a node is
    # still in memory or needs to be reloaded from the file. All node accesses
    # must always go through this cache to avoid having duplicate in-memory
    # nodes for the same on-disk node.
    # @param tree [SpaceTree] The tree that the cached nodes belong to.
    # @param size [Integer] maximum number of cache entries
    def initialize(tree, size)
      @tree = tree
      @size = size
      clear
    end

    # Insert an node into the cache.
    # @param node [SpaceTreeNode] Node to cache
    # @param modified [Boolean] True if the node was modified, false otherwise
    def insert(node, modified = true)
      # Store the node via its Ruby object ID instead of a direct reference.
      # This allows the node to be collected by the garbage collector.
      @in_memory_nodes[node.node_address] = node.object_id

      index = node.node_address % @size
      if (entry = @nodes[index]) && entry.modified
        # If the node entry is already in cache, we don't have to do anything.
        return if entry.node.node_address == node.node_address

        # If the new object will replace an existing entry in the cash we have
        # to save the object first.
        entry.node.save
      end
      @nodes[index] = Entry.new(node, modified)
    end

    # Retrieve a node reference from the cache.
    # @param address [Integer] address of the node to retrieve.
    def get(address)
      entry = @nodes[address % @size]
      # We may have collisions. Check if the cached node really matches the
      # requested address.
      return entry.node if entry && entry.node.node_address == address

      if (obj_id = @in_memory_nodes[address])
        # We have the node in memory so we can just return it.
        begin
          node = ObjectSpace._id2ref(obj_id)
          unless node.node_address == address
            raise RuntimeError, "In memory list is corrupted"
          end
          # Let's put the node in the cache. We might need it soon again.
          insert(node, false)
          return node
        rescue RangeError
          # Due to a race condition the object can still be in the
          # @in_memory_nodes list but has been collected already by the Ruby
          # GC. In that case we need to load it again.
          @in_memory_nodes.delete(address)
        end
      end

      SpaceTreeNode::load(@tree, address)
    end

    # Remove a node from the cache.
    # @param address [Integer] address of node to remove.
    def delete(address)
      # The object is likely still in memory, but we really don't want to
      # access it anymore.
      @in_memory_nodes.delete(address)

      index = address % @size
      if (entry = @nodes[index]) && entry.node.node_address == address
        @nodes[index] = nil
      end
    end

    # Remove a node from the in-memory list. This is an internal method
    # and should never be called from user code. It will be called from a
    # finalizer, so many restrictions apply!
    # @param node_address [Integer] Node address of the node to remove from
    #        the list
    def _collect(address)
      @in_memory_nodes.delete(address)
    end

    # Write all modified objects into the backing store.
    def flush
      @nodes.each do |entry|
        if entry && entry.modified
          entry.node.save
          entry.modified = false
        end
      end
    end

    # Remove all entries from the cache.
    def clear
      # A hash that stores all SpaceTreeNode objects by ID that are currently
      # in memory. Nodes are added via insert() and will be removed via
      # delete() or _collect() called from a SpaceTreeNode finalizer. It only
      # stores the node Ruby object ID hashed by their address in the file.
      # This enables them from being collected by the Ruby garbage collector.
      @in_memory_nodes = {}
      # This is the actual cache. The Array stores nodes as Entry objects to
      # also store the modified/not-modified state.
      @nodes = ::Array.new(@size)
    end

  end

end

