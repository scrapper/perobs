# encoding: UTF-8
#
# = SpaceTree.rb -- Persistent Ruby Object Store
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

    # Simple cache that can hold up to size SpaceTreeNode entries. Entries are
    # hashed with a simple node_address % size function. This keeps the
    # overhead for managing the cache extremely low yet giving an OK
    # probability to have cache hits.
    # @param size [Integer] maximum number of cache entries
    def initialize(size)
      @size = size
      clear
    end

    # Insert a node into the cache.
    # @param node [SpaceTreeNode]
    def insert(node)
      @entries[node.node_address % @size] = node
    end

    # Retrieve a node reference from the cache.
    # @param address [Integer] address of the node to retrieve.
    def get(address)
      node = @entries[address % @size]
      # We can have collisions. Check if the cached node really matches the
      # requested address.
      (node && node.node_address == address) ? node : nil
    end

    # Remove a node from the cache.
    # @param address [Integer] address of node to remove.
    def delete(address)
      index = address % @size
      if (node = @entries[index]) && node.node_address == address
        @entries[index] = nil
      end
    end

    # Remove all entries from the cache.
    def clear
      @entries = ::Array.new(@size)
    end

  end

end

