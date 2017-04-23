# encoding: UTF-8
#
# = BTree.rb -- Persistent Ruby Object Store
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

require 'perobs/BTreeNode'

module PEROBS

  class BTreeNodeCache

    def initialize(size)
      clear
    end

    def [](address)
      if (node = @modified_nodes[address])
        return node
      end

      if (node = @nodes[address])
        return node
      end

      nil
    end

    def insert(node)
      unless node
        PEROBS.log.fatal "nil cannot be cached"
      end

      @nodes[node.node_address] = node
    end

    def mark_as_modified(node)
      @modified_nodes[node.node_address] = node
      #@nodes[node.node_address] = node
    end

    # Remove a node from the cache.
    # @param address [Integer] address of node to remove.
    def delete(address)
      @nodes.delete(address)
      @modified_nodes.delete(address)
    end

    # Flush all dirty nodes into the backing store.
    def flush
      @modified_nodes.each_value { |node| node.write_node }
      @modified_nodes = {}
      @nodes.delete_if { |address, node| node.parent }
    end

    # Remove all nodes from the cache.
    def clear
      @nodes = {}
      @modified_nodes = {}
    end

  end

end

