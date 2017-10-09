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

    def initialize(tree)
      @tree = tree
      clear
    end

    def get(address)
      if (node = @modified_nodes[address])
        return node
      end

      if (node = @top_nodes[address])
        return node
      end

      if (node = @ephemeral_nodes[address])
        return node
      end

      BTreeNode::load(@tree, address)
    end

    def set_root(node)
      node = node.get_node if node.is_a?(BTreeNodeLink)

      @top_nodes = {}
      @top_nodes[node.node_address] = node
    end

    def insert(node, modified = true)
      unless node
        PEROBS.log.fatal "nil cannot be cached"
      end
      node = node.get_node if node.is_a?(BTreeNodeLink)

      if modified
        @modified_nodes[node.node_address] = node
      end
      @ephemeral_nodes[node.node_address] = node

      if !@top_nodes.include?(node) && node.is_top?
        @top_nodes[node.node_address] = node
      end
    end

    def _collect(address)
      # Just a dummy for now
    end

    # Remove a node from the cache.
    # @param address [Integer] address of node to remove.
    def delete(address)
      @ephemeral_nodes.delete(address)
      @top_nodes.delete(address)
      @modified_nodes.delete(address)
    end

    # Flush all dirty nodes into the backing store.
    def flush(now = false)
      if now || @modified_nodes.size > 1024
        @modified_nodes.each_value { |node| node.write_node }
        @modified_nodes = {}
      end
      @ephemeral_nodes = {}
    end

    # Remove all nodes from the cache.
    def clear
      @top_nodes = {}
      @ephemeral_nodes = {}
      @modified_nodes = {}
    end

  end

end

