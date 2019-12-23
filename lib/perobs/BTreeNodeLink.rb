# encoding: UTF-8
#
# = BTreeNodeLink.rb -- Persistent Ruby Object Store
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

module PEROBS

  # This class is used to form the links between the in-memory BTreeNode
  # objects. The link is based on the address of the node in the file. The
  # class objects transparently convert the address into a corresponding
  # BTreeNode object and pass on all method calls.
  class BTreeNodeLink

    attr_reader :node_address

    # Create a new BTreeNodeLink object.
    # @param tree [BTree] The BTree that holds the nodes.
    # @param node_or_address [BTreeNode or BTreeNodeLink or Integer] a
    #        BTreeNode, BTreeNodeLink reference or the node
    #        address in the file.
    def initialize(tree, node_or_address)
      @tree = tree
      if node_or_address.is_a?(BTreeNode) ||
         node_or_address.is_a?(BTreeNodeLink)
        @node_address = node_or_address.node_address
      elsif node_or_address.is_a?(Integer)
        @node_address = node_or_address
      else
        PEROBS.log.fatal "Unsupported argument type #{node_or_address.class}"
      end
      if @node_address == 0
        PEROBS.log.fatal "Node address may not be 0"
      end
    end

    # All calls to a BTreeNodeLink object will be forwarded to the
    # corresponding BTreeNode object. If that
    def method_missing(method, *args, &block)
      #$stderr.puts "Method missing: #{method}"
      get_node.send(method, *args, &block)
    end

    # Make it properly introspectable.
    def respond_to?(method, include_private = false)
      get_node.respond_to?(method)
    end

    # Directly define some commonly used methods to avoid the method_missing
    # overhead.
    def is_leaf
      get_node.is_leaf
    end

    def keys
      get_node.keys
    end

    def values
      get_node.values
    end

    def children
      get_node.children
    end

    def get(key)
      get_node.get(key)
    end

    def search_key_index(key)
      get_node.search_key_index(key)
    end

    def insert(key, value)
      get_node.insert(key, value)
    end

    def insert_element(key, voc)
      get_node.insert_element(key, voc)
    end

    def split_node
      get_node.split_node
    end

    # Compare this node to another node.
    # @return [Boolean] true if node address is identical, false otherwise
    def ==(node)
      @node_address == node.node_address
    end

    # Compare this node to another node.
    # @return [Boolean] true if node address is not identical, false otherwise
    def !=(node)
      if node.nil?
        return !@node_address.nil?
      end

      @node_address != node.node_address
    end

    def is_top?
      get_node.is_top?
    end

    # Check the link to a sub-node. This method silently ignores all errors if
    # the sub-node does not exist.
    # @return [Boolean] True if link is OK, false otherweise
    def check_node_link(branch, stack)
      begin
        return get_node.check_node_link(branch, stack)
      rescue
        return false
      end
    end

    # @return Textual version of the BTreeNode
    def to_s
      get_node.to_s
    end

    def get_node
      @tree.node_cache.get(@node_address)
    end

  end

end

