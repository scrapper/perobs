# encoding: UTF-8
#
# = SpaceTreeNodeLink.rb -- Persistent Ruby Object Store
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

  # This class is used to form the links between the in-memory SpaceTreeNode
  # objects. The link is based on the address of the node in the file. The
  # class objects transparently convert the address into a corresponding
  # SpaceTreeNode object and pass on all method calls.
  class SpaceTreeNodeLink

    attr_reader :node_address

    # Create a new SpaceTreeNodeLink object.
    # @param tree [SpaceTree] The SpaceTree that holds the nodes.
    # @param node [SpaceTreeNode or SpaceTreeNodeLink or Integer] a
    #             SpaceTreeNode, SpaceTreeNodeLink reference or the node
    #             address in the file.
    def initialize(tree, node_or_address)
      @tree = tree
      if node_or_address.is_a?(SpaceTreeNode) ||
         node_or_address.is_a?(SpaceTreeNodeLink)
        @node_address = node_or_address.node_address
      elsif node_or_address.is_a?(Integer)
        @node_address = node_or_address
      else
        PEROBS.log.fatal "Unsupported argument type #{node_or_address.class}"
      end
    end

    # All calls to a SpaceTreeNodeLink object will be forwarded to the
    # corresponding SpaceTreeNode object. If that
    def method_missing(method, *args, &block)
      get_node.send(method, *args, &block)
    end

    # Make it properly introspectable.
    def respond_to?(method, include_private = false)
      get_node.respond_to?(method)
    end

    # Compare this node to another node.
    # @return [Boolean] true if node address is identical, false otherwise
    def ==(node)
      @node_address == node.node_address
    end

    # Compare this node to another node.
    # @return [Boolean] true if node address is not identical, false otherwise
    def !=(node)
      @node_address != node.node_address
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

    # @return Textual version of the SpaceTreeNode
    def to_s
      get_node.to_s
    end

    private

    def get_node
      @tree.get_node(@node_address)
    end

  end

end

