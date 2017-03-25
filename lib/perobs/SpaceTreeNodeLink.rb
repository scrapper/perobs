# encoding: UTF-8
#
# = SpaceTreeNodeLink.rb -- Persistent Ruby Object Store
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

require 'weakref'

module PEROBS

  # This class is used to form the links between the in-memory SpaceTreeNode
  # objects. A SpaceTreeNodeLink uses a WeakRef object to reference the actual
  # SpaceTreeNode object. This allows the referenced object to be garbage
  # collected. In case the link needs to be followed again, the SpaceTreeNode
  # objects will be reconstructed from the SpaceTree blob file. This provides
  # a simple mechanism to keep parts of the SpaceTree in memory for faster
  # access times but without consuming too much memory.
  class SpaceTreeNodeLink

    # Create a new SpaceTreeNodeLink object.
    # @param tree [SpaceTree] The SpaceTree that holds the nodes.
    # @param node_address [Integer] The address of the SpaceTreeNode in the
    #        file.
    def initialize(tree, node_address)
      @tree = tree
      @node_address = node_address
      @node = nil
    end

    # All calls to a SpaceTreeNodeLink object will be forwarded to the
    # corresponding SpaceTreeNode object. If that
    def method_missing(method, *args, &block)
      ensure_node
      @node.send(method, *args, &block)
    end

    # Make it properly introspectable.
    def respond_to?(method, include_private = false)
      ensure_node
      @node.respond_to?(method)
    end

    private

    def ensure_node
      unless @node && @node.weakref_alive?
        @node = WeakRef.new(@tree.get_node(@node_address))
      end
    end

  end

end

