# encoding: UTF-8
#
# = BigTree.rb -- Persistent Ruby Object Store
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

require 'perobs/Object'
require 'perobs/BigTreeNode'

module PEROBS

  # The BigTree class implements a BTree as a PEROBS object. It allows to
  # manage huge amounts of data in a reasonably efficient way. The number of
  # entries is limited by the space on the backing store, not the main
  # memory. Entries are addressed by a Integer key.
  class BigTree < PEROBS::Object

    attr_persist :node_size, :root

    # Internal constructor. Use Store.new() instead.
    # @param p [Handle]
    # @param node_size [Integer] The size of the tree nodes. This determines
    #        how many entries must be read/written for each operation.
    def initialize(p, node_size = 127)
      super(p)
      unless node_size % 2 == 1
        PEROBS.log.fatal "Node size (#{node_size}) must be uneven"
      end
      self.node_size = node_size
      clear
    end

    # Remove all entries from the BigTree.
    def clear
      self.root = @store.new(BigTreeNode, myself, true)
    end

    # Insert a new value into the tree using the key as a unique index. If the
    # key already exists the old value will be overwritten.
    # @param key [Integer] Unique key
    # @param value [Integer] value
    def insert(key, value)
      @store.transaction do
        @root.insert(key, value)
      end
    end

    # Retrieve the value associated with the given key. If no entry was found,
    # return nil.
    # @param key [Integer] Unique key
    # @return [Integer or nil] found value or nil
    def get(key)
      @root.get(key)
    end

    # Find and remove the value associated with the given key. If no entry was
    # found, return nil, otherwise the found value.
    # @param key [Integer] Unique key
    # @return [Integer or nil] found value or nil
    def remove(key)
      removed_value = nil

      @store.transaction do
        removed_value = @root.remove(key)

        # Check if the root node only contains one child link after the delete
        # operation. Then we can delete that node and pull the tree one level
        # up. This could happen for a sequence of nodes that all got merged to
        # single child nodes.
        while !@root.is_leaf? && @root.children.size == 1
          old_root = @root
          set_root(@root.children.first)
          @root.parent = nil
        end
      end

      removed_value
    end

    # @return [Integer] The number of entries stored in the tree.
    def length
      i = 0
      each { |k, v| i += 1 }
      i
    end

    # Iterate over all entries in the tree. Entries are always sorted by the
    # key.
    # @yield [key, value]
    def each(&block)
      @root.each(&block)
    end

    # @return [String] Human reable form of the tree.
    def to_s
      @root.to_s
    end

    # Check if the tree file contains any errors.
    # @return [Boolean] true if no erros were found, false otherwise
    def check(&block)
      @root.check(&block)
    end

    # Internal method.
    def set_root(root)
      @root = root
    end

  end

end
