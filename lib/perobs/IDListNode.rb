# encoding: UTF-8
#
# = IDListNode.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2018 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/IDListPageFile'
require 'perobs/IDListPage'

module PEROBS

  # The IDListNode class provides the nodes of a tree that is used to store
  # the IDListPage objects. The root node is at level 0, the sub nodes of the
  # root node at level 1 and so on. On each level the nodes uses ORDER bits of
  # the ID to select which sub-node to branch into. The LSB of the masked bits
  # is at position ORDER * @level. The base_id specifies the bits that are
  # common to all values stored in the subtree or page. A leaf node references
  # a page by the page index. Tree nodes don't have a page but have 2 ** ORDER
  # references to their sub-IDListNode objects.
  class IDListNode

    # The ORDER determines the number of bits each node uses to select the sub
    # node. Each tree node has 2 ** ORDER sub nodes. Performance tests have
    # shown that an ORDER of 7 is the sweet spot that yields best resulsts.
    ORDER = 7

    attr_accessor :page_entries
    attr_reader :base_id, :level, :page_idx

    # Create a new IDListNode object.
    # @param page_file [IDListPageFile] The page file that is the primary
    #        owner of all IDListPage objects. This tree only references pages
    #        by their index in the page file.
    # @param base_id [Integer] The mask_bits LSBs of this value are common to
    #        all values stored in this subtree.
    # @param level [Integer] The level of the node in the tree. The root node
    #        has level 0, its children are on level 1 and so on.
    # @param page_idx [Integer] Specifies the page that holds all values of
    #        this node.
    def initialize(page_file, base_id, level, page_idx = nil)
      @page_file = page_file
      @base_id = base_id
      @level = level
      @bit_mask = (2 ** (@level * ORDER)) - 1

      if @base_id > @bit_mask
        raise ArgumentError, "base_id #{'%016X' % @base_id} must not be " +
          "larger than mask #{'%016X' % @bit_mask}"
      end

      # References to the subtree nodes. These must be all nil for leaf
      # nodes non referencing an IDListNode object for tree nodes.
      @sub_nodes = ::Array.new(2 ** ORDER, nil)

      # This page_entries counter must always correspond with the number of
      # entries in the page. It is usually set from the IDListPage object
      # whenever the entry count changed.
      @page_entries = 0
      # The index of the page in the IDListPageFile. This must be nil for all
      # non-leaf nodes.
      @page_idx = page_idx || @page_file.new_page(self)
    end

    # Insert a value into the subtree.
    # @param id [Integer] Value to insert
    def insert(id)
      if (id & @bit_mask) != @base_id
        raise ArgumentError,
          "ID #{'%016X' % id} does not belong to node with base_id " +
          "#{'%016X' % @base_id} and mask #{'%016X' % @bit_mask}"
      end

      if @page_idx
        p = page
        # The node is a leaf node. Check if we can add the value to the page.
        if p.is_full?
          return if p.include?(id)
          # The page is already full. We have to turn the node into a tree
          # node and then continue to traverse the tree. We only split the
          # pages if the value wasn't already included in the page. This
          # prevents empty pages to occur after a split.
          split_page
        else
          return p.insert(id)
        end
      end

      # We still have not found a leaf node. Use the for this node relevant
      # bits of the id to determine which subtree we have to descent into.
      @sub_nodes[node_bits(id)].insert(id)
    end

    # Check if the given value is already included in the subtree.
    # @param id [Integer] Value to check for
    # @return [True/False] True if value is found, false otherwise.
    def include?(id)
      if @page_idx
        # This is a leaf node. Check if the value is included in the page.
        page.include?(id)
      else
        # This is a tree node. Use the @mask_bits-th bit of id
        # to determine which subtree we have to descent into.
        @sub_nodes[node_bits(id)].include?(id)
      end
    end

    def node(id)
      return self if @page_idx

      @sub_nodes[node_bits(id)].node(id)
    end

    def page_entries=(value)
      @page_entries = value
    end

    def check
      all_nodes_are_nil = true
      all_nodes_are_set = true
      @sub_nodes.each do |n|
        all_nodes_are_nil = false if !n.nil?
        all_nodes_are_set = false if n.nil?
      end
      if @page_idx
        unless all_nodes_are_nil
          raise RuntimeError, "All subnodes must be nil if a @page_idx is set"
        end

        page.check
      else
        unless all_nodes_are_set
          raise RuntimeError, "All subnodes must be set unless a @page_idx " +
            "is set"
        end

        @sub_nodes.each { |n| n.check }
      end
    end

    def to_s(prefix = '')
      s = "#{prefix}#{prefix[-1] == ' ' ? '`' : '-'}+ " +
        "L: #{@level} BaseID: #{@base_id}" +
        "#{ @page_idx ? " #{page.to_s}" : ''}\n"
      unless @page_idx
        @sub_nodes.each_with_index do |n, i|
          decorator = i < @sub_nodes.length - 1 ? ' |' : '  '
          s += n.to_s(prefix + decorator) unless n.nil?
        end
      end

      s
    end

    private

    def node_bits(id)
      (id >> (@level * ORDER)) & ((2 ** ORDER) - 1)
    end

    def page
      # The leaf pages reference the IDListPage objects only by their index.
      # This method will convert the index into a reference to the actual
      # object. These references should be very short-lived as a life
      # reference prevents the page object from being collected.
      @page_file.page(@page_idx)
    end

    def split_page
      p = page
      # When the page becomes full, we have to convert the leaf node into a
      # tree node with two new leaf nodes. The zero node will inherit the page
      # reference.
      @sub_nodes[0] = IDListNode.new(@page_file, @base_id, @level + 1,
                                     @page_idx)
      # Delete the page reference as this node is no longer a leaf node.
      @page_idx = nil
      # The page now belongs to the zero node.
      p.node = @sub_nodes.first
      # The one node will get a new page. The base_id of the one node gets a 1
      # bit added to the left.
      1.upto(@sub_nodes.length - 1) do |i|
        next_base_id = (i << (@level * ORDER)) + @base_id
        @sub_nodes[i] = IDListNode.new(@page_file, next_base_id, @level + 1)
        # Remove all values from old page that now belong in this sub node and
        # move them over.
        mask = (1 << ((@level + 1) * ORDER)) - 1
        p.delete(mask, next_base_id).each { |id| @sub_nodes[i].insert(id) }
      end
    end

  end

end

