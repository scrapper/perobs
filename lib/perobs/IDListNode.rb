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

  # The IDListNode class provides the nodes of the binary tree that is used to
  # store the IDListPage objects. The tree uses mask_bits least
  # significant bits of the base_id to describe the values that are stored in
  # the sub-tree or page. Descending the tree from the root adds an LSB for
  # each level. The base_id specifies the bits that are common to all values
  # stored in the subtree or page. A leaf node references a page by the page
  # index. Tree nodes don't have a page but reference to other IDListNode
  # objects. The zero subtree stores all values that have this particular bit
  # (as specified by the node level) set to 0. The one subtree stores all
  # values that have the bit set to 1.
  class IDListNode

    attr_accessor :page_entries
    attr_reader :base_id, :mask_bits, :page_idx

    # Create a new IDListNode object.
    # @param page_file [IDListPageFile] The page file that is the primary
    #        owner of all IDListPage objects. This tree only references pages
    #        by their index in the page file.
    # @param base_id [Integer] The mask_bits LSBs of this value are common to
    #        all values stored in this subtree.
    # @param mask_bits [Integer] Number of bits
    # @param page_idx [Integer] Specifies the page that holds all values of
    #        this node.
    def initialize(page_file, base_id, mask_bits, page_idx = nil)
      @page_file = page_file
      @base_id = base_id
      @mask_bits = mask_bits

      if @base_id > ((2 ** @mask_bits) - 1)
        raise ArgumentError, "base_id #{'%016X' % @base_id} must not be " +
          "larger than mask #{'%016X' % ((2 ** @mask_bits) - 1)}"
      end

      # References to the subtree nodes. These must be both nil for leaf
      # nodes.
      @zero = nil
      @one = nil

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
      if (id & ((2 ** @mask_bits) - 1)) != @base_id
        raise ArgumentError,
          "ID #{'%016X' % id} does not belong to node with base_id " +
          "#{'%016X' % @base_id} and mask #{(2 ** @mask_bits) - 1}"
      end

      if @page_idx
        # The node is a leaf node. Check if we can add the value to the page.
        if page.is_full?
          return if page.include?(id)
          # The page is already full. We have to turn the node into a tree
          # node and then continue to traverse the tree. We only split the
          # pages if the value wasn't already included in the page. This
          # prevents empty pages to occur after a split.
          split_page
        else
          # We can insert the value into the page.
          page.insert(id)
          return
        end
      end

      # We still have not found a leaf node. Use the @mask_bits-th bit of id
      # to determine which subtree we have to descent into.
      if (id & (1 << @mask_bits)) != 0
        @one.insert(id)
      else
        @zero.insert(id)
      end
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
        if (id & (1 << @mask_bits)) != 0
          @one.include?(id)
        else
          @zero.include?(id)
        end
      end
    end

    def node(id)
      return self if @page_idx

      (id & (1 << @mask_bits)) != 0 ? @one.node(id) : @zero.node(id)
    end

    def page_entries=(value)
      @page_entries = value
    end

    def check
      if (@zero.nil? && !@one.nil?) || (!@zero.nil? && @one.nil?)
        raise RuntimeError, "@zero and @one must either both be nil or not nil"
      end

      if @zero.nil? && @page_idx.nil?
        raise RuntimeError, "@zero and @page_idx can't both be nil"
      end
      if !@zero.nil? && !@page_idx.nil?
        raise RuntimeError, "@zero and @page_idx can't both be not nil"
      end

      if @page_idx
        page.check
      else
        @zero.check
        @one.check
      end
    end

    private

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
      @zero = IDListNode.new(@page_file, @base_id, @mask_bits + 1, @page_idx)
      # Delete the page reference as this node is no longer a leaf node.
      @page_idx = nil
      # The page now belongs to the zero node.
      p.node = @zero
      # The one node will get a new page. The base_id of the one node gets a 1
      # bit added to the left.
      next_base_id = (1 << @mask_bits) + @base_id
      @one = IDListNode.new(@page_file, next_base_id, @mask_bits + 1)
      # Remove all values that have a 1 bit at the specific bit for this new
      # level and insert them into the one node.
      p.delete(1 << @mask_bits).each { |id| @one.insert(id) }
    end

  end

end

