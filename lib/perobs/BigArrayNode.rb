# encoding: UTF-8
#
# = BigArrayNode.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2017, 2018, 2019
# by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/Array'

module PEROBS

  # The BigArrayNode class provides the BTree nodes for the BigArray objects.
  # A node can either be a branch node or a leaf node. Branch nodes don't
  # store values, only offsets and references to child nodes. Leaf nodes don't
  # have child nodes but store the actual values. The leaf nodes always
  # contain at least node_size / 2 number of consecutive values. The index of
  # the first value in the BigArray is the sum of the offsets stored in the
  # parent nodes. Branch nodes store the offsets and the corresponding
  # child node references. The first offset is always 0. Consecutive offsets
  # are set to the previous offset plus the total number of values stored in
  # the previous child node. The leaf nodes don't contain wholes. A
  # concatenation of all leaf node values represents the stored Array.
  #
  # Root Node             +--------------------------------+
  # Offsets               | 0                           11 |
  # Children                |                            |
  #                         v                            v
  # Level 1    +--------------------------++--------------------------+
  # Offsets    | 0         4         7    ||   0          2         5 |
  # Children     |         |         |         |          |         |
  #              v         v         v         v          v         v
  # Leaves  +---------++-------++----------++-------++----------++-------+
  # Values  | A B C D || E F G || H I J K  || L  M  || N  O  P  || Q  R  |
  #
  # Index     0 1 2 3    4 5 6    7 8 9 10    11 12    13 14 15    16 17
  #
  class BigArrayNode < PEROBS::Object

    attr_persist :tree, :parent, :offsets, :values, :children,
      :prev_sibling, :next_sibling

    # Internal constructor. Use Store.new(BigArrayNode, ...) instead.
    # @param p [Handle]
    # @param tree [BigArray] The tree this node should belong to
    # @param is_leaf [Boolean] True if a leaf node should be created, false
    #        for a branch node.
    # @param parent [BigArrayNode] Parent node
    # @param prev_sibling [BigArrayNode] Previous sibling
    # @param next_sibling [BigArrayNode] Next sibling
    def initialize(p, tree, is_leaf, parent = nil,
                   prev_sibling = nil, next_sibling = nil)
      super(p)
      self.tree = tree
      self.parent = parent

      if is_leaf
        # Create a new leaf node. It stores values and has no children.
        self.values = @store.new(PEROBS::Array)
        self.children = self.offsets = nil

        # Link the neighboring siblings to the newly inserted node.  If the
        # node has no sibling on a side we also must register it as first or
        # last leaf with the BigArray object.
        if (self.prev_sibling = prev_sibling)
          @prev_sibling.next_sibling = myself
        else
          @tree.first_leaf = myself
        end
        if (self.next_sibling = next_sibling)
          @next_sibling.prev_sibling = myself
        else
          @tree.last_leaf = myself
        end
      else
        # Create a new branch node. It stores keys and child node references
        # but no values.
        self.offsets = @store.new(PEROBS::Array)
        self.children = @store.new(PEROBS::Array)
        self.values = nil
        # Branch nodes don't need sibling links.
        self.prev_sibling = self.next_sibling = nil
      end
    end

    # @return [Boolean] True if this is a leaf node, false otherwise.
    def is_leaf?
      @children.nil?
    end

    def size
      is_leaf? ? @values.size : @children.size
    end

    # @return [Integer] the number of values stored in this node.
    def values_count
      count = 0
      node = self
      while node
        if node.is_leaf?
          return count + node.values.size
        else
          count += node.offsets.last
          node = node.children.last
        end
      end
    end


    # Set the given value at the given index.
    # @param index [Integer] Position to insert at
    # @param value [Integer] value to insert
    def set(index, value)
      node = self

      # Traverse the tree to find the right node to add or replace the value.
      while node do
        # Once we have reached a leaf node we can insert or replace the value.
        if node.is_leaf?
          if index >= node.values.size
            node.fatal "Set index (#{index}) larger than values array " +
              "(#{node.values.size})."
          end
          node.values[index] = value
          return
        else
          # Descend into the right child node to add the value to.
          cidx = node.search_child_index(index)
          index -= node.offsets[cidx]
          node = node.children[cidx]
        end
      end

      node.fatal "Could not find proper node to set the value while " +
        "looking for index #{index}"
    end

    # Insert the given value at the given index. All following values will be
    # pushed to a higher index.
    # @param index [Integer] Position to insert at
    # @param value [Integer] value to insert
    def insert(index, value)
      node = self
      cidx = nil

      # Traverse the tree to find the right node to add or replace the value.
      while node do
        # All nodes that we find on the way that are full will be split into
        # two half-full nodes.
        if node.size >= @tree.node_size
          # Re-add the index from the last parent node since we will descent
          # into one of the split nodes.
          index += node.parent.offsets[cidx] if node.parent
          node = node.split_node
        end

        # Once we have reached a leaf node we can insert or replace the value.
        if node.is_leaf?
          node.values.insert(index, value)
          node.parent.adjust_offsets(node, 1) if node.parent
          return
        else
          # Descend into the right child node to add the value to.
          cidx = node.search_child_index(index)
          if (index -= node.offsets[cidx]) < 0
            node.fatal "Index (#{index}) became negative"
          end
          node = node.children[cidx]
        end
      end

      node.fatal "Could not find proper node to insert the value while " +
        "looking for index #{index}"
    end

    # Return the value that matches the given key or return nil if they key is
    # unknown.
    # @param index [Integer] Position to insert at
    # @return [Integer or nil] value that matches the key
    def get(index)
      node = self

      # Traverse the tree to find the right node to add or replace the value.
      while node do
        # Once we have reached a leaf node we can insert or replace the value.
        if node.is_leaf?
          return node.values[index]
        else
          # Descend into the right child node to add the value to.
          cidx = (node.offsets.bsearch_index { |o| o > index } ||
                  node.offsets.length) - 1
          if (index -= node.offsets[cidx]) < 0
            node.fatal "Index (#{index}) became negative"
          end
          node = node.children[cidx]
        end
      end

      PEROBS.log.fatal "Could not find proper node to get from while " +
        "looking for index #{index}"
    end

    # Delete the element at the specified index, returning that element, or
    # nil if the index is out of range.
    # @param index [Integer] Index in the BigArray
    # @return [Object] found value or nil
    def delete_at(index)
      node = self
      deleted_value = nil

      while node do
        if node.is_leaf?
          deleted_value = node.values.delete_at(index)
          if node.parent
            node.parent.adjust_offsets(node, -1)
            if node.size < min_size
              node.parent.consolidate_child_nodes(node)
            end
          end

          return deleted_value
        else
          # Descend into the right child node to add the value to.
          cidx = (node.offsets.bsearch_index { |o| o > index } ||
                  node.offsets.length) - 1
          if (index -= node.offsets[cidx]) < 0
            node.fatal "Index (#{index}) became negative"
          end
          node = node.children[cidx]
        end
      end

      PEROBS.log.fatal "Could not find proper node to delete from while " +
        "looking for index #{index}"
    end

    # Iterate over all the values of the node.
    # @yield [value]
    def each
      return nil unless is_leaf?

      @values.each do |v|
        yield(v)
      end
    end

    # Iterate over all the values of the node in reverse order.
    # @yield [value]
    def reverse_each
      return nil unless is_leaf?

      @values.reverse_each do |v|
        yield(v)
      end
    end

    # Check consistency of the node and all subsequent nodes. In case an error
    # is found, a message is logged and false is returned.
    # @yield [key, value]
    # @return [Boolean] true if tree has no errors
    def check
      branch_depth = nil

      traverse do |node, position, stack|
        if position == 0
          # Nodes should have between min_size() and
          # @tree.node_size children or values. Only the root node may have
          # less.
          if node.size > @tree.node_size
            node.error "BigArray node #{node._id} is too large. It has " +
              "#{node.size} nodes instead of max. #{@tree.node_size}."
            return false
          end
          if node.parent && node.size < min_size
            node.error "BigArray node #{node._id} is too small"
            return false
          end

          if node.is_leaf?
            # All leaf nodes must have same distance from root node.
            if branch_depth
              unless branch_depth == stack.size
                node.error "All leaf nodes must have same distance from root"
                return false
              end
            else
              branch_depth = stack.size
            end

            return false unless node.check_leaf_node_links

            if node.children
              node.error "children must be nil for a leaf node"
              return false
            end
          else
            unless node.children.size == node.offsets.size
              node.error "Offset count (#{node.offsets.size}) must be equal " +
                "to children count (#{node.children.size})"
                return false
            end

            if node.values
              node.error "values must be nil for a branch node"
              return false
            end

            unless @prev_sibling.nil? && @next_sibling.nil?
              node.error "prev_sibling and next_sibling must be nil for " +
                "branch nodes"
            end

            return false unless node.check_offsets

            return false unless node.check_child_nodes(stack)
          end
        elsif position <= node.size
          # These checks are done after we have completed the respective child
          # node with index 'position - 1'.
          index = position - 1
          if node.is_leaf?
            if block_given?
              # If a block was given, call this block with the key and value.
              return false unless yield(node.first_index + index,
                                        node.values[index])
            end
          end
        end
      end

      true
    end

    def check_leaf_node_links
      if @prev_sibling.nil?
        if @tree.first_leaf != self
          error "Leaf node #{@_id} has no previous sibling " +
            "but is not the first leaf of the tree"
          return false
        end
      elsif @prev_sibling.next_sibling != self
        error "next_sibling of previous sibling does not point to " +
          "this node"
        return false
      end

      if @next_sibling.nil?
        if @tree.last_leaf != self
          error "Leaf node #{@_id} has no next sibling " +
            "but is not the last leaf of the tree"
          return false
        end
      elsif @next_sibling.prev_sibling != self
        error "previous_sibling of next sibling does not point to " +
          "this node"
        return false
      end

      true
    end

    def check_offsets
      return true if @parent.nil? && @offsets.empty?

      if @offsets[0] != 0
        error "First offset is not 0: #{@offsets.inspect}"
        return false
      end

      last_offset = nil
      @offsets.each_with_index do |offset, i|
        if i > 0
          if offset < last_offset
            error "Offset are not strictly monotoneously " +
              "increasing: #{@offsets.inspect}"
            return false
          end
          expected_offset = last_offset + @children[i - 1].values_count
          if offset != expected_offset
            error "Offset #{i} must be #{expected_offset} " +
              "but is #{offset}."
            return false
          end
        end

        last_offset = offset
      end

      true
    end

    def check_child_nodes(stack)
      if @children.uniq.size != @children.size
        error "Node #{@_id} has multiple identical children"
        return false
      end

      @children.each_with_index do |child, i|
        unless child.is_a?(BigArrayNode)
          error "Child #{@_id} is of class #{child.class} " +
            "instead of BigArrayNode"
          return false
        end

        unless child.parent.is_a?(BigArrayNode)
          error "Parent reference of child #{i} is of class " +
            "#{child.class} instead of BigArrayNode"
          return false
        end

        if child.parent != self
          error "Child node #{child._id} has wrong parent " +
            "#{child.parent._id}. It should be #{@_id}."
          return false
        end

        if child == self
          error "Child #{i} point to self"
          return false
        end

        if stack.include?(child)
          error "Child #{i} points to ancester node"
          return false
        end

        unless child.parent == self
          error "Child #{i} does not have parent pointing " +
            "to this node"
          return false
        end
      end

      true
    end

    # @return [String] Human reable form of the sub-tree.
    def to_s
      str = ''

      traverse do |node, position, stack|
        if position == 0
          begin
            str += "#{node.parent ? node.parent.tree_prefix + '  +' : 'o'}" +
              "#{node.tree_branch_mark}-" +
              "#{node.size == 0 ? '--' : 'v-'}#{node.tree_summary}\n"
          rescue => e
            str += "@@@@@@@@@@: #{e.message}\n"
          end
        else
          begin
            if node.is_leaf?
              if node.values[position - 1]
                str += "#{node.tree_prefix}  " +
                  "#{position == node.size ? '-' : '|'} " +
                  "[ #{node.value_index(position - 1)}: " +
                  "#{node.values[position - 1]} ]\n"
              end
            end
          rescue => e
            str += "@@@@@@@@@@: #{e.message}\n"
          end
        end
      end

      str
    end

    # Split the current node into two nodes. The upper half of the elements
    # will be moved into a newly created node. This node will retain the lower
    # half.
    # @return [BigArrayNode] common parent of the two nodes
    def split_node
      unless @parent
        # The node is the root node. We need to create a parent node first.
        self.parent = @store.new(BigArrayNode, @tree, false)
        @parent.offsets[0] = 0
        @parent.children[0] = myself
        @tree.root = @parent
      end

      # Create the new sibling that will take the 2nd half of the
      # node content.
      sibling = @store.new(BigArrayNode, @tree, is_leaf?, @parent, myself,
                           @next_sibling)
      # Determine the index of the middle element that gets moved to the
      # parent. The node size must be an uneven number.
      mid = size / 2
      if is_leaf?
        # Before:
        #    +--------------------------+
        #    | 0         4         7    |
        #      |         |         |
        #      v         v         v
        # +---------++-------++----------+
        # | A B C D || E F G || H I J K  |
        #
        # After:
        #    +--------------------------+
        #    | 0    2       4         7 |
        #      |    |       |         |
        #      v    v       v         v
        # +-----++----++-------++----------+
        # | A B || C D || E F G || H I J K  |
        #
        #
        # Insert the middle element key into the parent node
        @parent.insert_child_after_peer(mid, sibling, self)
        # Copy the values from the mid element onwards into the new
        # sibling node.
        sibling.values += @values[mid..-1]
        # Delete the copied offsets and values from this node.
        @values.slice!(mid..-1)
      else
        # Before:
        #    +--------------+
        #    | 0         11 |
        #      |          |
        #      v          v
        # +----------++-------+
        # | 0 4 7 10 || 0 2 5 |
        #   | | | |     | | |
        #   v v v v     v v v
        #
        # After:
        #  +------------------+
        #  | 0      7      11 |
        #    |      |       |
        #    v      v       v
        # +-----++-----++-------+
        # | 0 4    0 3 || 0 2 5 |
        #   | |    | |    | | |
        #   v v    v v    v v v
        #
        # Insert the new sibling into the parent node.
        offset_delta = @offsets[mid]
        @parent.insert_child_after_peer(offset_delta, sibling, self)
        # Copy the offsets from after the mid value onwards to the new sibling
        # node. We substract the offset delta from each of them.
        sibling.offsets += @offsets[mid..-1].map{ |v| v - offset_delta }
        # Delete the copied offsets from this node.
        @offsets.slice!(mid..-1)
        # Same copy for the children.
        sibling.children += @children[mid..-1]
        # Reparent the children to the new sibling parent.
        sibling.children.each { |c| c.parent = sibling }
        # And delete the copied children references.
        @children.slice!(mid..-1)
      end

      @parent
    end

    def insert_child_after_peer(offset, node, peer = nil)
      peer_index = @children.find_index(peer)
      cidx = peer_index ? peer_index + 1 : 0
      @offsets.insert(cidx, @offsets[peer_index] + offset)
      @children.insert(cidx, node)
    end

    def consolidate_child_nodes(child)
      unless (child_index = @children.index(child))
        error "Cannot find child to consolidate"
      end

      if child_index == 0
        # Consolidate with successor if it exists.
        return unless (succ = @children[child_index + 1])

        if child.size + succ.size <= @tree.node_size
          # merge child with successor
          merge_child_with_next(child_index)
        else
          move_first_element_of_successor_to_child(child_index)
        end
      else
        # consolidate with predecessor
        pred = @children[child_index - 1]

        if pred.size + child.size <= @tree.node_size
          # merge child with predecessor
          merge_child_with_next(child_index - 1)
        else
          move_last_element_of_predecessor_to_child(child_index)
        end
      end
    end

    # @param index [offset] offset to search the child index for
    # @return [Integer] Index of the matching offset or the insert position.
    def search_child_index(offset)
      # Handle special case for empty offsets list.
      return 0 if @offsets.empty? || offset <= @offsets.first

      (@offsets.bsearch_index { |o| o >= offset } || @offsets.length) - 1
    end

    # @return The index of the current node in the children list of the parent
    # node. If the node is the root node, nil is returned.
    def index_in_parent_node
      return nil unless @parent

      @parent.children.find_index(self)
     end

    def first_index
      # TODO: This is a very expensive method. Find a way to make this way
      # faster.
      node = parent
      child = myself
      while node
        if (index = node.children.index(child)) && index > 0
          return node.offsets[index - 1]
        end
        child = node
        node = node.parent
      end

      0
    end

    # Compute the array index of the value with the given index in the current
    # node.
    # @param idx [Integer] Index of the value in the current node
    # @return [Integer] Array index of the value
    def value_index(idx)
      node = self
      while node.parent
        idx += node.parent.offsets[node.index_in_parent_node]
        node = node.parent
      end

      idx
    end

    # This method takes care of adjusting the offsets in tree in case elements
    # were inserted or removed. All nodes that hold children after the
    # insert/remove operation needs to be adjusted. Since child nodes get their
    # offsets via their parents, only the parent node and the direct ancestor
    # followers need to be adjusted.
    # @param after_child [BigArrayNode] specifies the modified leaf node
    # @param delta [Integer] specifies how many elements were inserted or
    #        removed.
    def adjust_offsets(after_child, delta)
      node = self

      while node
        adjust = false
        0.upto(node.children.size - 1) do |i|
          # Iterate over the children until we have found the after_child
          # node. Then turn on adjustment mode. The offsets of the following
          # entries will be adjusted by delta.
          if adjust
            node.offsets[i] += delta
          elsif node.children[i] == after_child
            adjust = true
          end
        end

        unless adjust
          node.fatal "Could not find child #{after_child._id}"
        end

        after_child = node
        node = node.parent
      end
    end

    # This is a generic tree iterator. It yields before it descends into the
    # child node and after (which is identical to before the next child
    # descend). It yields the node, the position and the stack of parent
    # nodes.
    # @yield [node, position, stack]
    def traverse
      # We use a non-recursive implementation to traverse the tree. This stack
      # keeps track of all the known still to be checked nodes.
      stack = [ [ self, 0 ] ]

      while !stack.empty?
        node, position = stack.pop

        # Call the payload method. The position marks where we are in the node
        # with respect to the traversal. 0 means we've just entered the node
        # for the first time and are about to descent to the first child.
        # Position 1 is after the 1st child has been processed and before the
        # 2nd child is being processed. If we have N children, the last
        # position is N after we have processed the last child and are about
        # to return to the parent node.
        yield(node, position, stack)

        if position <= node.size
          # Push the next position for this node onto the stack.
          stack.push([ node, position + 1 ])

          if !node.is_leaf? && node.children[position]
            # If we have a child node for this position, push the linked node
            # and the starting position onto the stack.
            stack.push([ node.children[position], 0 ])
          end
        end
      end
    end

    # Gather some statistics about the node and all sub nodes.
    # @param stats [Stats] Data structure that stores the gathered data
    def statistics(stats)
      traverse do |node, position, stack|
        if position == 0
          if node.is_leaf?
            stats.leaf_nodes += 1
            depth = stack.size + 1
            if stats.min_depth.nil? || stats.min_depth < depth
              stats.min_depth = depth
            end
            if stats.max_depth.nil? || stats.max_depth > depth
              stats.max_depth = depth
            end
          else
            stats.branch_nodes += 1
          end
        end
      end
    end

    # Return the decoration that marks the tree structure of this node for the
    # inspection method.
    def tree_prefix
      node = self
      str = ''

      while node
        is_last_child = false
        if node.parent
          is_last_child = node.parent.children.last == node
        else
          # Don't add lines for the top-level.
          break
        end

        str = (is_last_child ? '   ' : '  |') + str
        node = node.parent
      end

      str
    end

    # Branch node decoration for the inspection method.
    def tree_branch_mark
      return '' unless @parent
      '-'
    end

    # Text for the node line for the inspection method.
    def tree_summary
      s = " @#{@_id}"
      if @parent
        begin
          s += " +#{@parent.offsets[index_in_parent_node]} ^#{@parent._id}"
        rescue
          s += ' ^@'
        end
      end
      if @prev_sibling
        begin
          s += " <#{@prev_sibling._id}"
        rescue
          s += ' <@'
        end
      end
      if @next_sibling
        begin
          s += " >#{@next_sibling._id}"
        rescue
          s += ' >@'
        end
      end

      s
    end

    # Print and log an error message for the node.
    def error(msg)
      msg = "Error in BigArray node @#{@_id}: #{msg}\n" + @tree.to_s
      $stderr.puts msg
      PEROBS.log.error msg
    end

    # Print and log an error message for the node.
    def fatal(msg)
      msg = "Fatal error in BigArray node @#{@_id}: #{msg}\n" + @tree.to_s
      $stderr.puts msg
      PEROBS.log.fatal msg
    end

    private

    def min_size
      @tree.node_size / 2
    end

    # Move first element of successor to end of child node
    # @param child_index [Integer] index of the child
    def move_first_element_of_successor_to_child(child_index)
      child = @children[child_index]
      succ = @children[child_index + 1]

      if child.is_leaf?
        # Adjust offset for the successor node
        @offsets[child_index + 1] += 1
        # Move the value
        child.values << succ.values.shift
      else
        # Before:
        #
        # Root Node             +--------------------------------+
        # Offsets               | 0                            7 |
        # Children                |                            |
        #              child      v    succ                    v
        # Level 1    +---------------++-------------------------------------+
        # Offsets    | 0         4   ||    0         4          6         9 |
        # Children     |         |         |         |          |         |
        #              v         v         v         v          v         v
        # Leaves  +---------++-------++----------++-------++----------++-------+
        # Values  | A B C D || E F G || H I J K  || L  M  || N  O  P  || Q  R  |
        #
        # Index     0 1 2 3    4 5 6    7 8 9 10    11 12    13 14 15    16 17
        #
        # After:
        #
        # Root Node             +--------------------------------+
        # Offsets               | 0                           11 |
        # Children                |                            |
        #              child      v                succ        v
        # Level 1    +--------------------------++--------------------------+
        # Offsets    | 0         4         7    ||   0          2         5 |
        # Children     |         |         |         |          |         |
        #              v         v         v         v          v         v
        # Leaves  +---------++-------++----------++-------++----------++-------+
        # Values  | A B C D || E F G || H I J K  || L  M  || N  O  P  || Q  R  |
        #
        # Index     0 1 2 3    4 5 6    7 8 9 10    11 12    13 14 15    16 17
        #
        # Adjust the offsets of the successor. The 2nd original offset
        # determines the delta for the parent node.
        succ.offsets.shift
        delta = succ.offsets.first
        succ.offsets.map! { |o| o -= delta }
        # The additional child offset can be taken from the parent node
        # reference.
        child.offsets << @offsets[child_index + 1]
        # The parent node offset of the successor needs to be corrected by the
        # delta value.
        @offsets[child_index + 1] += delta
        # Move the child reference
        child.children << succ.children.shift
        child.children.last.parent = child
      end
    end

    # Move last element of predecessor node to child
    # @param child_index [Integer] index of the child
    def move_last_element_of_predecessor_to_child(child_index)
      pred = @children[child_index - 1]
      child = @children[child_index]

      if child.is_leaf?
        # Adjust offset for the predecessor node
        @offsets[child_index] -= 1
        # Move the value
        child.values.unshift(pred.values.pop)
      else
        # Before:
        #
        # Root Node             +--------------------------------+
        # Offsets               | 0                           13 |
        # Children                |                            |
        #              pred       v                      child v
        # Level 1    +---------------------------------++-------------------+
        # Offsets    | 0         4         7        11 ||       0         3 |
        # Children     |         |         |         |          |         |
        #              v         v         v         v          v         v
        # Leaves  +---------++-------++----------++-------++----------++-------+
        # Values  | A B C D || E F G || H I J K  || L  M  || N  O  P  || Q  R  |
        #
        # Index     0 1 2 3    4 5 6    7 8 9 10    11 12    13 14 15    16 17
        #
        # After:
        #
        # Root Node             +--------------------------------+
        # Offsets               | 0                           11 |
        # Children                |                            |
        #              prepd      v                child       v
        # Level 1    +--------------------------++--------------------------+
        # Offsets    | 0         4         7    ||   0          2         5 |
        # Children     |         |         |         |          |         |
        #              v         v         v         v          v         v
        # Leaves  +---------++-------++----------++-------++----------++-------+
        # Values  | A B C D || E F G || H I J K  || L  M  || N  O  P  || Q  R  |
        #
        # Index     0 1 2 3    4 5 6    7 8 9 10    11 12    13 14 15    16 17
        #
        # Remove the last predecessor offset and update the child offset with
        # it
        delta = @offsets[child_index] - pred.offsets.last
        @offsets[child_index] = pred.offsets.pop
        # Adjust all the offsets of the child
        child.offsets.map! { |o| o += delta }
        # And prepend the 0 offset
        child.offsets.unshift(0)
        # Move the child reference
        child.children.unshift(pred.children.pop)
        child.children.first.parent = child
      end
    end

    def merge_child_with_next(child_index)
      c1 = @children[child_index]
      c2 = @children[child_index + 1]

      if c1.is_leaf?
        # Update the sibling links
        c1.next_sibling = c2.next_sibling
        c1.next_sibling.prev_sibling = c1 if c1.next_sibling

        c1.values += c2.values
        # Adjust the last_leaf reference in the @tree if c1 is now the last
        # sibling.
        @tree.last_leaf = c1 unless c1.next_sibling
      else
        # Before:
        #
        # Root Node             +---------------------+
        # Offsets               | 0                11 |
        # Children                |                 |
        #              c1         v              c2 v
        # Level 1    +--------------------------++-----+
        # Offsets    | 0         4         7    ||   0 |
        # Children     |         |         |         |
        #              v         v         v         v
        # Leaves  +---------++-------++----------++-------+
        # Values  | A B C D || E F G || H I J K  || L  M  |
        #
        # Index     0 1 2 3    4 5 6    7 8 9 10    11 12
        #
        # After:
        #
        # Root Node             +---+
        # Offsets               | 0 |
        # Children                |
        #             c1          v
        # Level 1    +---------------------------------+
        # Offsets    | 0         4         7        11 |
        # Children     |         |         |         |
        #              v         v         v         v
        # Leaves  +---------++-------++----------++-------+
        # Values  | A B C D || E F G || H I J K  || L  M  |
        #
        # Index     0 1 2 3    4 5 6    7 8 9 10    11 12
        delta = @offsets[child_index + 1] - @offsets[child_index]
        c1.offsets += c2.offsets.map { |o| o += delta }
        c2.children.each { |c| c.parent = c1 }
        c1.children += c2.children
      end

      # Remove the child successor from the node.
      @offsets.delete_at(child_index + 1)
      @children.delete_at(child_index + 1)

      if @parent && size < min_size
        @parent.consolidate_child_nodes(self)
      end
    end

  end

end

