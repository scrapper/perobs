# encoding: UTF-8
#
# = BigTreeNode.rb -- Persistent Ruby Object Store
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
require 'perobs/Array'

module PEROBS

  # The BigTreeNode class provides the BTree nodes for the BigTree objects.
  # A node can either be a branch node or a leaf node. Branch nodes don't
  # store values, only references to child nodes. Leaf nodes don't have child
  # nodes but store the actual values. All nodes store a list of keys that are
  # used to naviate the tree and find the values. A key is either directly
  # associated with a value or determines the lower key boundary for the
  # following child node.
  class BigTreeNode < PEROBS::Object

    attr_persist :tree, :parent, :keys, :values, :children,
      :prev_sibling, :next_sibling

    # Internal constructor. Use Store.new(BigTreeNode, ...) instead.
    # @param p [Handle]
    # @param tree [BigTree] The tree this node should belong to
    # @param is_leaf [Boolean] True if a leaf node should be created, false
    #        for a branch node.
    # @param parent [BigTreeNode] Parent node
    # @param prev_sibling [BigTreeNode] Previous sibling
    # @param next_sibling [BigTreeNode] Next sibling
    def initialize(p, tree, is_leaf, parent = nil, prev_sibling = nil,
                   next_sibling = nil)
      super(p)
      self.tree = tree
      self.parent = parent
      self.keys = @store.new(PEROBS::Array)

      if is_leaf
        # Create a new leaf node. It stores values and has no children.
        self.values = @store.new(PEROBS::Array)
        self.children = nil
      else
        # Create a new tree node. It doesn't store values and can have child
        # nodes.
        self.children = @store.new(PEROBS::Array)
        self.values = nil
      end
      # Link the neighboring siblings to the newly inserted node. If the node
      # is a leaf node and has no sibling on a side we also must register it
      # as first or last leaf with the BigTree object.
      if (self.prev_sibling = prev_sibling)
        @prev_sibling.next_sibling = myself
      elsif is_leaf?
        @tree.first_leaf = myself
      end
      if (self.next_sibling = next_sibling)
        @next_sibling.prev_sibling = myself
      elsif is_leaf?
        @tree.last_leaf = myself
      end
    end

    # @return [Boolean] True if this is a leaf node, false otherwise.
    def is_leaf?
      @children.nil?
    end

    # Insert or replace the given value by using the key as unique address.
    # @param key [Integer] Unique key to retrieve the value
    # @param value [Integer] value to insert
    def insert(key, value)
      node = myself

      # Traverse the tree to find the right node to add or replace the value.
      while node do
        # All nodes that we find on the way that are full will be split into
        # two half-full nodes.
        if node.keys.size >= @tree.node_size
          node = node.split_node
        end

        # Once we have reached a leaf node we can insert or replace the value.
        if node.is_leaf?
          return node.insert_element(key, value)
        else
          # Descend into the right child node to add the value to.
          node = node.children[node.search_key_index(key)]
        end
      end

      PEROBS.log.fatal "Could not find proper node to insert into"
    end

    # Return the value that matches the given key or return nil if they key is
    # unknown.
    # @param key [Integer] key to search for
    # @return [Integer or nil] value that matches the key
    def get(key)
      node = self

      while node do
        # Find index of the entry that best fits the key.
        i = node.search_key_index(key)
        if node.is_leaf?
          # This is a leaf node. Check if there is an exact match for the
          # given key and return the corresponding value or nil.
          return node.keys[i] == key ? node.values[i] : nil
        end

        # Descend into the right child node to continue the search.
        node = node.children[i]
      end

      PEROBS.log.fatal "Could not find proper node to get from while " +
        "looking for key #{key}"
    end

    # Return the node chain from the root to the leaf node storing the
    # key/value pair.
    # @param key [Integer] key to search for
    # @return [Array of BigTreeNode] node list (may be empty)
    def node_chain(key)
      node = myself
      list = [ node ]

      while node do
        # Find index of the entry that best fits the key.
        i = node.search_key_index(key)
        if node.is_leaf?
          # This is a leaf node. Check if there is an exact match for the
          # given key and return the corresponding value or nil.
          return node.keys[i] == key ? list : []
        end

        # Add current node to chain.
        list << node
        # Descend into the right child node to continue the search.
        node = node.children[i]
      end

      PEROBS.log.fatal "Could not find node chain for key #{key}"
    end

    # Return if given key is stored in the node.
    # @param key [Integer] key to search for
    # @return [Boolean] True if key was found, false otherwise
    def has_key?(key)
      node = self

      while node do
        # Find index of the entry that best fits the key.
        i = node.search_key_index(key)
        if node.is_leaf?
          # This is a leaf node. Check if there is an exact match for the
          # given key and return the corresponding value or nil.
          return node.keys[i] == key
        end

        # Descend into the right child node to continue the search.
        node = node.children[i]
      end

      PEROBS.log.fatal "Could not find proper node to get from while " +
        "looking for key #{key}"
    end

    # Return the value that matches the given key and remove the value from
    # the tree. Return nil if the key is unknown.
    # @param key [Integer] key to search for
    # @return [Object] value that matches the key
    def remove(key)
      node = self

      while node do
        # Find index of the entry that best fits the key.
        i = node.search_key_index(key)
        if node.is_leaf?
          # This is a leaf node. Check if there is an exact match for the
          # given key and return the corresponding value or nil.
          if node.keys[i] == key
            @tree.entry_counter -= 1
            return node.remove_element(i)
          else
            return nil
          end
        end

        # Descend into the right child node to continue the search.
        node = node.children[i]
      end

      PEROBS.log.fatal 'Could not find proper node to remove from'
    end

    # Iterate over all the key/value pairs in this node and all sub-nodes.
    # @yield [key, value]
    def each
      traverse do |node, position, stack|
        if node.is_leaf? && position < node.keys.size
          yield(node.keys[position], node.values[position])
        end
      end
    end

    # Iterate over all the key/value pairs of the node.
    # @yield [key, value]
    def each_element
      return unless is_leaf?

      0.upto(@keys.length - 1) do |i|
        yield(@keys[i], @values[i])
      end
    end

    # Iterate over all the key/value pairs of the node in reverse order.
    # @yield [key, value]
    def reverse_each_element
      return unless is_leaf?

      (@keys.length - 1).downto(0) do |i|
        yield(@keys[i], @values[i])
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
          if node.parent
            # After a split the nodes will only have half the maximum keys.
            # For branch nodes one of the split nodes will have even 1 key
            # less as this will become the branch key in a parent node.
            if node.keys.size < min_keys - (node.is_leaf? ? 0 : 1)
              node.error "BigTree node #{node._id} has too few keys"
              return false
            end
          end

          if node.keys.size > @tree.node_size
            node.error "BigTree node must not have more then " +
              "#{@tree.node_size} keys, but has #{node.keys.size} keys"
            return false
          end

          last_key = nil
          node.keys.each do |key|
            if last_key && key < last_key
              node.error "Keys are not increasing monotoneously: " +
                "#{node.keys.inspect}"
              return false
            end
            last_key = key
          end

          if node.is_leaf?
            if branch_depth
              unless branch_depth == stack.size
                node.error "All leaf nodes must have same distance from root"
                return false
              end
            else
              branch_depth = stack.size
            end
            if node.prev_sibling.nil? && @tree.first_leaf != node
              node.error "Leaf node #{node._id} has no previous sibling " +
                "but is not the first leaf of the tree"
              return false
            end
            if node.next_sibling.nil? && @tree.last_leaf != node
              node.error "Leaf node #{node._id} has no next sibling " +
                "but is not the last leaf of the tree"
            end
            unless node.keys.size == node.values.size
              node.error "Key count (#{node.keys.size}) and value " +
                "count (#{node.values.size}) don't match"
                return false
            end
            if node.children
              node.error "children must be nil for a leaf node"
              return false
            end
          else
            if node.values
              node.error "values must be nil for a branch node"
              return false
            end
            unless node.children.size == node.keys.size + 1
              node.error "Key count (#{node.keys.size}) must be one " +
                "less than children count (#{node.children.size})"
                return false
            end
            node.children.each_with_index do |child, i|
              unless child.is_a?(BigTreeNode)
                node.error "Child #{i} is of class #{child.class} " +
                  "instead of BigTreeNode"
                return false
              end
              unless child.parent.is_a?(BigTreeNode)
                node.error "Parent reference of child #{i} is of class " +
                  "#{child.class} instead of BigTreeNode"
                return false
              end
              if child == node
                node.error "Child #{i} point to self"
                return false
              end
              if stack.include?(child)
                node.error "Child #{i} points to ancester node"
                return false
              end
              unless child.parent == node
                node.error "Child #{i} does not have parent pointing " +
                  "to this node"
                return false
              end
              if i > 0
                unless node.children[i - 1].next_sibling == child
                  node.error "next_sibling of node " +
                    "#{node.children[i - 1]._id} " +
                    "must point to node #{child._id}"
                  return false
                end
              end
              if i < node.children.length - 1
                unless child == node.children[i + 1].prev_sibling
                  node.error "prev_sibling of node " +
                    "#{node.children[i + 1]._id} " +
                    "must point to node #{child._id}"
                  return false
                end
              end
            end
          end
        elsif position <= node.keys.size
          # These checks are done after we have completed the respective child
          # node with index 'position - 1'.
          index = position - 1
          if node.is_leaf?
            if block_given?
              # If a block was given, call this block with the key and value.
              return false unless yield(node.keys[index], node.values[index])
            end
          else
            unless node.children[index].keys.last < node.keys[index]
              node.error "Child #{node.children[index]._id} " +
                "has too large key #{node.children[index].keys.last}. " +
                "Must be smaller than #{node.keys[index]}."
              return false
            end
            unless node.children[position].keys.first >= node.keys[index]
              node.error "Child #{node.children[position]._id} " +
                "has too small key #{node.children[position].keys.first}. " +
                "Must be larger than or equal to #{node.keys[index]}."
              return false
            end
          end
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
              "#{node.keys.first.nil? ? '--' : 'v-'}#{node.tree_summary}\n"
          rescue => e
            str += "@@@@@@@@@@: #{e.message}\n"
          end
        else
          begin
            if node.is_leaf?
              if node.keys[position - 1]
                str += "#{node.tree_prefix}  |" +
                  "[#{node.keys[position - 1]}, " +
                  "#{node.values[position - 1]}]\n"
              end
            else
              if node.keys[position - 1]
                str += "#{node.tree_prefix}  #{node.keys[position - 1]}\n"
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
    # @return [BigTreeNode] common parent of the two nodes
    def split_node
      unless @parent
        # The node is the root node. We need to create a parent node first.
        self.parent = @store.new(BigTreeNode, @tree, false)
        @parent.children[0] = myself
        @tree.root = @parent
      end

      # Create the new sibling that will take the 2nd half of the
      # node content.
      sibling = @store.new(BigTreeNode, @tree, is_leaf?, @parent, myself,
                           @next_sibling)
      # Determine the index of the middle element that gets moved to the
      # parent. The node size must be an uneven number.
      mid = @keys.size / 2
      # Insert the middle element key into the parent node
      @parent.insert_element(@keys[mid], sibling)
      if is_leaf?
        # Copy the keys and values from the mid element onwards into the new
        # sibling node.
        sibling.keys += @keys[mid..-1]
        sibling.values += @values[mid..-1]
        # Delete the copied keys and values from this node.
        @values.slice!(mid..-1)
      else
        # Copy the keys from after the mid value onwards to the new sibling
        # node.
        sibling.keys += @keys[mid + 1..-1]
        # Same for the children.
        sibling.children += @children[mid + 1..-1]
        # Reparent the children to the new sibling parent.
        sibling.children.each { |c| c.parent = sibling }
        # And delete the copied children references.
        @children.slice!(mid + 1..-1)
      end
      # Delete the copied keys from this node.
      @keys.slice!(mid..-1)

      @parent
    end

    # Insert the given value or child into the current node using the key as
    # index.
    # @param key [Integer] key to address the value or child
    # @param child_or_value [Integer or BigTreeNode] value or BigTreeNode
    # @return [Boolean] true if new element, false if override existing
    #         element
    def insert_element(key, child_or_value)
      if @keys.size >= @tree.node_size
        PEROBS.log.fatal "Cannot insert into a full BigTreeNode: #{@keys.size}"
      end

      i = search_key_index(key)
      if @keys[i] == key
        # Overwrite existing entries
        @keys[i] = key
        if is_leaf?
          @values[i] = child_or_value
        else
          @children[i + 1] = child_or_value
        end
      else
        # Create a new entry
        @keys.insert(i, key)
        if is_leaf?
          @values.insert(i, child_or_value)
          @tree.entry_counter += 1
        else
          @children.insert(i + 1, child_or_value)
        end
      end
    end

    # Remove the element from a leaf node at the given index.
    # @param index [Integer] The index of the entry to be removed
    # @return [Object] The removed value
    def remove_element(index)
      # Delete the key at the specified index.
      unless (key = @keys.delete_at(index))
        PEROBS.log.fatal "Could not remove element #{index} from BigTreeNode " +
          "@#{@_id}"
      end
      update_branch_key(key) if index == 0

      # Delete the corresponding value.
      removed_value = @values.delete_at(index)
      if @keys.length < min_keys
        if @prev_sibling && @prev_sibling.parent == @parent
          borrow_from_previous_sibling(@prev_sibling) ||
            @prev_sibling.merge_with_leaf_node(myself)
        elsif @next_sibling && @next_sibling.parent == @parent
          borrow_from_next_sibling(@next_sibling) ||
            merge_with_leaf_node(@next_sibling)
        elsif @parent
          PEROBS.log.fatal "Cannot not find adjecent leaf siblings"
        end
      end

      # The merge has potentially invalidated this node. After this method has
      # been called this copy of the node should no longer be used.
      removed_value
    end

    # Remove the specified node from this branch node.
    # @param node [BigTreeNode] The child to remove
    def remove_child(node)
      unless (index = search_node_index(node))
        PEROBS.log.fatal "Cannot remove child #{node._id} from node #{@_id}"
      end

      if index == 0
        # Removing the first child is a bit more complicated as the
        # corresponding branch key is in a parent node.
        key = @keys.shift
        update_branch_key(key)
      else
        # For all other children we can just remove the corresponding key.
        @keys.delete_at(index - 1)
      end

      # Remove the child node link.
      child = @children.delete_at(index)
      # If we remove the first or last leaf node we must update the reference
      # in the BigTree object.
      @tree.first_leaf = child.next_sibling if child == @tree.first_leaf
      @tree.last_leaf = child.prev_sibling if child == @tree.last_leaf
      # Unlink the neighbouring siblings from the child
      child.prev_sibling.next_sibling = child.next_sibling if child.prev_sibling
      child.next_sibling.prev_sibling = child.prev_sibling if child.next_sibling

      if @keys.length < min_keys
        # The node has become too small. Try borrowing a node from an adjecent
        # sibling or merge with an adjecent node.
        if @prev_sibling && @prev_sibling.parent == @parent
          borrow_from_previous_sibling(@prev_sibling) ||
            @prev_sibling.merge_with_branch_node(myself)
        elsif @next_sibling && @next_sibling.parent == @parent
          borrow_from_next_sibling(@next_sibling) ||
            merge_with_branch_node(@next_sibling)
        end
      end

      if @parent.nil? && @children.length <= 1
        # If the node just below the root only has one child it will become
        # the new root node.
        new_root = @children.first
        new_root.parent = nil
        @tree.root = new_root
      end
    end

    def merge_with_leaf_node(node)
      if @keys.length + node.keys.length > @tree.node_size
        PEROBS.log.fatal "Leaf nodes are too big to merge"
      end

      self.keys += node.keys
      self.values += node.values

      node.parent.remove_child(node)
    end

    def merge_with_branch_node(node)
      if @keys.length + 1 + node.keys.length > @tree.node_size
        PEROBS.log.fatal "Branch nodes are too big to merge"
      end

      index = @parent.search_node_index(node) - 1
      self.keys << @parent.keys[index]
      self.keys += node.keys
      node.children.each { |c| c.parent = myself }
      self.children += node.children

      node.parent.remove_child(node)
    end

    # Search the keys of the node that fits the given key. The result is
    # either the index of an exact match or the index of the position where
    # the given key would have to be inserted.
    # @param key [Integer] key to search for
    # @return [Integer] Index of the matching key or the insert position.
    def search_key_index(key)
      # Handle special case for empty keys list.
      return 0 if @keys.empty?

      # Keys are unique and always sorted. Use a binary search to find the
      # index that fits the given key.
      li = pi = 0
      ui = @keys.size - 1
      while li <= ui
        # The pivot element is always in the middle between the lower and upper
        # index.
        pi = li + (ui - li) / 2

        if key < @keys[pi]
          # The pivot element is smaller than the key. Set the upper index to
          # the pivot index.
          ui = pi - 1
        elsif key > @keys[pi]
          # The pivot element is larger than the key. Set the lower index to
          # the pivot index.
          li = pi + 1
        else
          # We've found an exact match. For leaf nodes return the found index.
          # For branch nodes we have to add one to the index since the larger
          # child is the right one.
          return is_leaf? ? pi : pi + 1
        end
      end
      # No exact match was found. For the insert operaton we need to return
      # the index of the first key that is larger than the given key.
      @keys[pi] < key ? pi + 1 : pi
    end

    def search_node_index(node)
      index = search_key_index(node.keys.first)
      unless @children[index] == node
        raise RuntimeError, "Child at index #{index} is not the requested node"
      end

      index
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

        if position <= node.keys.size
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
          s += " ^#{@parent._id}"
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
      msg = "Error in BigTree node @#{@_id}: #{msg}\n" + @tree.to_s
      $stderr.puts msg
      PEROBS.log.error msg
    end

    private

    def min_keys
      @tree.node_size / 2
    end

    # Try to borrow an element from the preceding sibling.
    # @return [True or False] True if an element was borrowed, false
    #         otherwise.
    def borrow_from_previous_sibling(prev_node)
      if prev_node.keys.length - 1 > min_keys
        index = @parent.search_node_index(self) - 1

        if is_leaf?
          # Move the last key of the previous node to the front of this node
          @keys.unshift(prev_node.keys.pop)
          # Register the new lead key of this node with its parent
          @parent.keys[index] = @keys.first
          # Move the last value of the previous node to the front of this node
          @values.unshift(prev_node.values.pop)
        else
          # For branch nodes the branch key will be the borrowed key.
          @keys.unshift(@parent.keys[index])
          # And the last key of the previous key will become the new branch
          # key for this node.
          @parent.keys[index] = prev_node.keys.pop
          # Move the last child of the previous node to the front of this node
          @children.unshift(node = prev_node.children.pop)
          node.parent = myself
        end

        return true
      end

      false
    end

    # Try to borrow an element from the next sibling.
    # @return [True or False] True if an element was borrowed, false
    #         otherwise.
    def borrow_from_next_sibling(next_node)
      if next_node.keys.length - 1 > min_keys
        # The next sibling now has a new lead key that requires the branch key
        # to be updated in the parent node.
        index = next_node.parent.search_node_index(next_node) - 1

        if is_leaf?
          # Move the first key of the next node to the end of the this node
          self.keys << next_node.keys.shift
          # Register the new lead key of next_node with its parent
          next_node.parent.keys[index] = next_node.keys.first
          # Move the first value of the next node to the end of this node
          self.values << next_node.values.shift
        else
          # For branch nodes we need to get the lead key from the parent of
          # next_node.
          self.keys << next_node.parent.keys[index]
          # The old lead key of next_node becomes the branch key in the parent
          # of next_node. And the keys of next_node are shifted.
          next_node.parent.keys[index] = next_node.keys.shift
          # Move the first child of the next node to the end of this node
          self.children << (node = next_node.children.shift)
          node.parent = myself
        end

        return true
      end

      false
    end

    def update_branch_key(old_key)
      new_key = @keys.first
      return unless (node = @parent)

      while node
        if (index = node.keys.index(old_key))
          node.keys[index] = new_key
          return
        end
        node = node.parent
      end

      # The smallest element has no branch key.
    end

  end

end

