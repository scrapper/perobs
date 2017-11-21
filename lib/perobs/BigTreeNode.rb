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

    attr_persist :tree, :parent, :keys, :values, :children

    # Internal constructor. Use Store.new(BigTreeNode, ...) instead.
    # @param p [Handle]
    # @param tree [BigTree] The tree this node should belong to
    # @param is_leaf [Boolean] True if a leaf node should be created, false
    #        for a branch node.
    # @param parent [BigTreeNode] Parent node
    def initialize(p, tree, is_leaf, parent = nil)
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
          node.insert_element(key, value)
          return
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

    # Return the value that matches the given key and remove the value from
    # the tree. Return nil if the key is unknown.
    # @param key [Integer] key to search for
    # @return [Integer or nil] value that matches the key
    def remove(key)
      node = self

      while node do
        # Find index of the entry that best fits the key.
        i = node.search_key_index(key)
        if node.is_leaf?
          # This is a leaf node. Check if there is an exact match for the
          # given key and return the corresponding value or nil.
          if node.keys[i] == key
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

    # Check consistency of the node and all subsequent nodes. In case an error
    # is found, a message is logged and false is returned.
    # @yield [key, value]
    # @return [Boolean] true if tree has no errors
    def check
      traverse do |node, position, stack|
        if position == 0
          if node.parent && node.keys.size < 1
            node.error "BigTree node must have at least one entry"
            return false
          end
          if node.keys.size > @tree.node_size
            node.error "BigTree node must not have more then " +
              "#{@tree.node_size} keys, but has #{node.keys.size} keys"
          end

          last_key = nil
          node.keys.each do |key|
            if last_key && key < last_key
              node.error "Keys are not increasing monotoneously: " +
                "#{node.keys.inspect}"
              return false
            end
          end

          if node.is_leaf?
            unless node.keys.size == node.values.size
              node.error "Key count (#{node.keys.size}) and value " +
                "count (#{node.values.size}) don't match"
                return false
            end
          else
            if node.values
              node.error "values must be nil for a branch node"
              return false
            end
            unless node.keys.size == node.children.size - 1
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
              if stack.include?(child)
                node.error "Child #{i} points to ancester node"
                return false
              end
              unless child.parent == node
                node.error "Child #{i} does not have parent pointing " +
                  "to this node"
                return false
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
    # @return [BTreeNodeLink] common parent of the two nodes
    def split_node
      unless @parent
        # The node is the root node. We need to create a parent node first.
        self.parent = @store.new(BigTreeNode, @tree, false)
        @parent.set_child(0, myself)
        @tree.set_root(@parent)
      end

      # Create the new sibling that will take the 2nd half of the
      # node content.
      sibling = @store.new(BigTreeNode, @tree, is_leaf?, @parent)
      # Determine the index of the middle element that gets moved to the
      # parent. The node size must be an uneven number.
      mid = @keys.size / 2 + 1
      # Insert the middle element key into the parent node
      @parent.insert_element(@keys[mid], sibling)
      copy_elements(mid + (is_leaf? ? 0 : 1), sibling)
      trim(mid)

      @parent
    end

    # Merge the node with its next sibling node.
    # @param upper_sibling [BigTreeNode] The next sibling node
    # @param parent_index [Integer] The index in the parent node
    def merge_node(upper_sibling, parent_index)
      if upper_sibling == self
        PEROBS.log.fatal "Cannot merge node @#{@node_address} with self"
      end
      unless upper_sibling.is_leaf?
        insert_element(@parent.keys[parent_index], upper_sibling.children[0])
      end
      upper_sibling.copy_elements(0, myself, @keys.size,
                                  upper_sibling.keys.size)

      @parent.remove_element(parent_index)
    end

    # Insert the given value or child into the current node using the key as
    # index.
    # @param key [Integer] key to address the value or child
    # @param child_or_value [Integer or BigTreeNode] value or BigTreeNode
    def insert_element(key, child_or_value)
      if @keys.size >= @tree.node_size
        PEROBS.log.fatal "Cannot insert into a full BigTreeNode"
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
        else
          @children.insert(i + 1, child_or_value)
        end
      end
    end

    # Remove the element at the given index.
    # @param index [Integer] The index of the entry to be removed
    # @return [Object] The removed value
    def remove_element(index)
      # We need this key to find the link in the parent node.
      first_key = @keys[0]
      removed_value = nil

      # Delete the key at the specified index.
      unless @keys.delete_at(index)
        PEROBS.log.fatal "Could not remove element #{index} from BTreeNode " +
          "@#{@node_address}"
      end
      if is_leaf?
        # For leaf nodes, also delete the corresponding value.
        removed_value = @values.delete_at(index)
      else
        # The corresponding child has can be found at 1 index higher.
        @children.delete_at(index + 1)
      end

      # Find the lower and upper siblings and the index of the key for this
      # node in the parent node.
      lower_sibling, upper_sibling, parent_index =
        find_closest_siblings(first_key)

      if lower_sibling &&
         lower_sibling.keys.size + @keys.size < @tree.node_size
        lower_sibling.merge_node(myself, parent_index - 1)
      elsif upper_sibling &&
            @keys.size + upper_sibling.keys.size < @tree.node_size
        merge_node(upper_sibling, parent_index)
      end

      # The merge has potentially invalidated this node. After this method has
      # been called this copy of the node should no longer be used.
      removed_value
    end

    # Copy all elements at and after the source index to the given destination
    # node.
    # @param src_idx [Integer] index of the first element to copy
    # @param dest_node [BigTreeNode] destination node
    # @param dst_idx [Integer] index where to store the first node
    # @param count [Integer] number of elements to copy, if nil, all remaining
    #        elements will be copied
    def copy_elements(src_idx, dest_node, dst_idx = 0, count = nil)
      count ||= @keys.size - src_idx

      if dst_idx + count > @tree.node_size
        PEROBS.log.fatal "Destination too small for copy operation"
      end
      if dest_node.is_leaf? != is_leaf?
        PEROBS.log.fatal "Destination node must #{is_leaf? ? 'not ' : ''}" +
          "be a leaf node"
      end

      dest_node.keys[dst_idx, count] = @keys[src_idx, count]
      if is_leaf?
        # For leaves we copy the keys and corresponding values.
        dest_node.values[dst_idx, count] = @values[src_idx, count]
      else
        # For branch nodes we copy all but the first specified key (that
        # one moved up to the parent) and all the children to the right of the
        # moved-up key.
        (count + 1).times do |i|
          dest_node.set_child(dst_idx + i, @children[src_idx + i])
        end
      end
    end

    # Remove all elements from the node at and after the given index.
    # @param idx [Integer]
    def trim(idx)
      @keys.slice!(idx..-1)
      if is_leaf?
        @values.slice!(idx..-1)
      else
        @children.slice!(idx + 1..-1)
      end
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

    # Set the child link at the given index to the given BigTreeNode. It will
    # also ensure that the back link from the child to this node is set
    # correctly.
    # @param index [Integer]
    # @param child [BigTreeNode]
    def set_child(index, child)
      if child
        @children[index] = child
        @children[index].parent = myself
      else
        @children[index] = nil
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

    def tree_branch_mark
      return '' unless @parent
      '-'
    end

    def tree_summary
      s = " @#{@_id}"
      if @parent
        begin
          s += " ^#{@parent._id}"
        rescue
          s += ' ^@'
        end
      end

      s
    end

    def error(msg)
      $stderr.puts msg + "\n" + @tree.to_s
      PEROBS.log.error "Error in BigTree node @#{@_id}: #{msg}\n" +
        @tree.to_s
    end

    private

    def find_closest_siblings(key)
      # The root node has no siblings.
      return [ nil, nil, nil ] unless @parent

      parent_index = @parent.search_key_index(key)
      unless @parent.children[parent_index] == self
        PEROBS.log.fatal "Failed to find self in parent"
      end
      # The child that corresponds to the key at parent_index has an index of
      # parent_index + 1! The lower_sibling has an child index of
      # parent_index and the upper sibling has a child index of parent_index +
      # 2.
      lower_sibling = parent_index < 1 ?
        nil : @parent.children[parent_index - 1]
      upper_sibling = parent_index >= (@parent.children.size - 1) ?
        nil : @parent.children[parent_index + 1]

      [ lower_sibling, upper_sibling, parent_index ]
    end

  end

end

