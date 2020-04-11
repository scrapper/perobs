# encoding: UTF-8
#
# = BTreeNode.rb -- Persistent Ruby Object Store
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

require 'zlib'

require 'perobs/BTree'
require 'perobs/BTreeNodeLink'

module PEROBS

  # The BTreeNode class manages more or less standard BTree nodes. All nodes
  # contain BTree.order number of keys. Leaf node contain BTree.order number
  # of values and no child references. Branch nodes only contain BTree.order +
  # 1 number of child references but no values. The is_leaf flag is used to
  # mark a node as leaf or branch node.
  class BTreeNode

    attr_reader :node_address, :parent, :is_leaf, :next_sibling, :prev_sibling,
      :keys, :values, :children

    # Create a new BTreeNode object for the given tree with the given parent
    # or recreate the node with the given node_address from the backing store.
    # If node_address is nil a new node will be created. If not, node_address
    # must be an existing address that can be found in the backing store to
    # restore the node.
    # @param tree [BTree] The tree this node is part of
    # @param parent [BTreeNode] reference to parent node
    # @param prev_sibling [BTreeNode] reference to previous sibling node
    # @param next_sibling [BTreeNode] reference to next sibling node
    # @param node_address [Integer] the address of the node to read from the
    #        backing store
    # @param is_leaf [Boolean] true if the node should be a leaf node, false
    #        if not
    def initialize(tree, node_address = nil, parent = nil, is_leaf = true,
                   prev_sibling = nil, next_sibling = nil,
                   keys = [], values = [], children = [])
      @tree = tree
      if node_address == 0
        PEROBS.log.fatal "Node address may not be 0"
      end
      @node_address = node_address
      @parent = link(parent)
      @prev_sibling = link(prev_sibling)
      @next_sibling = link(next_sibling)
      @keys = keys
      if (@is_leaf = is_leaf)
        @values = values
        @children = []
      else
        @children = children
        @values = []
      end
    end

    # Create a new SpaceTreeNode. This method should be used for the creation
    # of new nodes instead of calling the constructor directly.
    # @param tree [BTree] The tree the new node should belong to
    # @param parent [BTreeNode] The parent node
    # @param is_leaf [Boolean] True if the node has no children, false
    #        otherwise
    # @param prev_sibling [BTreeNode] reference to previous sibling node
    # @param next_sibling [BTreeNode] reference to next sibling node
    def BTreeNode::create(tree, parent = nil, is_leaf = true,
                          prev_sibling = nil, next_sibling = nil)
      unless parent.nil? || parent.is_a?(BTreeNode) ||
             parent.is_a?(BTreeNodeLink)
        PEROBS.log.fatal "Parent node must be a BTreeNode but is of class " +
          "#{parent.class}"
      end

      address = tree.nodes.free_address
      node = BTreeNode.new(tree, address, parent, is_leaf, prev_sibling,
                           next_sibling)
      # This is a new node. Make sure the data is written to the file.
      tree.node_cache.insert(node)

      # Insert the newly created node into the existing node chain.
      if (node.prev_sibling = prev_sibling)
        node.prev_sibling.next_sibling = BTreeNodeLink.new(tree, node)
      end
      if (node.next_sibling = next_sibling)
        node.next_sibling.prev_sibling = BTreeNodeLink.new(tree, node)
      end

      BTreeNodeLink.new(tree, node)
    end

    # Restore a node from the backing store at the given address and tree.
    # @param tree [BTree] The tree the node belongs to
    # @param address [Integer] The address in the blob file.
    def BTreeNode::load(tree, address, unused = nil)
      unless address.is_a?(Integer)
        PEROBS.log.fatal "address is not Integer: #{address.class}"
      end

      unless (bytes = tree.nodes.retrieve_blob(address))
        PEROBS.log.fatal "SpaceTree node at address #{address} " +
          "does not exist"
      end

      unless Zlib::crc32(bytes) != 0
        PEROBS.log.fatal "Checksum failure in BTreeNode entry @#{address}"
      end
      ary = bytes.unpack(BTreeNode::node_bytes_format(tree))
      # Read is_leaf
      if ary[0] != 0 && ary[0] != 1
        PEROBS.log.fatal "First byte of a BTreeNode entry must be 0 or 1"
      end
      is_leaf = ary[0] == 0 ? false : true
      # This is the number of keys this node has.
      key_count = ary[1]
      data_count = ary[2]
      # Read the parent node address
      parent = ary[3] == 0 ? nil : BTreeNodeLink.new(tree, ary[3])
      prev_sibling = ary[4] == 0 ? nil : BTreeNodeLink.new(tree, ary[4])
      next_sibling = ary[5] == 0 ? nil : BTreeNodeLink.new(tree, ary[5])
      # Read the keys
      keys = ary[6, key_count]

      children = nil
      values = nil
      if is_leaf
        # Read the values
        values = ary[6 + tree.order, data_count]
      else
        # Read the child addresses
        children = []
        data_count.times do |i|
          child_address = ary[6 + tree.order + i]
          unless child_address > 0
            PEROBS.log.fatal "Child address must be larger than 0"
          end
          children << BTreeNodeLink.new(tree, child_address)
        end
      end

      node = BTreeNode.new(tree, address, parent, is_leaf,
                           prev_sibling, next_sibling, keys, values,
                           children)
      tree.node_cache.insert(node, false)

      node
    end

    # This is a wrapper around BTreeNode::load() that returns a BTreeNodeLink
    # instead of the actual node.
    # @param tree [BTree] The tree the node belongs to
    # @param address [Integer] The address in the blob file.
    # @return [BTreeNodeLink] Link to loaded noded
    def BTreeNode::load_and_link(tree, address)
      BTreeNodeLink.new(tree, BTreeNode::load(tree, address))
    end


    # @return [String] The format used for String.pack.
    def BTreeNode::node_bytes_format(tree)
      # This does not include the 4 bytes for the CRC32 checksum
      "CSSQQQQ#{tree.order}Q#{tree.order + 1}"
    end

    # @return [Integer] The number of bytes needed to store a node.
    def BTreeNode::node_bytes(order)
      1 + # is_leaf
      2 + # actual key count
      2 + # actual value or children count (aka data count)
      8 + # parent address
      8 + # previous sibling address
      8 + # next sibling address
      8 * order + # keys
      8 * (order + 1) + # values or child addresses
      4 # CRC32 checksum
    end

    # Save the node into the blob file.
    def save
      write_node
    end

    # The node address uniquely identifies a BTreeNode.
    def uid
      @node_address
    end

    # Insert or replace the given value by using the key as unique address.
    # @param key [Integer] Unique key to retrieve the value
    # @param value [Integer] value to insert
    def insert(key, value)
      node = self

      # Traverse the tree to find the right node to add or replace the value.
      while node do
        # All nodes that we find on the way that are full will be split into
        # two half-full nodes.
        if node.keys.size >= @tree.order
          node = node.split_node
        end

        # Once we have reached a leaf node we can insert or replace the value.
        if node.is_leaf
          return node.insert_element(key, value)
        else
          # Descend into the right child node to add the value to.
          node = node.children[node.search_key_index(key)]
          node = node.get_node if node
        end
      end

      PEROBS.log.fatal 'Could not find proper node to add to'
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
        if node.is_leaf
          # This is a leaf node. Check if there is an exact match for the
          # given key and return the corresponding value or nil.
          return node.keys[i] == key ? node.values[i] : nil
        end

        # Descend into the right child node to continue the search.
        node = node.children[i]
        node = node.get_node if node
      end

      PEROBS.log.fatal "Could not find proper node to get from while " +
        "looking for key #{key}"
    end

    # Return the key/value pair that matches the given key or the next larger
    # key/value pair with a key that is at least as large as key +
    # min_miss_increment.
    # @param key [Integer] key to search for
    # @param min_miss_increment [Integer] minimum required key increment in
    #        case an exact key match could not be found
    # @return [Integer or nil] value that matches the key
    def get_best_match(key, min_miss_increment)
      node = self

      while node do
        # Find index of the entry that best fits the key.
        i = node.search_key_index(key)
        if node.is_leaf
          # This is a leaf node. Check if there is an exact match for the
          # given key.
          if node.keys[i] == key
            # Return the corresponding value/value pair.
            return [ key, node.values[i] ]
          else
            # No exact key match. Now search the larger keys for the first
            # that is at least key + min_miss_increment large.
            keys = node.keys
            keys_length = keys.length
            while node
              if (i += 1) >= keys_length
                # We've reached the end of a node. Continue search in next
                # sibling.
                return nil unless (node = node.next_sibling)
                node = node.get_node
                keys = node.keys
                keys_length = keys.length
                i = -1
              elsif keys[i] >= key + min_miss_increment
                # We've found a key that fits the critera. Return the
                # corresponding key/value pair.
                return [ keys[i], node.values[i] ]
              end
            end

            return nil
          end
        end

        # Descend into the right child node to continue the search.
        node = node.children[i]
        node = node.get_node if node
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
        if node.is_leaf
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
        node = node.get_node if node
      end

      PEROBS.log.fatal 'Could not find proper node to remove from'
    end

    # Split the current node into two nodes. The upper half of the elements
    # will be moved into a newly created node. This node will retain the lower
    # half.
    # @return [BTreeNodeLink] common parent of the two nodes
    def split_node
      unless @parent
        # The node is the root node. We need to create a parent node first.
        self.parent = link(BTreeNode::create(@tree, nil, false))
        @parent.set_child(0, self)
        @tree.set_root(@parent)
      end

      # Create the new sibling that will take the 2nd half of the
      # node content.
      sibling = BTreeNode::create(@tree, @parent, @is_leaf, link(self),
                                  @next_sibling)
      # Determine the index of the middle element that gets moved to the
      # parent. The order must be an uneven number, so adding 1 will get us
      # the middle element.
      mid = @tree.order / 2
      # Insert the middle element key into the parent node
      @parent.insert_element(@keys[mid], sibling)
      copy_elements(mid + (@is_leaf ? 0 : 1), sibling)
      trim(mid)

      @parent
    end

    # Insert the given value or child into the current node using the key as
    # index.
    # @param key [Integer] key to address the value or child
    # @param value_or_child [Integer or BTreeNode] value or BTreeNode
    #        reference
    # @return true for insert, false for overwrite
    def insert_element(key, value_or_child)
      if @keys.size >= @tree.order
        PEROBS.log.fatal "Cannot insert into a full BTreeNode"
      end

      i = search_key_index(key)
      if @keys[i] == key
        # Overwrite existing entries
        @keys[i] = key
        if is_leaf
          @values[i] = value_or_child
        else
          @children[i + 1] = link(value_or_child)
        end
        @tree.node_cache.insert(self)

        return false
      else
        # Create a new entry
        @keys.insert(i, key)
        if is_leaf
          @values.insert(i, value_or_child)
        else
          @children.insert(i + 1, link(value_or_child))
        end
        @tree.node_cache.insert(self)

        return true
      end
    end

    # Remove the element at the given index.
    def remove_element(index)
      # Delete the key at the specified index.
      unless (key = @keys.delete_at(index))
        PEROBS.log.fatal "Could not remove element #{index} from BigTreeNode " +
          "@#{@node_address}"
      end
      update_branch_key(key) if index == 0

      # Delete the corresponding value.
      removed_value = @values.delete_at(index)
      @tree.node_cache.insert(self)

      if @keys.length < min_keys
        if @prev_sibling && @prev_sibling.parent == @parent
          borrow_from_previous_sibling(@prev_sibling) ||
            @prev_sibling.merge_with_leaf_node(self)
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

    def remove_child(node)
      unless (index = search_node_index(node))
        PEROBS.log.fatal "Cannot remove child #{node.node_address} " +
          "from node #{@node_address}"
      end

      @tree.node_cache.insert(self)
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
      # Unlink the neighbouring siblings from the child
      child.prev_sibling.next_sibling = child.next_sibling if child.prev_sibling
      child.next_sibling.prev_sibling = child.prev_sibling if child.next_sibling

      if @keys.length < min_keys
        # The node has become too small. Try borrowing a node from an adjecent
        # sibling or merge with an adjecent node.
        if @prev_sibling && @prev_sibling.parent == @parent
          borrow_from_previous_sibling(@prev_sibling) ||
            @prev_sibling.merge_with_branch_node(self)
        elsif @next_sibling && @next_sibling.parent == @parent
          borrow_from_next_sibling(@next_sibling) ||
            merge_with_branch_node(@next_sibling)
        end
      end

      # Delete the node from the cache and backing store.
      @tree.delete_node(node.node_address)
    end

    def merge_with_leaf_node(node)
      if @keys.length + node.keys.length > @tree.order
        PEROBS.log.fatal "Leaf nodes are too big to merge"
      end

      @keys += node.keys
      @values += node.values
      @tree.node_cache.insert(self)

      node.parent.remove_child(node)
    end

    def merge_with_branch_node(node)
      if @keys.length + 1 + node.keys.length > @tree.order
        PEROBS.log.fatal "Branch nodes are too big to merge"
      end

      index = @parent.search_node_index(node) - 1
      @keys << @parent.keys[index]
      @keys += node.keys
      node.children.each { |c| c.parent = link(self) }
      @children += node.children
      @tree.node_cache.insert(self)

      node.parent.remove_child(node)
    end

    def search_node_index(node)
      index = search_key_index(node.keys.first)
      unless @children[index] == node
        raise RuntimeError, "Child at index #{index} is not the requested node"
      end

      index
    end

    def copy_elements(src_idx, dest_node, dst_idx = 0, count = nil)
      dest_node = dest_node.get_node
      unless count
        count = @tree.order - src_idx
      end
      if dst_idx + count > @tree.order
        PEROBS.log.fatal "Destination too small for copy operation"
      end
      if dest_node.is_leaf != @is_leaf
        PEROBS.log.fatal "Source #{@is_leaf} and destination " +
          "#{dest_node.is_leaf} node must be of same kind"
      end

      dest_node.keys[dst_idx, count] = @keys[src_idx, count]
      if @is_leaf
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
      @tree.node_cache.insert(dest_node)
    end

    def parent=(p)
      @parent = p
      @tree.node_cache.insert(self)

      p
    end

    def prev_sibling=(node)
      @prev_sibling = node
      if node.nil? && @is_leaf
        # If this node is a leaf node without a previous sibling we need to
        # register it as the first leaf node.
        @tree.set_first_leaf(BTreeNodeLink.new(@tree, self))
      end

      @tree.node_cache.insert(self)

      node
    end

    def next_sibling=(node)
      @next_sibling = node
      @tree.node_cache.insert(self)
      if node.nil? && @is_leaf
        # If this node is a leaf node without a next sibling we need to
        # register it as the last leaf node.
        @tree.set_last_leaf(BTreeNodeLink.new(@tree, self))
      end

      node
    end

    def set_child(index, child)
      if child
        @children[index] = link(child)
        @children[index].parent = link(self)
      else
        @children[index] = nil
      end
      @tree.node_cache.insert(self)

      child
    end

    def trim(idx)
      @keys = @keys[0..idx - 1]
      if @is_leaf
        @values = @values[0..idx - 1]
      else
        @children = @children[0..idx]
      end
      @tree.node_cache.insert(self)
    end

    # Search the keys of the node that fits the given key. The result is
    # either the index of an exact match or the index of the position where
    # the given key would have to be inserted.
    # @param key [Integer] key to search for
    # @return [Integer] Index of the matching key or the insert position.
    def search_key_index(key)
      (@is_leaf ? @keys.bsearch_index { |x| x >= key } :
                  @keys.bsearch_index { |x| x > key }) || @keys.length
    end

    # Iterate over all the key/value pairs in this node and all sub-nodes.
    # @yield [key, value]
    def each
      traverse do |node, position, stack|
        if node.is_leaf && position < node.keys.size
          yield(node.keys[position], node.values[position])
        end
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

        if position <= @tree.order
          # Push the next position for this node onto the stack.
          stack.push([ node, position + 1 ])

          if !node.is_leaf && node.children[position]
            # If we have a child node for this position, push the linked node
            # and the starting position onto the stack.
            stack.push([ node.children[position], 0 ])
          end
        end
      end
    end

    # Check consistency of the node and all subsequent nodes. In case an error
    # is found, a message is logged and false is returned.
    # @yield [key, value]
    # @return [nil or Integer] nil in case of errors or the number of nodes
    def check
      branch_depth = nil
      nodes_count = 0

      traverse do |node, position, stack|
        if position == 0
          nodes_count += 1
          if node.parent
            # After a split the nodes will only have half the maximum keys.
            # For branch nodes one of the split nodes will have even 1 key
            # less as this will become the branch key in a parent node.
            if node.keys.size < min_keys - (node.is_leaf ? 0 : 1)
              node.error "BTreeNode #{node.node_address} has too few keys"
              return nil
            end
          end

          if node.keys.size > @tree.order
            node.error "BTreeNode must not have more then #{@tree.order} " +
              "keys, but has #{node.keys.size} keys"
          end

          last_key = nil
          node.keys.each do |key|
            if last_key && key < last_key
              node.error "Keys are not increasing monotoneously: " +
                "#{node.keys.inspect}"
              return nil
            end
            last_key = key
          end

          if node.is_leaf
            if branch_depth
              unless branch_depth == node.tree_level
                node.error "All leaf nodes must have same distance from root "
                return nil
              end
            else
              branch_depth = node.tree_level
            end
            if node.prev_sibling.nil? && @tree.first_leaf != node
              node.error "Leaf node #{node.node_address} has no previous " +
                "sibling but is not the first leaf of the tree"
              return nil
            end
            if node.next_sibling.nil? && @tree.last_leaf != node
              node.error "Leaf node #{node.node_address} has no next sibling " +
                "but is not the last leaf of the tree"
              return nil
            end
            unless node.keys.size == node.values.size
              node.error "Key count (#{node.keys.size}) and value " +
                "count (#{node.values.size}) don't match"
                return nil
            end
            unless node.children.empty?
              node.error "@children must be nil for a leaf node"
              return nil
            end
          else
            unless node.values.empty?
              node.error "@values must be nil for a branch node"
              return nil
            end
            unless node.children.size == node.keys.size + 1
              node.error "Key count (#{node.keys.size}) must be one " +
                "less than children count (#{node.children.size})"
                return nil
            end
            node.children.each_with_index do |child, i|
              unless child.is_a?(BTreeNodeLink)
                node.error "Child #{i} is of class #{child.class} " +
                  "instead of BTreeNodeLink"
                return nil
              end
              unless child.parent.is_a?(BTreeNodeLink)
                node.error "Parent reference of child #{i} is of class " +
                  "#{child.parent.class} instead of BTreeNodeLink"
                return nil
              end
              if child == node
                node.error "Child #{i} points to self"
                return nil
              end
              if stack.include?(child)
                node.error "Child #{i} points to ancester node"
                return nil
              end
              unless child.parent == node
                node.error "Child #{i} does not have parent pointing " +
                  "to this node"
                return nil
              end
              if i > 0
                unless node.children[i - 1].next_sibling == child
                  node.error "next_sibling of node " +
                    "#{node.children[i - 1].node_address} " +
                    "must point to node #{child.node_address}"
                  return nil
                end
              end
              if i < node.children.length - 1
                unless child == node.children[i + 1].prev_sibling
                  node.error "prev_sibling of node " +
                    "#{node.children[i + 1].node_address} " +
                    "must point to node #{child.node_address}"
                  return nil
                end
              end
            end
          end
        elsif position <= node.keys.size
          # These checks are done after we have completed the respective child
          # node with index 'position - 1'.
          index = position - 1
          if !node.is_leaf
            unless node.children[index].keys.last < node.keys[index]
              node.error "Child #{node.children[index].node_address} " +
                "has too large key #{node.children[index].keys.last}. " +
                "Must be smaller than #{node.keys[index]}."
              return nil
            end
            unless node.children[position].keys.first >= node.keys[index]
              node.error "Child #{node.children[position].node_address} " +
                "has too small key #{node.children[position].keys.first}. " +
                "Must be larger than or equal to #{node.keys[index]}."
              return nil
            end
          else
            if block_given?
              # If a block was given, call this block with the key and value.
              return nil unless yield(node.keys[index], node.values[index])
            end
          end
        end
      end

      nodes_count
    end

    def is_top?
      @parent.nil? || @parent.parent.nil? || @parent.parent.parent.nil?
    end

    def to_s
      str = ''

      traverse do |node, position, stack|
        if position == 0
          begin
            str += "#{node.parent ? node.parent.tree_prefix + '  +' : 'o'}" +
              "#{node.tree_branch_mark}-" +
              "#{node.keys.first.nil? ? '--' : 'v-'}#{node.tree_summary}\n"
          rescue
            str += "@@@@@@@@@@\n"
          end
        else
          begin
            if node.is_leaf
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
          rescue
            str += "@@@@@@@@@@\n"
          end
        end
      end

      str
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
        node = node.get_node if node
      end

      str
    end

    def tree_branch_mark
      return '' unless @parent
      '-'
    end

    def tree_summary
      s = " @#{@node_address}"
      if @parent
        begin
          s += " ^#{@parent.node_address}"
        rescue
          s += ' ^@'
        end
      end
      if @prev_sibling
        begin
          s += " <#{@prev_sibling.node_address}"
        rescue
          s += ' <@'
        end
      end
      if @next_sibling
        begin
          s += " >#{@next_sibling.node_address}"
        rescue
          s += ' >@'
        end
      end

      s
    end

    def tree_level
      level = 1
      node = self
      while (node = node.parent)
        level += 1
      end

      level
    end


    def error(msg)
      PEROBS.log.error "Error in BTreeNode @#{@node_address}: #{msg}"
    end

    def write_node
      ary = [
        @is_leaf ? 1 : 0,
        @keys.size,
        @is_leaf ? @values.size : @children.size,
        @parent ? @parent.node_address : 0,
        @prev_sibling ? @prev_sibling.node_address : 0,
        @next_sibling ? @next_sibling.node_address : 0
      ] + @keys + ::Array.new(@tree.order - @keys.size, 0)

      if @is_leaf
        ary += @values + ::Array.new(@tree.order + 1 - @values.size, 0)
      else
        if @children.size != @keys.size + 1
          PEROBS.log.fatal "write_node: Children count #{@children.size} " +
            "is not #{@keys.size + 1}"
        end
        @children.each do |child|
          PEROBS.log.fatal "write_node: Child must not be nil" unless child
        end
        ary += @children.map{ |c| c.node_address } +
          ::Array.new(@tree.order + 1 - @children.size, 0)
      end
      bytes = ary.pack(BTreeNode::node_bytes_format(@tree))
      bytes += [ Zlib::crc32(bytes) ].pack('L')
      @tree.nodes.store_blob(@node_address, bytes)
    end

    private

    def min_keys
      @tree.order / 2
    end

    def link(node)
      return nil if node.nil?

      if node.is_a?(BTreeNodeLink)
        return node
      elsif node.is_a?(BTreeNode) || node.is_a?(Integer)
        return BTreeNodeLink.new(@tree, node)
      else
        PEROBS.log.fatal "Node link must be a BTreeNode, not a #{node.class}"
      end
    end

    # Try to borrow an element from the preceding sibling.
    # @return [True or False] True if an element was borrowed, false
    #         otherwise.
    def borrow_from_previous_sibling(prev_node)
      if prev_node.keys.length - 1 > min_keys
        index = @parent.search_node_index(self) - 1

        @tree.node_cache.insert(self)
        @tree.node_cache.insert(prev_node.get_node)
        @tree.node_cache.insert(@parent.get_node)
        if @is_leaf
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
          node.parent = link(self)
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

        @tree.node_cache.insert(self)
        @tree.node_cache.insert(next_node.get_node)
        @tree.node_cache.insert(next_node.parent.get_node)
        if @is_leaf
          # Move the first key of the next node to the end of the this node
          @keys << next_node.keys.shift
          # Register the new lead key of next_node with its parent
          next_node.parent.keys[index] = next_node.keys.first
          # Move the first value of the next node to the end of this node
          @values << next_node.values.shift
        else
          # For branch nodes we need to get the lead key from the parent of
          # next_node.
          @keys << next_node.parent.keys[index]
          # The old lead key of next_node becomes the branch key in the parent
          # of next_node. And the keys of next_node are shifted.
          next_node.parent.keys[index] = next_node.keys.shift
          # Move the first child of the next node to the end of this node
          @children << (node = next_node.children.shift)
          node.parent = link(self)
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
          @tree.node_cache.insert(node.get_node)
          return
        end
        node = node.parent
      end

      # The smallest element has no branch key.
    end

  end

end

