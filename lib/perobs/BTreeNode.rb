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

    attr_reader :node_address, :parent, :is_leaf, :keys, :values, :children

    # Create a new BTreeNode object for the given tree with the given parent
    # or recreate the node with the given node_address from the backing store.
    # If node_address is nil a new node will be created. If not, node_address
    # must be an existing address that can be found in the backing store to
    # restore the node.
    # @param tree [BTree] The tree this node is part of
    # @param parent [BTreeNode] reference to parent node
    # @param node_address [Integer] the address of the node to read from the
    #        backing store
    # @param is_leaf [Boolean] true if the node should be a leaf node, false
    #        if not
    def initialize(tree, node_address = nil, parent = nil, is_leaf = true,
                   keys = [], values = [], children = [])
      @tree = tree
      if node_address == 0
        PEROBS.log.fatal "Node address may not be 0"
      end
      @node_address = node_address
      @parent = parent ? BTreeNodeLink.new(tree, parent) : nil
      @keys = keys
      if (@is_leaf = is_leaf)
        @values = values
        @children = []
      else
        @children = children
        @values = []
      end

      ObjectSpace.define_finalizer(
        self, BTreeNode._finalize(@tree, @node_address, object_id))
      @tree.node_cache.insert(self, false)
    end

    # This method generates the destructor for the objects of this class. It
    # is done this way to prevent the Proc object hanging on to a reference to
    # self which would prevent the object from being collected. This internal
    # method is not intended for users to call.
    def BTreeNode::_finalize(tree, node_address, ruby_object_id)
      proc { tree.node_cache._collect(node_address, ruby_object_id) }
    end

    # Create a new SpaceTreeNode. This method should be used for the creation
    # of new nodes instead of calling the constructor directly.
    # @param tree [BTree] The tree the new node should belong to
    # @param parent [BTreeNode] The parent node
    # @param is_leaf [Boolean] True if the node has no children, false
    #        otherwise
    def BTreeNode::create(tree, parent = nil, is_leaf = true)
      unless parent.nil? || parent.is_a?(BTreeNode) ||
             parent.is_a?(BTreeNodeLink)
        PEROBS.log.fatal "Parent node must be a BTreeNode but is of class " +
          "#{parent.class}"
      end

      address = tree.nodes.free_address
      node = BTreeNode.new(tree, address, parent, is_leaf)
      # This is a new node. Make sure the data is written to the file.
      tree.node_cache.insert(node)

      node
    end

    # Restore a node from the backing store at the given address and tree.
    # @param tree [BTree] The tree the node belongs to
    # @param address [Integer] The address in the blob file.
    def BTreeNode::load(tree, address)
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
      # Read the keys
      keys = ary[4, key_count]

      children = nil
      values = nil
      if is_leaf
        # Read the values
        values = ary[4 + tree.order, data_count]
      else
        # Read the child addresses
        children = []
        data_count.times do |i|
          child_address = ary[4 + tree.order + i]
          unless child_address > 0
            PEROBS.log.fatal "Child address must be larger than 0"
          end
          children << BTreeNodeLink.new(tree, child_address)
        end
      end

      node = BTreeNode.new(tree, address, parent, is_leaf, keys, values,
                           children)
      tree.node_cache.insert(node, false)

      node
    end

    # @return [String] The format used for String.pack.
    def BTreeNode::node_bytes_format(tree)
      # This does not include the 4 bytes for the CRC32 checksum
      "CSSQQ#{tree.order}Q#{tree.order + 1}"
    end

    # @return [Integer] The number of bytes needed to store a node.
    def BTreeNode::node_bytes(order)
      1 + # is_leaf
      2 + # actual key count
      2 + # actual value or children count (aka data count)
      8 + # parent address
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
          node.insert_element(key, value)
          return
        else
          # Descend into the right child node to add the value to.
          node = node.children[node.search_key_index(key)]
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
        self.parent = BTreeNode::create(@tree, nil, false)
        @parent.set_child(0, self)
        @tree.set_root(@parent)
      end

      # Create the new sibling that will take the 2nd half of the
      # node content.
      sibling = BTreeNode::create(@tree, @parent, @is_leaf)
      # Determine the index of the middle element that gets moved to the
      # parent. The order must be an uneven number, so adding 1 will get us
      # the middle element.
      mid = @tree.order / 2 + 1
      # Insert the middle element key into the parent node
      @parent.insert_element(@keys[mid], sibling)
      copy_elements(mid + (@is_leaf ? 0 : 1), sibling)
      trim(mid)

      @parent
    end

    def merge_node(upper_sibling, parent_index)
      if upper_sibling == self
        PEROBS.log.fatal "Cannot merge node @#{@node_address} with self"
      end
      unless upper_sibling.is_leaf
        insert_element(@parent.keys[parent_index], upper_sibling.children[0])
      end
      upper_sibling.copy_elements(0, self, @keys.size, upper_sibling.keys.size)
      @tree.delete_node(upper_sibling.node_address)

      @parent.remove_element(parent_index)
    end

    # Insert the given value or child into the current node using the key as
    # index.
    # @param key [Integer] key to address the value or child
    # @param value_or_child [Integer or BTreeNode] value or BTreeNode
    #        reference
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
          @children[i + 1] = BTreeNodeLink.new(@tree, value_or_child)
        end
      else
        # Create a new entry
        @keys.insert(i, key)
        if is_leaf
          @values.insert(i, value_or_child)
        else
          @children.insert(i + 1, BTreeNodeLink.new(@tree, value_or_child))
        end
      end
      @tree.node_cache.insert(self)
    end

    # Remove the element at the given index.
    def remove_element(index)
      # We need this key to find the link in the parent node.
      first_key = @keys[0]
      removed_value = nil

      # Delete the key at the specified index.
      unless @keys.delete_at(index)
        PEROBS.log.fatal "Could not remove element #{index} from BTreeNode " +
          "@#{@node_address}"
      end
      if @is_leaf
        # For leaf nodes, also delete the corresponding value.
        removed_value = @values.delete_at(index)
      else
        # The corresponding child has can be found at 1 index higher.
        @children.delete_at(index + 1)
      end
      @tree.node_cache.insert(self)

      # Find the lower and upper siblings and the index of the key for this
      # node in the parent node.
      lower_sibling, upper_sibling, parent_index =
        find_closest_siblings(first_key)

      if lower_sibling &&
         lower_sibling.keys.size + @keys.size < @tree.order
        lower_sibling.merge_node(self, parent_index - 1)
      elsif upper_sibling &&
            @keys.size + upper_sibling.keys.size < @tree.order
        merge_node(upper_sibling, parent_index)
      end

      # The merge has potentially invalidated this node. After this method has
      # been called this copy of the node should no longer be used.
      removed_value
    end

    def copy_elements(src_idx, dest_node, dst_idx = 0, count = nil)
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
      @parent = p ? BTreeNodeLink.new(@tree, p) : nil
      @tree.node_cache.insert(self)
    end

    def set_child(index, child)
      if child
        @children[index] = BTreeNodeLink.new(@tree, child)
        @children[index].parent = self
      else
        @children[index] = nil
      end
      @tree.node_cache.insert(self)
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
          return @is_leaf ? pi : pi + 1
        end
      end
      # No exact match was found. For the insert operaton we need to return
      # the index of the first key that is larger than the given key.
      @keys[pi] < key ? pi + 1 : pi
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
    # @return [Boolean] true if tree has no errors
    def check
      traverse do |node, position, stack|
        if position == 0
          if node.parent && node.keys.size < 1
            node.error "BTreeNode must have at least one entry"
            return false
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
              return false
            end
          end

          if node.is_leaf
            unless node.keys.size == node.values.size
              node.error "Key count (#{node.keys.size}) and value " +
                "count (#{node.values.size}) don't match"
                return false
            end
          else
            unless node.keys.size == node.children.size - 1
              node.error "Key count (#{node.keys.size}) must be one " +
                "less than children count (#{node.children.size})"
                return false
            end
            node.children.each_with_index do |child, i|
              unless child.is_a?(BTreeNodeLink)
                node.error "Child #{i} is of class #{child.class} " +
                  "instead of BTreeNodeLink"
                return false
              end
              unless child.parent.is_a?(BTreeNodeLink)
                node.error "Parent reference of child #{i} is of class " +
                  "#{child.class} instead of BTreeNodeLink"
                return false
              end
              if child.node_address == node.node_address
                node.error "Child #{i} points to self"
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
          if !node.is_leaf
            unless node.children[index].keys.last < node.keys[index]
              node.error "Child #{node.children[index].node_address} " +
                "has too large key #{node.children[index].keys.last}. " +
                "Must be smaller than #{node.keys[index]}."
              return false
            end
            unless node.children[position].keys.first >=
                   node.keys[index]
              node.error "Child #{node.children[position].node_address} " +
                "has too small key #{node.children[position].keys.first}. " +
                "Must be larger than or equal to #{node.keys[index]}."
              return false
            end
          else
            if block_given?
              # If a block was given, call this block with the key and value.
              return false unless yield(node.keys[index], node.values[index])
            end
          end
        end
      end

      true
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

      s
    end

    def error(msg)
      PEROBS.log.error "Error in BTreeNode @#{@node_address}: #{msg}\n" +
        @tree.to_s
    end

    def write_node
      ary = [
        @is_leaf ? 1 : 0,
        @keys.size,
        @is_leaf ? @values.size : @children.size,
        @parent ? @parent.node_address : 0
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

