# encoding: UTF-8
#
# = SpaceTreeNode.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'
require 'perobs/FlatFileBlobHeader'
require 'perobs/FlatFile'
require 'perobs/SpaceTreeNodeLink'

module PEROBS

  # The SpaceTree keeps a complete list of all empty spaces in the FlatFile.
  # Spaces are stored with size and address. The Tree is Tenerary Tree. The
  # nodes can link to other nodes with smaller spaces, same spaces and bigger
  # spaces.
  class SpaceTreeNode

    attr_accessor :size, :blob_address
    attr_reader :node_address, :parent, :smaller, :equal, :larger

    # Each node can hold a reference to the parent, a lower, equal or larger
    # size node and the actual value and the address in the FlatFile. Each of
    # these entries is 8 bytes long.
    NODE_BYTES = 6 * 8
    # The pack/unpack format.
    NODE_BYTES_FORMAT = 'Q6'

    # Create a new SpaceTreeNode object. If node_address is not nil, the data
    # will be read from the SpaceTree file at the given node_address.
    # @param tree [SpaceTree] Tree that the object should belong to
    # @param node_address [Integer] Address of the node in the file
    # @param blob_address [Integer] Address of the free space blob
    # @param size [Integer] Size of the free space blob
    # @param parent [SpaceTreeNode] Parent node in the tree
    # @param smaller [SpaceTreeNode] smaller node in the tree
    # @param equal [SpaceTreeNode] equal node in the tree
    # @param larger [SpaceTreeNode] larger node in the tree
    def initialize(tree, node_address, blob_address = 0, size = 0,
                   parent = nil, smaller = nil, equal = nil, larger = nil)
      @tree = tree
      if node_address <= 0
        PEROBS.log.fatal "Node address (#{node_address}) must be larger than 0"
      end
      @node_address = node_address
      if blob_address < 0
        PEROBS.log.fatal "Blob address (#{node_address}) must be larger than 0"
      end
      @blob_address = blob_address
      @size = size
      @parent = parent
      @smaller = smaller
      @equal = equal
      @larger = larger

      ObjectSpace.define_finalizer(
        self, SpaceTreeNode._finalize(@tree, @node_address, object_id))
      @tree.cache.insert(self, false)
    end

    # This method generates the destructor for the objects of this class. It
    # is done this way to prevent the Proc object hanging on to a reference to
    # self which would prevent the object from being collected. This internal
    # method is not intended for users to call.
    def SpaceTreeNode._finalize(tree, node_address, ruby_object_id)
      proc { tree.cache._collect(node_address, ruby_object_id) }
    end

    # Create a new SpaceTreeNode. This method should be used for the creation
    # of new nodes instead of calling the constructor directly.
    # @param tree [SpaceTree] The tree the node should belong to
    # @param node_address [Integer] Address of the node in the file
    # @param blob_address [Integer] Address of the free space blob
    # @param size [Integer] Size of the free space blob
    # @param parent [SpaceTreeNode] Parent node in the tree
    def SpaceTreeNode::create(tree, blob_address = 0, size = 0, parent = nil)
      node_address = tree.nodes.free_address

      node = SpaceTreeNode.new(tree, node_address, blob_address, size, parent)
      node.save

      node
    end

    # Restore a node from the backing store at the given address and tree.
    # @param tree [SpaceTree] The tree the node belongs to
    # @param node_address [Integer] The address in the file.
    def SpaceTreeNode::load(tree, node_address)
      unless node_address > 0
        PEROBS.log.fatal "node_address (#{node_address}) must be larger than 0"
      end
      unless (bytes = tree.nodes.retrieve_blob(node_address))
        PEROBS.log.fatal "SpaceTreeNode at address #{node_address} does " +
          "not exist"
      end

      blob_address, size, parent_node_address,
        smaller_node_address, equal_node_address,
        larger_node_address = bytes.unpack(NODE_BYTES_FORMAT)

      parent = parent_node_address != 0 ?
        SpaceTreeNodeLink.new(tree, parent_node_address) : nil
      smaller = smaller_node_address != 0 ?
        SpaceTreeNodeLink.new(tree, smaller_node_address) : nil
      equal = equal_node_address != 0 ?
        SpaceTreeNodeLink.new(tree, equal_node_address) : nil
      larger = larger_node_address != 0 ?
        SpaceTreeNodeLink.new(tree, larger_node_address) : nil

      node = SpaceTreeNode.new(tree, node_address, blob_address, size,
                               parent, smaller, equal, larger)

      node
    end

    # Save the node into the blob file.
    def save
      bytes = [ @blob_address, @size,
                @parent ? @parent.node_address : 0,
                @smaller ? @smaller.node_address : 0,
                @equal ? @equal.node_address : 0,
                @larger ? @larger.node_address : 0].pack(NODE_BYTES_FORMAT)
      @tree.nodes.store_blob(@node_address, bytes)
    end

    # Add a new node for the given address and size to the tree.
    # @param address [Integer] address of the free space
    # @param size [Integer] size of the free space
    def add_space(address, size)
      node = self

      loop do
        if node.size == 0
          # This happens only for the root node if the tree is empty.
          node.set_size_and_address(size, address)
          break
        elsif size < node.size
          # The new size is smaller than this node.
          if node.smaller
            # There is already a smaller node, so pass it on.
            node = node.smaller
          else
            # There is no smaller node yet, so we create a new one as a
            # smaller child of the current node.
            node.set_link('@smaller',
                          SpaceTreeNode::create(@tree, address, size, node))
            break
          end
        elsif size > node.size
          # The new size is larger than this node.
          if node.larger
            # There is already a larger node, so pass it on.
            node = node.larger
          else
            # There is no larger node yet, so we create a new one as a larger
            # child of the current node.
            node.set_link('@larger',
                          SpaceTreeNode::create(@tree, address, size, node))
            break
          end
        else
          # Same size as current node. Insert new node as equal child at top of
          # equal list.
          new_node = SpaceTreeNode::create(@tree, address, size, node)
          new_node.set_link('@equal', node.equal)

          node.set_link('@equal', new_node)

          break
        end
      end
    end

    # Check if this node or any sub-node has an entry for the given address
    # and size.
    # @param address [Integer] address of the free space
    # @param size [Integer] size of the free space
    # @return [Boolean] True if found, otherwise false
    def has_space?(address, size)
      node = self
      loop do
        if node.blob_address == address
          return size == node.size
        elsif size < node.size && node.smaller
          node = node.smaller
        elsif size > node.size && node.larger
          node = node.larger
        elsif size == node.size && node.equal
          node = node.equal
        else
          return false
        end
      end
    end

    # Return an address/size touple that matches exactly the requested size.
    # Return nil if nothing was found.
    # @param size [Integer] size of the free space
    # @return [Array or nil] address, size touple or nil
    def find_matching_space(size)
      node = self

      loop do
        if node.size < size
          if node.larger
            # The current space is not yet large enough. If we have a larger sub
            # node check that one next.
            node = node.larger
          else
            break
          end
        elsif node.size == size
          # We've found a space that is an exact match. Remove it from the
          # list and return it.
          address = node.blob_address
          node.delete_node
          return [ address, size ]
        else
          break
        end
      end

      return nil
    end

    # Return an address/size touple that matches the requested size or is
    # larger than the requested size plus the overhead for another blob.
    # Return nil if nothing was found.
    # @param size [Integer] size of the free space
    # @return [Array or nil] address, size touple or nil
    def find_equal_or_larger_space(size)
      node = self

      loop do
        if node.size < size
          if node.larger
            # The current space is not yet large enough. If we have a larger sub
            # node check that one next.
            node = node.larger
          else
            break
          end
        elsif node.size == size ||
              node.size >= size * 2 + FlatFileBlobHeader::LENGTH
          # We've found a space that is either a perfect match or is large
          # enough to hold at least one more record. Remove it from the list and
          # return it.
          actual_size = node.size
          address = node.blob_address
          node.delete_node
          return [ address, actual_size ]
        elsif node.smaller
          # The current space is larger than size but not large enough for an
          # additional record. So check if we have a perfect match in the
          # smaller brach if available.
          node = node.smaller
        else
          break
        end
      end

      return nil
    end

    # Remove a smaller/equal/larger link from the current node.
    # @param child_node [SpaceTreeNodeLink] node to remove
    def unlink_node(child_node)
      if @smaller == child_node
        @smaller = nil
      elsif @equal == child_node
        @equal = nil
      elsif @larger == child_node
        @larger = nil
      else
        PEROBS.log.fatal "Cannot unlink unknown child node with address " +
          "#{child_node.node_address} from #{to_s}"
      end
      @tree.cache.insert(self)
    end

    # Depth-first iterator for all nodes. The iterator yields the given block
    # at 5 points for any found node. The mode variable indicates the point.
    # :on_enter Coming from the parent we've entered the node for the first
    #           time
    # :smaller We are about to follow the link to the smaller sub-node
    # :equal We are about to follow the link to the equal sub-node
    # :larger We are about to follow the link to the larger sub-node
    # :on_exit We have completed this node
    def each
      # We use a non-recursive implementation to traverse the tree. This stack
      # keeps track of all the known still to be checked nodes.
      stack = [ [ self, :on_enter ] ]

      while !stack.empty?
        node, mode = stack.pop

        # Empty trees only have a dummy node that has no parent, and a size
        # and address of 0.
        break if node.size == 0 && node.blob_address == 0 && node.parent.nil?

        case mode
        when :on_enter
          yield(node, mode, stack)
          stack.push([ node, :smaller ])
        when :smaller
          yield(node, mode, stack) if node.smaller
          stack.push([ node, :equal ])
          stack.push([ node.smaller, :on_enter]) if node.smaller
        when :equal
          yield(node, mode, stack) if node.equal
          stack.push([ node, :larger ])
          stack.push([ node.equal, :on_enter]) if node.equal
        when :larger
          yield(node, mode, stack) if node.larger
          stack.push([ node, :on_exit])
          stack.push([ node.larger, :on_enter]) if node.larger
        when :on_exit
          yield(node, mode, stack)
        end
      end
    end

    def delete_node
      if @equal
        # Replace the current node with the next @equal node.
        @equal.set_link('@smaller', @smaller) if @smaller
        @equal.set_link('@larger', @larger) if @larger
        relink_parent(@equal)
      elsif @smaller && @larger.nil?
        # We have no @larger node, so we can just replace the current node
        # with the @smaller node.
        relink_parent(@smaller)
      elsif @larger && @smaller.nil?
        # We have no @smaller node, wo we can just replace the current node
        # with the @larger node.
        relink_parent(@larger)
      elsif @smaller && @larger
        # Find the largest node in the smaller sub-node. This node will
        # replace the current node.
        node = @smaller.find_largest_node
        if node != @smaller
          # If the found node is not the direct @smaller node, attach the
          # smaller sub-node of the found node to the parent of the found
          # node.
          node.relink_parent(node.smaller)
          # The @smaller sub node of the current node is attached to the
          # @smaller link of the found node.
          node.set_link('@smaller', @smaller)
        end
        # Attach the @larger sub-node of the current node to the @larger link
        # of the found node.
        node.set_link('@larger', @larger)
        # Point the link in the parent of the current node to the found node.
        relink_parent(node)
      else
        # The node is a leaf node.
        relink_parent(nil)
      end
      @tree.delete_node(@node_address) if @parent
    end

    # Replace the link in the parent node of the current node that points to
    # the current node with the given node.
    # @param node [SpaceTreeNode]
    def relink_parent(node)
      if @parent
        if @parent.smaller == self
          @parent.set_link('@smaller', node)
        elsif @parent.equal == self
          @parent.set_link('@equal', node)
        elsif @parent.larger == self
          @parent.set_link('@larger', node)
        else
          PEROBS.log.fatal "Cannot relink unknown child node with address " +
            "#{node.node_address} from #{parent.to_s}"
        end
      else
        if node
          @tree.set_root(node)
          node.parent = nil
        else
          set_size_and_address(0, 0)
        end
      end
    end

    # Find the node with the smallest size in this sub-tree.
    # @return [SpaceTreeNode]
    def find_smallest_node
      node = self
      loop do
        if node.smaller
          node = node.smaller
        else
          # We've found a 'leaf' node.
          return node
        end
      end
    end

    # Find the node with the largest size in this sub-tree.
    # @return [SpaceTreeNode]
    def find_largest_node
      node = self
      loop do
        if node.larger
          node = node.larger
        else
          # We've found a 'leaf' node.
          return node
        end
      end
    end

    def set_size_and_address(size, address)
      @size = size
      @blob_address = address
      @tree.cache.insert(self)
    end

    def set_link(name, node_or_address)
      if node_or_address
        # Set the link to the given SpaceTreeNode or node address.
        instance_variable_set(name,
                              node = node_or_address.is_a?(SpaceTreeNodeLink) ?
                              node_or_address :
                              SpaceTreeNodeLink.new(@tree, node_or_address))
        # Link the node back to this node via the parent variable.
        node.parent = self
      else
        # Clear the node link.
        instance_variable_set(name, nil)
      end
      @tree.cache.insert(self)
    end

    def parent=(p)
      @parent = p ? SpaceTreeNodeLink.new(@tree, p) : nil
      @tree.cache.insert(self)
    end
    # Compare this node to another node.
    # @return [Boolean] true if node address is identical, false otherwise
    def ==(node)
      node && @node_address == node.node_address
    end

    # Collects address and size touples of all nodes in the tree with a
    # depth-first strategy and stores them in an Array.
    # @return [Array] Array with [ address, size ] touples.
    def to_a
      ary = []

      each do |node, mode, stack|
        if mode == :on_enter
          ary << [ node.blob_address, node.size ]
        end
      end

      ary
    end

    # Textual version of the node data. It has the form
    # node_address:[blob_address, size] ^parent_node_address
    # <smaller_node_address >larger_node_address
    # @return [String]
    def to_s
      s = "#{@node_address}:[#{@blob_address}, #{@size}]"
      if @parent
        begin
          s += " ^#{@parent.node_address}"
        rescue
          s += ' ^@'
        end
      end
      if @smaller
        begin
          s += " <#{@smaller.node_address}"
        rescue
          s += ' <@'
        end
      end
      if @equal
        begin
          s += " =#{@equal.node_address}"
        rescue
          s += ' =@'
        end
      end
      if @larger
        begin
          s += " >#{@larger.node_address}"
        rescue
          s += ' >@'
        end
      end

      s
    end

    # Check this node and all sub nodes for possible structural or logical
    # errors.
    # @param flat_file [FlatFile] If given, check that the space is also
    #        present in the given flat file.
    # @return [false,true] True if OK, false otherwise
    def check(flat_file)
      node_counter = 0
      max_depth = 0

      each do |node, mode, stack|
        max_depth = stack.size if stack.size > max_depth

        case mode
        when :smaller
          if node.smaller
            return false unless node.check_node_link('smaller', stack)
            smaller_node = node.smaller
            if smaller_node.size >= node.size
              PEROBS.log.error "Smaller SpaceTreeNode size " +
                "(#{smaller_node}) is not smaller than #{node}"
              return false
            end
          end
        when :equal
          if node.equal
            return false unless node.check_node_link('equal', stack)
            equal_node = node.equal

            if equal_node.smaller || equal_node.larger
              PEROBS.log.error "Equal node #{equal_node} must not have " +
                "smaller/larger childs"
              return false
            end

            if node.size != equal_node.size
              PEROBS.log.error "Equal SpaceTreeNode size (#{equal_node}) is " +
                "not equal parent node #{node}"
              return false
            end
          end
        when :larger
          if node.larger
            return false unless node.check_node_link('larger', stack)
            larger_node = node.larger
            if larger_node.size <= node.size
              PEROBS.log.error "Larger SpaceTreeNode size " +
                "(#{larger_node}) is not larger than #{node}"
              return false
            end
          end
        when :on_exit
          if flat_file &&
             !flat_file.has_space?(node.blob_address, node.size)
            PEROBS.log.error "SpaceTreeNode has space at offset " +
              "#{node.blob_address} of size #{node.size} that isn't " +
              "available in the FlatFile."
            return false
          end

          node_counter += 1
        end
      end
      PEROBS.log.debug "#{node_counter} SpaceTree nodes checked"
      PEROBS.log.debug "Maximum tree depth is #{max_depth}"

      return true
    end

    # Check the integrity of the given sub-node link and the parent link
    # pointing back to this node.
    # @param link [String] 'smaller', 'equal' or 'larger'
    # @param stack [Array] List of parent nodes [ address, mode ] touples
    # @return [Boolean] true of OK, false otherwise
    def check_node_link(link, stack)
      if (node = instance_variable_get('@' + link))
        # Node links must only be of class SpaceTreeNodeLink
        unless node.nil? || node.is_a?(SpaceTreeNodeLink)
          PEROBS.log.error "Node link #{link} of node #{to_s} " +
            "is of class #{node.class}"
          return false
        end

        # Link must not point back to self.
        if node == self
          PEROBS.log.error "#{link} address of node " +
            "#{node.to_s} points to self #{to_s}"
          return false
        end

        # Link must not point to any of the parent nodes.
        if stack.include?(node)
          PEROBS.log.error "#{link} address of node #{to_s} " +
            "points to parent node #{node}"

            return false
        end

        # Parent link of node must point back to self.
        if node.parent != self
          PEROBS.log.error "@#{link} node #{node.to_s} does not have parent " +
            "link pointing " +
            "to parent node #{to_s}. Pointing at " +
            "#{node.parent.nil? ? 'nil' : node.parent.to_s} instead."

          return false
        end
      end

      true
    end

    # Convert the node and all child nodes into a tree like text form.
    # @return [String]
    def to_tree_s
      str = ''

      each do |node, mode, stack|
        if mode == :on_enter
          begin
            branch_mark = node.parent.nil? ? '' :
              node.parent.smaller == node ? '<' :
              node.parent.equal == node ? '=' :
              node.parent.larger == node ? '>' : '@'

            str += "#{node.text_tree_prefix}#{branch_mark}-" +
              "#{node.smaller || node.equal || node.larger ? 'v-' : '--'}" +
              "#{node.to_s}\n"
          rescue
            str += "#{node.text_tree_prefix}- @@@@@@@@@@\n"
          end
        end
      end

      str
    end

    # The indentation and arch routing for the text tree.
    # @return [String]
    def text_tree_prefix
      if (node = @parent)
        str = '+'
      else
        # Prefix start for root node line
        str = 'o'
      end

      while node
        last_child = false
        if node.parent
          if node.parent.smaller == node
            last_child = node.parent.equal.nil? && node.parent.larger.nil?
          elsif node.parent.equal == node
            last_child = node.parent.larger.nil?
          elsif node.parent.larger == node
            last_child = true
          end
        else
          # Padding for the root node
          str = '  ' + str
          break
        end

        str = (last_child ? '   ' : '|  ') + str
        node = node.parent
      end

      str
    end

  end

end
