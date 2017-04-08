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
    # @param parent [SpaceTreeNode] Parent node in the tree
    # @param node_address [Integer] Address of the node in the file
    # @param blob_address [Integer] Address of the free space blob
    # @param size [Integer] Size of the free space blob
    def initialize(tree, parent = nil, node_address = nil, blob_address = 0,
                   size = 0)
      @tree = tree
      if blob_address < 0
        PEROBS.log.fatal "Node address (#{node_address}) must be larger than 0"
      end
      @blob_address = blob_address
      @size = size
      # The root node is always at address 0. Since it's never referenced from
      # another node, 0 means nil pointer.
      @smaller = @equal = @larger = nil
      @node_address = node_address

      unless node_address.nil? || node_address.is_a?(Integer)
        PEROBS.log.fatal "node_address is not Integer: #{node_address.class}"
      end

      if node_address
        # This must be an existing node. Try to read it and fill the instance
        # variables.
        if size != 0
          PEROBS.log.fatal "If node_address is not nil size must be 0"
        end
        if blob_address != 0
          PEROBS.log.fatal "If node_address is not nil blob_address must be 0"
        end
        unless read_node
          PEROBS.log.fatal "SpaceTree node at address #{node_address} " +
            "does not exist"
        end
      else
        # This is a new node. Make sure the data is written to the file.
        @node_address = @tree.nodes.free_address
        self.parent = parent
      end
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
                          @tree.new_node(node, address, size))
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
                          @tree.new_node(node, address, size))
            break
          end
        else
          # Same size as current node. Insert new node as equal child at top of
          # equal list.
          new_node = @tree.new_node(node, address, size)
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
        if size < node.size && node.smaller
          node = node.smaller
        elsif size > node.size && node.larger
          node = node.larger
        elsif size == node.size && node.equal
          node = node.equal
        elsif node.blob_address == address
          return true
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
      write_node
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

        case mode
        when :on_enter
          yield(node, mode, stack)
          stack.push([ node, :smaller ])
        when :smaller
          yield(node, mode, stack) if node.check_node_link('smaller', stack)
          stack.push([ node, :equal ])
          stack.push([ node.smaller, :on_enter]) if node.smaller
        when :equal
          yield(node, mode, stack) if node.check_node_link('equal', stack)
          stack.push([ node, :larger ])
          stack.push([ node.equal, :on_enter]) if node.equal
        when :larger
          yield(node, mode, stack) if node.check_node_link('larger', stack)
          stack.push([ node, :on_exit])
          stack.push([ node.larger, :on_enter]) if node.larger
        when :on_exit
          yield(node, mode, stack)
        end
      end
    end

    def delete_node
      node = nil

      if @equal
        # Pull-up equal node by copying it's content into the current node.
        node = @equal
        set_link('@equal', node.equal)
      elsif @smaller && @larger.nil?
        # The node only has a single sub-node on the smaller branch. Pull-up
        # smaller node and replace it with the current node by copying the
        # content of the sub-node.
        node = @smaller
        set_link('@smaller', node.smaller)
        set_link('@equal', node.equal)
        set_link('@larger', node.larger)
      elsif @larger && @smaller.nil?
        # The node only has a single sub-node on the larger branch. Pull-up
        # larger node and replace it with the current node.
        node = @larger
        set_link('@smaller', node.smaller)
        set_link('@equal', node.equal)
        set_link('@larger', node.larger)
      elsif @smaller && @larger
        # We'll replace the current node with the largest node of the
        # smaller sub-tree by copying the values into this node.
        node = @smaller.find_largest_node
        if node.smaller
          # The largest node of the @smaller sub-tree has a smaller branch.
          if (smallest_node = node.find_smallest_node) != node &&
             node != @smaller
            # Find the smallest node in that branch and attach the old
            # @smaller branch to its smaller link.
            smallest_node.set_link('@smaller', @smaller)
          end
          set_link('@smaller', node.smaller)
        end
        set_link('@equal', node.equal)
        if node.parent == self
          set_link('@smaller', node.smaller)
        else
          node.parent.unlink_node(node) if node.parent
        end
      end

      if node
        # We have found a node that can replace the node we want to delete.
        # Copy the data from that node into this node.
        set_size_and_address(node.size, node.blob_address)

        # Delete the just copied node from the file.
        @tree.delete_node(node.node_address)
      else
        # The node is a leaf node. We have to delete this node by removing the
        # link from the parent and deleting it from the file.
        if @parent
          @parent.unlink_node(self)
          @tree.delete_node(@node_address)
        else
          # The root node can't be deleted. We just set @size to 0 to mark it
          # as empty.
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
      write_node
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
      write_node
    end

    def parent=(p)
      @parent = p ? SpaceTreeNodeLink.new(@tree, p) : nil
      write_node
    end
    # Collects address and size touples of all nodes in the tree with a
    # depth-first strategy and stores them in an Array.
    # @return [Array] Array with [ address, size ] touples.
    def to_a
      ary = []

      each do |node, mode, stack|
        if mode == :on_enter
          ary << [ node.blob_address, node.size ] unless node.size == 0
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
            smaller_node = node.smaller
            if smaller_node.size >= node.size
              PEROBS.log.error "Smaller SpaceTreeNode size " +
                "(#{smaller_node}) is not smaller than #{node}"
              return false
            end
          end
        when :equal
          if node.equal
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
            PEROBS.log.error "SpaceTreeNode has space that isn't " +
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

    private

    def write_node
      bytes = [ @blob_address, @size,
                @parent ? @parent.node_address : 0,
                @smaller ? @smaller.node_address : 0,
                @equal ? @equal.node_address : 0,
                @larger ? @larger.node_address : 0].pack(NODE_BYTES_FORMAT)
      @tree.nodes.store_blob(@node_address, bytes)
    end

    def read_node
      return false unless (bytes = @tree.nodes.retrieve_blob(@node_address))

      @blob_address, @size, parent_node_address,
        smaller_node_address, equal_node_address,
        larger_node_address = bytes.unpack(NODE_BYTES_FORMAT)
      # The parent address can also be 0 as the parent can rightly point back
      # to the root node which always has the address 0.
      @parent = @node_address != 0 ?
        SpaceTreeNodeLink.new(@tree, parent_node_address) : nil
      @smaller = smaller_node_address != 0 ?
        SpaceTreeNodeLink.new(@tree, smaller_node_address) : nil
      @equal = equal_node_address != 0 ?
        SpaceTreeNodeLink.new(@tree, equal_node_address) : nil
      @larger = larger_node_address != 0 ?
        SpaceTreeNodeLink.new(@tree, larger_node_address) : nil

      true
    end

  end

end
