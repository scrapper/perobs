# encoding: UTF-8
#
# = SpaceTreeNode.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016 by Chris Schlaeger <chris@taskjuggler.org>
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

    attr_accessor :size, :smaller, :equal, :larger, :blob_address
    attr_reader :node_address

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
      @blob_address = blob_address
      @size = size
      # The root node is always at address 0. Since it's never referenced from
      # another node, 0 means nil pointer.
      @smaller = @equal = @larger = 0
      @node_address = node_address

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
        write_node
      end

      @parent = parent ? SpaceTreeNodeLink.new(@tree, parent.node_address) : nil
    end

    # Add a new node for the given address and size to the tree.
    # @param address [Integer] address of the free space
    # @param size [Integer] size of the free space
    def add_space(address, size)
      node = self
      level = 0

      loop do
        level += 1
        if level > 64
          PEROBS.log.fatal "SpaceTreeNode::add_space got lost"
        end
        if node.size == 0
          # This happens only for the root node if the tree is empty.
          node.size = size
          node.blob_address = address
          node.write_node
          break
        elsif size < node.size
          # The new size is smaller than this node.
          if node.smaller != 0
            # There is already a smaller node, so pass it on.
            node = @tree.get_node(node.smaller)
          else
            # There is no smaller node yet, so we create a new one as a
            # smaller child of the current node.
            node.set_link('@smaller',
                          @tree.get_node(nil, node, address, size).node_address)
            node.write_node
            break
          end
        elsif size > node.size
          # The new size is larger than this node.
          if node.larger != 0
            # There is already a larger node, so pass it on.
            node = @tree.get_node(node.larger)
          else
            # There is no larger node yet, so we create a new one as a larger
            # child of the current node.
            node.set_link('@larger',
                          @tree.get_node(nil, node, address, size).node_address)
            node.write_node
            break
          end
        else
          # Same size as current node. Insert new node as equal child at top of
          # equal list.
          new_node = @tree.get_node(nil, node, address, size)
          new_node.set_link('@equal', node.equal)
          new_node.write_node

          node.set_link('@equal', new_node.node_address)
          node.write_node

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
      if size < @size
        if @smaller != 0
          return @tree.get_node(@smaller).has_space?(address, size)
        end
      elsif size > @size
        if @larger != 0
          return @tree.get_node(@larger).has_space?(address, size)
        end
      else
        return true if @blob_address == address
        if @equal != 0
          return @tree.get_node(@equal).has_space?(address, size)
        end
      end

      false
    end

    # Return an address/size touple that matches the requested size or is
    # larger than the requested size plus the overhead for another blob.
    # Return nil if nothing was found.
    # @param size [Integer] size of the free space
    # @return [Array or nil] address, size touple or nil
    def find_matching_space(size)
      node = self
      parent_node = nil

      loop do
        if node.size < size
          if node.larger != 0
            # The current space is not yet large enough. If we have a larger sub
            # node check that one next.
            parent_node = node
            node = @tree.get_node(node.larger)
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
          node.delete_node(parent_node)
          return [ address, actual_size ]
        elsif node.smaller != 0
          # The current space is larger than size but not large enough for an
          # additional record. So check if we have a perfect match in the
          # smaller brach if available.
          parent_node = node
          node = @tree.get_node(node.smaller)
        else
          break
        end
      end

      return nil
    end


    # Adds address, size touples of of this node and all sub-nodes to the
    # given Array.
    # @param ary [Array]
    def gather_addresses_and_sizes(ary)

      stack = [ self ]
      while !stack.empty?
        node = stack.pop
        ary << [ node.blob_address, node.size ] unless node.size == 0

        stack.push(@tree.get_node(node.larger)) if node.larger != 0
        stack.push(@tree.get_node(node.equal)) if node.equal != 0
        stack.push(@tree.get_node(node.smaller)) if node.smaller != 0
      end
    end

    def replace_node_address(child_node_address, new_address)
      if child_node_address == @smaller
        set_link('@smaller', new_address)
      elsif child_node_address == @equal
        set_link('@equal', new_address)
      elsif child_node_address == @larger
        set_link('@larger', new_address)
      else
        PEROBS.log.fatal "Unknown child node address #{child_node_address}"
      end
      write_node
    end

    # Check this node and all sub nodes for possible structural or logical
    # errors.
    # @param flat_file [FlatFile] If given, check that the space is also
    #        present in the given flat file.
    # @return [false,true] True if OK, false otherwise
    def check(flat_file)
      # We use a non-recursive implementation to traverse the tree. This stack
      # keeps track of all the known still to be checked nodes.
      stack = [ [ self, :smaller ] ]
      node_counter = 0
      max_depth = 0

      while !stack.empty?
        max_depth = stack.size if stack.size > max_depth
        current_node, mode = stack.pop

        case mode
        when :smaller
          stack_addresses = stack.map { |e| e[0].node_address }
          unless current_node.check_node_links('smaller', stack_addresses)
            return false
          end
          unless current_node.check_node_links('equal', stack_addresses)
            return false
          end
          unless current_node.check_node_links('larger', stack_addresses)
            return false
          end

          stack.push([ current_node, :equal ])

          if current_node.smaller != 0
            node = @tree.get_node(current_node.smaller)
            if node.size >= current_node.size
              PEROBS.log.error "Smaller SpaceTreeNode size (#{node.size}) is " +
                "not smaller than #{current_node.size}" + @tree.text_tree
                return false
            end
            stack.push([ node, :smaller ])
          end
        when :equal
          stack.push([ current_node, :larger ])

          if current_node.equal != 0
            node = @tree.get_node(current_node.equal)

            return false unless check_equal_nodes(node, current_node.size)
          end
        when :larger
          stack.push([ current_node, :finalize ])

          if current_node.larger != 0
            node = @tree.get_node(current_node.larger)
            if node.size <= current_node.size
              PEROBS.log.error "Larger SpaceTreeNode size (#{node.size}) is " +
                "not larger than #{current_node.size}"
                return false
            end
            stack.push([ node, :smaller ])
          end
        when :finalize
          if flat_file && !flat_file.has_space?(@blob_address, @size)
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

    def check_node_links(link, parent_addresses)
      link_address = instance_variable_get('@' + link)

      if link_address != 0 && link_address == @node_address
        PEROBS.log.error "#{link} address of node " +
          "[#{@blob_address}, #{@size}] points to self"
        return false
      end

      if link_address != 0 && parent_addresses.include?(link_address)
        parent = @tree.get_node(link_address)
        PEROBS.log.error "#{link} address of node " +
          "[#{@blob_address}, " +
          "#{@size}] points to parent node " +
          "[#{parent.blob_address}, #{parent.size}]"
        stck = parent_addresses.map do |a|
          n = @tree.get_node(a)
          [ a, n.blob_address, n.size ]
        end

        return false
      end

      true
    end

    def check_equal_nodes(node, size)
      loop do
        if node.smaller != 0 || node.larger != 0
          PEROBS.log.error "Equal nodes must not have smaller/larger childs"
          return false
        end

        if node.size != size
          PEROBS.log.error "Equal SpaceTreeNode size (#{node.size}) is " +
            "not equal to #{size}"
            return false
        end

        return true if node.equal == 0

        node = @tree.get_node(node.equal)
      end
    end

    def delete_node(parent_node)
      node = nil
      address_of_node_to_delete = 0
      operation = 'nothing'

      if @equal != 0
        operation = 'equal'
        # Pull-up equal node by copying it's content into the current node.
        address_of_node_to_delete = @equal
        node = @tree.get_node(address_of_node_to_delete)
        set_link('@equal', node.equal)
      elsif @smaller != 0 && @larger == 0
        operation = 'smaller'
        # The node only has a single sub-node on the smaller branch. Pull-up
        # smaller node and replace it with the current node by copying the
        # content of the sub-node.
        address_of_node_to_delete = @smaller
        node = @tree.get_node(address_of_node_to_delete)
        set_link('@smaller', node.smaller)
        set_link('@equal', node.equal)
        set_link('@larger', node.larger)
      elsif @larger != 0 && @smaller == 0
        operation = 'larger'
        # The node only has a single sub-node on the larger branch. Pull-up
        # larger node and replace it with the current node.
        address_of_node_to_delete = @larger
        node = @tree.get_node(address_of_node_to_delete)
        set_link('@smaller', node.smaller)
        set_link('@equal', node.equal)
        set_link('@larger', node.larger)
      elsif @smaller != 0 && @larger != 0
        operation = 'move largest of small tree'
        # We'll replace the current node with the largest node of the
        # smaller sub-tree by copying the values into this node.
        node, address_of_node_to_delete, parent_node =
          @tree.get_node(@smaller).find_largest_node(self)
        if node.smaller != 0
          if (smallest_node = node.find_smallest_node) != node &&
              address_of_node_to_delete != @smaller
            smallest_node.set_link('@smaller', @smaller)
            smallest_node.write_node
          end
          set_link('@smaller', node.smaller)
        end
        set_link('@equal', node.equal)
        if parent_node != self
          parent_node.replace_node_address(address_of_node_to_delete, 0)
        else
          set_link('@smaller', node.smaller)
        end
      end

      if node
        # We have found a node that can replace the node we want to delete.
        # Copy the data from that node into this node.
        @size = node.size
        @blob_address = node.blob_address
        # Update the node file.
        write_node

        # Delete the just copied node from the file.
        @tree.delete_node(address_of_node_to_delete)
      else
        # We have not found a replacement node, so we have to delete this
        # node by removing the link from the parent and deleting it from the
        # file.
        if parent_node
          parent_node.replace_node_address(@node_address, 0)
          @tree.delete_node(@node_address)
        else
          # The root node can't be deleted. We just set @size to 0 to mark it
          # as empty.
          @size = 0
          write_node
        end
      end

      unless @tree.check
        PEROBS.log.fatal "Delete operation #{operation} failed:" +
          @tree.text_tree
      end
    end

    def text_tree(prefix)
      str = "#{prefix}\- #{@node_address} :: [#{@blob_address}, #{@size}]"
      if @smaller != 0
        begin
          str += @tree.get_node(@smaller).text_tree(prefix + "  |< ")
        rescue
          str += prefix + "  |< @@@@@@@@@@"
        end
      end
      if @equal != 0
        begin
          str += @tree.get_node(@equal).text_tree(prefix + "  |= ")
        rescue
          str += prefix + "  |= @@@@@@@@@@"
        end
      end
      if @larger != 0
        begin
          str += @tree.get_node(@larger).text_tree(prefix + "  |> ")
        rescue
          str += prefix + "  |> @@@@@@@@@@"
        end
      end

      str
    end

    def find_smallest_node
      node_address = @node_address
      loop do
        node = @tree.get_node(node_address)
        if node.smaller != 0
          node_address = node.smaller
        else
          # We've found a 'leaf' node.
          return node
        end
      end
    end

    def find_largest_node(parent_node)
      node_address = @node_address
      loop do
        node = @tree.get_node(node_address)
        if node.larger != 0
          node_address = node.larger
        else
          # We've found a 'leaf' node.
          return node, node_address, parent_node
        end
        parent_node = node
      end
    end

    def set_link(name, node_address)
      instance_variable_set(name, node_address)
    end

    def write_node
      bytes = [ @blob_address, @size, @parent ? @parent.node_address : 0,
                @smaller, @equal, @larger ].pack(NODE_BYTES_FORMAT)
      @tree.nodes.store_blob(@node_address, bytes)
    end

    private

    def read_node
      return false unless (bytes = @tree.nodes.retrieve_blob(@node_address))

      @blob_address, @size, parent_node_address,
        @smaller, @equal, @larger = bytes.unpack(NODE_BYTES_FORMAT)
      @parent = parent_node_address != 0 ?
        SpaceTreeNodeLink.new(@tree, parent_node_address) : nil

      true
    end

  end

end
