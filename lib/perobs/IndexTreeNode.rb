# encoding: UTF-8
#
# = IndexTreeNode.rb -- Persistent Ruby Object Store
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

module PEROBS

  # The IndexTreeNode is the building block of the IndexTree. Each node can
  # hold up to 16 entries. An entry can be empty (0), an reference to the
  # value (1) or a reference to another IndexTreeNode for the next nibble.
  # Each level of the tree is associated with an specific nibble of the ID.
  # The nibble is used to identify the entry within the node.
  class IndexTreeNode

    attr_reader :address

    # Create a new IndexTreeNode.
    # @param tree [IndexTree] The tree this node belongs to
    # @param nibble [Fixnum] the level of the node in the tree (root being 0)
    # @param address [Integer] The address of this node in the blob file
    def initialize(tree, nibble, address = nil)
      @tree = tree
      if nibble >= 16
        raise ArgumentError, 'nibble must be 0 - 15'
      end
      @nibble = nibble
      if (@address = address).nil? || !read_node
        # Create a new node if none with this address exists already.
        @entry_types = 0
        @entries = ::Array.new(16, 0)
        @address = @tree.nodes.free_address
        write_node
      end
    end

    # Store a value for the given ID. Existing values will be overwritten.
    # @param id [Integer] ID (or key)
    # @param value [Integer] value
    def put_value(id, value)
      index = calc_index(id)
      case get_entry_type(index)
      when 0
        # The entry is still empty. Store the id and value and set the entry
        # to holding a value (1).
        set_entry_type(index, 1)
        @entries[index] = address = @tree.ids.free_address
        store_id_and_value(address, id, value)
        write_node
      when 1
        existing_value = @entries[index]
        existing_id, existing_address = get_id_and_address(existing_value)
        if id == existing_id
          if value != existing_address
            # The entry already holds another value.
            store_id_and_value(@entries[index], id, value)
          end
        else
          # The entry already holds a value. We need to create a new node and
          # store the existing value and the new value in it.
          # First get the exiting value of the entry and the corresponding ID.
          # Create a new node.
          node = IndexTreeNode.new(@tree, @nibble + 1)
          # The entry of the current node is now a reference to the new node.
          set_entry_type(index, 2)
          @entries[index] = node.address
          # Store the existing value and the new value with their IDs.
          node.set_entry(existing_id, existing_value)
          node.put_value(id, value)
        end
        write_node
      when 2
        # The entry is a reference to another node.
        node = IndexTreeNode.new(@tree, @nibble + 1, @entries[index])
        node.put_value(id, value)
      else
        raise RuntimError, "Illegal node type #{get_entry_type(index)}"
      end
    end

    # Retrieve the value for the given ID.
    # @param id [Integer] ID (or key)
    # @return [Integer] value or nil
    def get_value(id)
      index = calc_index(id)
      case get_entry_type(index)
      when 0
        # There is no entry for this ID.
        return nil
      when 1
        # There is a value stored for the ID part that we have seen so far. We
        # still need to compare the requested ID with the full ID to determine
        # a match.
        stored_id, address = get_id_and_address(@entries[index])
        if id == stored_id
          # We have a match. Return the value.
          return address
        else
          # Just a partial match of the least significant nibbles.
          return nil
        end
      when 2
        # The entry is a reference to another node. Just follow it and look at
        # the next nibble.
        return IndexTreeNode.new(@tree, @nibble + 1, @entries[index]).
          get_value(id)
      else
        raise RuntimError, "Illegal node type #{get_entry_type(index)}"
      end
    end

    # Delete the entry for the given ID.
    # @param id [Integer] ID or key
    # @return [Boolean] True if a key was found and deleted, otherwise false.
    def delete_value(id)
      index = calc_index(id)
      case get_entry_type(index)
      when 0
        # There is no entry for this ID.
        return false
      when 1
        # We have a value. Check that the ID matches and delete the value.
        stored_id, address = get_id_and_address(@entries[index])
        if id == stored_id
          @tree.ids.delete_blob(@entries[index])
          @entries[index] = 0
          set_entry_type(index, 0)
          write_node
          return true
        else
          # Just a partial ID match.
          return false
        end
      when 2
        # The entry is a reference to another node.
        node = IndexTreeNode.new(@tree, @nibble + 1, @entries[index])
        result = node.delete_value(id)
        if node.empty?
          # If the sub-node is empty after the delete we delete the whole
          # sub-node.
          @tree.nodes.delete_blob(@entries[index])
          # Eliminate the reference to the sub-node and update this node in
          # the file.
          set_entry_type(index, 0)
          write_node
        end
        return result
      else
        raise RuntimError, "Illegal node type #{get_entry_type(index)}"
      end
    end

    # Convert the node and all sub-nodes to human readable format.
    def inspect
      str = "{\n"
      0.upto(15) do |i|
        case get_entry_type(i)
        when 0
          # Don't show empty entries.
        when 1
          id, address = get_id_and_address(@entries[i])
          str += "  #{id} => #{address},\n"
        when 2
          str += "  " + IndexTreeNode.new(@tree, @nibble + 1, @entries[i]).
            inspect.gsub(/\n/, "\n  ")
        end
      end
      str + "}\n"
    end

    # Utility method to set the value of an existing node entry.
    # @param id [Integer] ID or key
    # @param value [Integer] value to set. Note that this value must be an
    # address from the ids list.
    def set_entry(id, value)
      index = calc_index(id)
      set_entry_type(index, 1)
      @entries[index] = value
    end

    # Check if the node is empty.
    # @return [Boolean] True if all entries are empty.
    def empty?
      @entry_types == 0
    end

    private

    def calc_index(id)
      (id >> (4 * @nibble)) & 0xF
    end

    def read_node
      return false unless (bytes = @tree.nodes.retrieve_blob(@address))
      @entry_types = bytes[0, 4].unpack('L')[0]
      @entries = bytes[4, 16 * 8].unpack('Q16')
      true
    end

    def write_node
      bytes = ([ @entry_types ] + @entries).pack('LQ16')
      @tree.nodes.store_blob(@address, bytes)
    end

    def set_entry_type(index, type)
      if index < 0 || index > 15
        raise ArgumentError, "Index must be between 0 and 15"
      end
      @entry_types = ((@entry_types & ~(0x3 << 2 * index)) |
                     ((type & 0x3) << 2 * index)) & 0xFFFFFFFF
    end

    def get_entry_type(index)
      if index < 0 || index > 15
        raise ArgumentError, "Index must be between 0 and 15"
      end
      (@entry_types >> 2 * index) & 0x3
    end

    def get_id_and_address(id_address)
      @tree.ids.retrieve_blob(id_address).unpack('QQ')
    end

    def store_id_and_value(address, id, value)
      @tree.ids.store_blob(address, [ id, value ].pack('QQ'))
    end

  end

end
