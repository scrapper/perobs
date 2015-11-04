# encoding: UTF-8
#
# = Array.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/ObjectBase'

module PEROBS

  # An Array that is transparently persisted onto the back-end storage. It is
  # very similar to the Ruby built-in Array class but has some additional
  # limitations. The hash key must always be a String.
  class Array < ObjectBase

    # Create a new PersistentArray object.
    # @param store [Store] The Store this hash is stored in
    # @param size [Fixnum] The requested size of the Array
    # @param default [Any] The default value that is returned when no value is
    #        stored for a specific key.
    def initialize(store, size = 0, default = nil)
      super(store)
      @data = ::Array.new(size, default)
    end

    # Equivalent to Array::[]
    def [](index)
      _dereferenced(@data[index])
    end

    # Equivalent to Array::[]=
    def []=(index, obj)
      @data[index] = _referenced(obj)
      @store.cache.cache_write(self)

      obj
    end

    # Equivalent to Array::<<
    def <<(obj)
      @store.cache.cache_write(self)
      @data << _referenced(obj)
    end

    # Equivalent to Array::+
    def +(ary)
      @store.cache.cache_write(self)
      @data + ary
    end

    # Equivalent to Array::push
    def push(obj)
      @store.cache.cache_write(self)
      @data.push(_referenced(obj))
    end

    # Equivalent to Array::pop
    def pop
      @store.cache.cache_write(self)
      _dereferenced(@data.pop)
    end

    # Equivalent to Array::clear
    def clear
      @store.cache.cache_write(self)
      @data.clear
    end

    # Equivalent to Array::delete
    def delete(obj)
      @store.cache.cache_write(self)
      @data.delete { |v| _dereferenced(v) == obj }
    end

    # Equivalent to Array::delete_at
    def delete_at(index)
      @store.cache.cache_write(self)
      @data.delete_at(index)
    end

    # Equivalent to Array::delete_if
    def delete_if
      @data.delete_if do |item|
        yield(_dereferenced(item))
      end
    end

    # Equivalent to Array::find
    def find(ifnone = nil)
      @data.find(ifnone) { |v| yield(_dereferenced(v)) }
    end
    alias detect find

    # Equivalent to Array::each
    def each
      @data.each do |item|
        yield(_dereferenced(item))
      end
    end

    # Equivalent to Array::empty?
    def empty?
      @data.empty?
    end

    # Equivalent to Array::include?
    def include?(obj)
      @data.each { |v| return true if _dereferenced(v) == obj }

      false
    end

    # Equivalent to Array::length
    def length
      @data.length
    end
    alias size length

    # Equivalent to Array::map
    def map
      @data.map do |item|
        yield(_dereferenced(item))
      end
    end
    alias collect map

    # Equivalent to Array::sort!
    def sort!
      if block_given?
        @data.sort! { |v1, v2| yield(_dereferenced(v1), _dereferenced(v2)) }
      else
        @data.sort! { |v1, v2| _dereferenced(v1) <=> _dereferenced(v2) }
      end
    end

    # Convert the PEROBS::Array into a normal Array. All entries that
    # reference other PEROBS objects will be de-referenced. The resulting
    # Array will not include any POReference objects.
    # @return [Array]
    def to_ary
      a = ::Array.new
      @data.each { |v| a << _dereferenced(v) }
      a
    end

    # Return a list of all object IDs of all persistend objects that this Array
    # is referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def _referenced_object_ids
      @data.each.select { |v| v && v.is_a?(POReference) }.map { |o| o.id }
    end

    # This method should only be used during store repair operations. It will
    # delete all referenced to the given object ID.
    # @param id [Fixnum/Bignum] targeted object ID
    def _delete_reference_to_id(id)
      @data.delete_if { |v| v && v.is_a?(POReference) && v.id == id }
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Array] the actual Array object
    # @private
    def _deserialize(data)
      @data = data
    end

    # Textual dump for debugging purposes
    # @return [String]
    def inspect
      "[\n" + @data.map { |v| "  #{v.inspect}" }.join(",\n") + "\n]\n"
    end

    private

    def _serialize
      @data
    end

  end

end

