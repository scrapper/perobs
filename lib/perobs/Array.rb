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
      value = @data[index]
      value.is_a?(POReference) ? @store.object_by_id(value.id) : value
    end

    # Equivalent to Array::[]=
    def []=(index, value)
      if value.is_a?(ObjectBase)
        # The value is a reference to another persistent object. Store the ID
        # of that object in a POReference object.
        if @store != value.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        @data[index] = POReference.new(value.id)
      else
        @data[index] = value
      end
      @store.cache.cache_write(self)

      value
    end

    # Equivalent to Array::clear
    def clear
      @data.clear
    end

    # Equivalent to Array::delete
    def delete(obj, &block)
      @data.delete(obj, &block)
    end

    # Equivalent to Array::delete_at
    def delete_at(index)
      @data.delete_at(index)
    end

    # Equivalent to Array::delete_if
    def delete_if
      @data.delete_if do |item|
        yield(item.is_a?(POReference) ? @store.object_by_id(item.id) : item)
      end
    end

    # Equivalent to Array::each
    def each
      @data.each do |item|
        yield(item.is_a?(POReference) ? @store.object_by_id(item.id) : item)
      end
    end

    # Equivalent to Array::empty?
    def emtpy?
      @data.empty?
    end

    # Equivalent to Array::length
    def length
      @data.length
    end
    alias size length

    # Equivalent to Array::map
    def map
      @data.map do |item|
        yield(item.is_a?(POReference) ? @store.object_by_id(item.id) : item)
      end
    end
    alias collect map

    # Return a list of all object IDs of all persistend objects that this Array
    # is referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def referenced_objects_ids
      @data.each.select { |v| v && v.is_a?(POReference) }.map { |o| o.id }
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Array] the actual Array object
    # @private
    def deserialize(data)
      @data = data
    end

    private

    def serialize
      @data
    end

  end

end

