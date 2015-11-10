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
require 'perobs/Delegator'

module PEROBS

  # An Array that is transparently persisted onto the back-end storage. It is
  # very similar to the Ruby built-in Array class but has some additional
  # limitations. The hash key must always be a String.
  #
  # The implementation is largely a proxy around the standard Array class. But
  # all mutating methods must be re-implemented to convert PEROBS::Objects to
  # POXReference objects and to register the object as modified with the
  # cache.
  class Array < ObjectBase

    include Delegator

    attr_reader :data

    # Create a new PersistentArray object.
    # @param store [Store] The Store this hash is stored in
    # @param size [Fixnum] The requested size of the Array
    # @param default [Any] The default value that is returned when no value is
    #        stored for a specific key.
    def initialize(store, size = 0, default = nil)
      super(store)
      @data = ::Array.new(size, default)
    end

    # Equivalent to Array::[]=
    def []=(index, obj)
      @store.cache.cache_write(self)
      @data[index] = _referenced(obj)

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
      if ary.is_a?(PEROBS::Array)
        @data + ary.data
      else
        # For non PEROBS::Arrays we need to ensure that all PEROBS::Objects
        # are converted to POXReference objects.
        @data + ary.map { |obj| _referenced(obj) }
      end
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
      @store.cache.cache_write(self)
      @data.delete_if do |item|
        yield(_dereferenced(item))
      end
    end

    # Equivalent to Array::sort!
    def sort!
      @store.cache.cache_write(self)
      if block_given?
        @data.sort! { |v1, v2| yield(_dereferenced(v1), _dereferenced(v2)) }
      else
        @data.sort! { |v1, v2| _dereferenced(v1) <=> _dereferenced(v2) }
      end
    end

    # Convert the PEROBS::Array into a normal Array. All entries that
    # reference other PEROBS objects will be de-referenced. The resulting
    # Array will not include any POXReference objects.
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
      @data.each.select do |v|
        v && v.respond_to?(:is_poxreference?)
      end.map { |o| o.id }
    end

    # This method should only be used during store repair operations. It will
    # delete all referenced to the given object ID.
    # @param id [Fixnum/Bignum] targeted object ID
    def _delete_reference_to_id(id)
      @data.delete_if do |v|
        v && v.respond_to?(:is_poxreference?) && v.id == id
      end
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Array] the actual Array object
    # @private
    def _deserialize(data)
      @data = data.map { |v| v.is_a?(POReference) ?
                         POXReference.new(@store, v.id) : v }
    end

    private

    def _serialize
      @data.map do |v|
        v.respond_to?(:is_poxreference?) ? POReference.new(v.id) : v
      end
    end

  end

end

