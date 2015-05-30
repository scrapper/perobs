# encoding: UTF-8
#
# = Hash.rb -- Persistent Ruby Object Store
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

  # A Hash that is transparently persisted in the back-end storage. It is very
  # similar to the Ruby built-in Hash class but has some additional
  # limitations. The hash key must always be a String.
  class Hash < ObjectBase

    # Create a new PersistentHash object.
    # @param store [Store] The Store this hash is stored in
    # @param default [Any] The default value that is returned when no value is
    #        stored for a specific key.
    def initialize(store, default = nil)
      super(store)
      @default = nil
      @data = {}
    end

    # Retrieves the value object corresponding to the
    # key object. If not found, returns the default value.
    def [](key)
      #unless key.is_a?(String)
      #  raise ArgumentError, 'The Hash key must be of type String'
      #end
      _dereferenced(@data.include?(key) ? @data[key] : @default)
    end

    # Associates the value given by value with the key given by key.
    # @param key [String] The key
    # @param value [Any] The value to store
    def []=(key, value)
      #unless key.is_a?(String)
      #  raise ArgumentError, 'The Hash key must be of type String'
      #end
      @data[key] = _referenced(value)
      @store.cache.cache_write(self)

      value
    end

    # Equivalent to Hash::clear
    def clear
      @store.cache.cache_write(self)
      @data.clear
    end

    # Equivalent to Hash::delete
    def delete(key)
      @store.cache.cache_write(self)
      @data.delete(key)
    end

    # Equivalent to Hash::delete_if
    def delete_if
      @store.cache.cache_write(self)
      @data.delete_if do |k, v|
        yield(k, _dereferenced(v))
      end
    end

    # Equivalent to Hash::each
    def each
      @data.each do |k, v|
        yield(k, _dereferenced(v))
      end
    end

    # Equivalent to Hash::each_key
    def each_key
      @data.each_key { |k| yield(k) }
    end

    # Equivalent to Hash::each_value
    def each_value
      @data.each_value do |v|
        yield(_dereferenced(v))
      end
    end

    # Equivalent to Hash::empty?
    def emtpy?
      @data.empty?
    end

    # Equivalent to Hash::has_key?
    def has_key?(key)
      @data.has_key?(key)
    end
    alias include? has_key?
    alias key? has_key?
    alias member? has_key?

    # Equivalent to Hash::keys
    def keys
      @data.keys
    end

    # Equivalent to Hash::length
    def length
      @data.length
    end
    alias size length

    # Equivalent to Hash::map
    def map
      @data.map do |k, v|
        yield(k, _dereferenced(v))
      end
    end

    # Equivalent to Hash::values
    def values
      @data.values.map { |v| _dereferenced(v) }
    end

    # Return a list of all object IDs of all persistend objects that this Hash
    # is referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def _referenced_object_ids
      @data.each_value.select { |v| v && v.is_a?(POReference) }.map { |o| o.id }
    end

    # This method should only be used during store repair operations. It will
    # delete all referenced to the given object ID.
    # @param id [Fixnum/Bignum] targeted object ID
    def _delete_reference_to_id(id)
      @data.delete_if { |k, v| v && v.is_a?(POReference) && v.id == id }
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Hash] the actual Hash object
    # @private
    def _deserialize(data)
      @data = data
    end

    # Textual dump for debugging purposes
    # @return [String]
    def inspect
      "{\n" +
      @data.map { |k, v| "  #{k.inspect}=>#{v.inspect}" }.join(",\n") +
      "\n}\n"
    end

    private

    def _serialize
      @data
    end

  end

end

