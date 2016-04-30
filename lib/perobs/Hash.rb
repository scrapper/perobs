# encoding: UTF-8
#
# = Hash.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
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
  #
  # The implementation is largely a proxy around the standard Hash class. But
  # all mutating methods must be re-implemented to convert PEROBS::Objects to
  # POXReference objects and to register the object as modified with the
  # cache.
  #
  # We explicitely don't support Hash::store() as it conflicts with
  # ObjectBase::store() method to access the store.
  class Hash < ObjectBase

    # These methods do not mutate the Hash. They only perform read
    # operations.
    ([
      :==, :[], :assoc, :compare_by_identity, :compare_by_identity?, :default,
      :default_proc, :each, :each_key, :each_pair, :each_value, :empty?,
      :eql?, :fetch, :flatten, :has_key?, :has_value?, :hash, :include?,
      :invert, :key, :key?, :keys, :length, :member?, :merge,
      :pretty_print, :pretty_print_cycle, :rassoc, :reject, :select, :size,
      :to_a, :to_h, :to_hash, :to_s, :value?, :values, :values_at
    ] + Enumerable.instance_methods).uniq.each do |method_sym|
      # Create a wrapper method that passes the call to @data.
      define_method(method_sym) do |*args, &block|
        # Register the read operation with the cache.
        @store.cache.cache_read(self)
        @data.send(method_sym, *args, &block)
      end
    end

    # These methods mutate the Hash.
    [
      :[]=, :clear, :default=, :default_proc=, :delete, :delete_if,
      :initialize_copy, :keep_if, :merge!, :rehash, :reject!, :replace,
      :select!, :shift, :update
    ].each do |method_sym|
      # Create a wrapper method that passes the call to @data.
      define_method(method_sym) do |*args, &block|
        # Register the write operation with the cache.
        @store.cache.cache_write(self)
        @data.send(method_sym, *args, &block)
      end
    end

    # New PEROBS objects must always be created by calling # Store.new().
    # PEROBS users should never call this method or equivalents of derived
    # methods directly.
    # @param p [PEROBS::Handle] PEROBS handle
    # @param default [Any] The default value that is returned when no value is
    #        stored for a specific key.
    def initialize(p, default = nil)
      super(p)
      @default = nil
      @data = {}

      # Ensure that the newly created object will be pushed into the database.
      @store.cache.cache_write(self)
    end

    # Return a list of all object IDs of all persistend objects that this Hash
    # is referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def _referenced_object_ids
      @data.each_value.select { |v| v && v.respond_to?(:is_poxreference?) }.
        map { |o| o.id }
    end

    # This method should only be used during store repair operations. It will
    # delete all referenced to the given object ID.
    # @param id [Fixnum/Bignum] targeted object ID
    def _delete_reference_to_id(id)
      @data.delete_if do |k, v|
        v && v.respond_to?(:is_poxreference?) && v.id == id
      end
      @store.cache.cache_write(self)
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Hash] the actual Hash object
    # @private
    def _deserialize(data)
      @data = {}
      data.each { |k, v| @data[k] = v.is_a?(POReference) ?
                                    POXReference.new(@store, v.id) : v }
      @data
    end

    # Textual dump for debugging purposes
    # @return [String]
    def inspect
      "<#{self.class}:#{@_id}>\n{\n" +
      @data.map do |k, v|
        "  #{k.inspect} => " + (v.respond_to?(:is_poxreference?) ?
                "<PEROBS::ObjectBase:#{v._id}>" : v.inspect)
      end.join(",\n") +
      "\n}\n"
    end

    private

    def _serialize
      data = {}

      @data.each do |k, v|
        if v.respond_to?(:is_poxreference?)
          data[k] = POReference.new(v.id)
        else
          # Outside of the PEROBS library all PEROBS::ObjectBase derived
          # objects should not be used directly. The library only exposes them
          # via POXReference proxy objects.
          if v.is_a?(ObjectBase)
            raise RuntimeError, 'A PEROBS::ObjectBase object escaped! ' +
              "It is stored in a PEROBS::Hash with key #{k.inspect}. " +
              'Have you used self() instead of myself() to' +
              "get the reference of this PEROBS object?\n" +
              v.inspect
          end
          data[k] = v
        end
      end

      data
    end

  end

end

