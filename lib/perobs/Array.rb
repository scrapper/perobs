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
  # very similar to the Ruby built-in Array class but like other PEROBS
  # object classes it converts direct references to other PEROBS objects into
  # POXReference objects that only indirectly reference the other object. It
  # also tracks all reads and write to any Array element and updates the cache
  # accordingly.
  #
  # We don't support an Array.initialize_copy proxy as this would conflict
  # with BasicObject.initialize_copy. You can use PEROBS::Array.replace()
  # instead.
  class Array < ObjectBase

    attr_reader :data

    # These methods do not mutate the Array. They only perform read
    # operations.
    ([
      :&, :*, :+, :-, :==, :[], :<=>, :at, :abbrev, :assoc, :bsearch, :collect,
      :combination, :compact, :count, :cycle, :dclone, :drop, :drop_while,
      :each, :each_index, :empty?, :eql?, :fetch, :find_index, :first,
      :flatten, :frozen?, :hash, :include?, :index, :inspect, :join, :last,
      :length, :map, :pack, :permutation, :pretty_print, :pretty_print_cycle,
      :product, :rassoc, :reject, :repeated_combination,
      :repeated_permutation, :reverse, :reverse_each, :rindex, :rotate,
      :sample, :select, :shelljoin, :shuffle, :size, :slice, :sort, :take,
      :take_while, :to_a, :to_ary, :to_s, :transpose, :uniq, :values_at, :zip,
      :|
    ] + Enumerable.instance_methods).uniq.each do |method_sym|
      define_method(method_sym) do |*args, &block|
        @store.cache.cache_read(self)
        @data.send(method_sym, *args, &block)
      end
    end

    # These methods mutate the Array.
    [
      :<<, :[]=, :clear, :collect!, :compact!, :concat, :delete, :delete_at,
      :delete_if, :fill, :flatten!, :insert, :keep_if, :map!, :pop, :push,
      :reject!, :replace, :select!, :reverse!, :rotate!, :shift, :shuffle!,
      :slice!, :sort!, :sort_by!, :uniq!, :unshift
    ].each do |method_sym|
      define_method(method_sym) do |*args, &block|
        @store.cache.cache_write(self)
        @data.send(method_sym, *args, &block)
      end
    end

    # New PEROBS objects must always be created by calling # Store.new().
    # PEROBS users should never call this method or equivalents of derived
    # methods directly.
    # @param p [PEROBS::Handle] PEROBS handle
    # @param size [Fixnum] The requested size of the Array
    # @param default [Any] The default value that is returned when no value is
    #        stored for a specific key.
    def initialize(p, size = 0, default = nil)
      super(p)
      @data = ::Array.new(size, default)
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
        if v.respond_to?(:is_poxreference?)
          POReference.new(v.id)
        else
          # Outside of the PEROBS library all PEROBS::ObjectBase derived
          # objects should not be used directly. The library only exposes them
          # via POXReference proxy objects.
          if v.is_a?(ObjectBase)
            raise RuntimeError, 'A PEROBS::ObjectBase object escaped! ' +
              "It is stored in a PEROBS::Array at index #{@data.index(v)}. " +
              'Have you used self() instead of myself() to' +
              "get the reference of this PEROBS object?\n" +
              v.inspect
          end
          v
        end
      end
    end

  end

end

