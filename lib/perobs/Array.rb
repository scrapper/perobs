# frozen_string_literal: true

# = Array.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016, 2017 by Chris Schlaeger <chris@taskjuggler.org>
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

require_relative 'Log'
require_relative 'ObjectBase'

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

    # These methods do not mutate the Array but create a new PEROBS::Array
    # object. They only perform read operations.
    (%i[
      | & + - collect compact drop drop_while
      flatten map reject reverse rotate select shuffle slice
      sort take take_while uniq values_at
    ] + Enumerable.instance_methods).uniq.each do |method_sym|
      define_method(method_sym) do |*args, &block|
        @store.cache.cache_read(self)
        @store.new(PEROBS::Array, @data.send(method_sym, *args, &block))
      end
    end

    # These methods do not mutate the Array and only perform read operations.
    # They do not return basic objects types.
    (%i[
      == [] <=> at bsearch bsearch_index count cycle
      each each_index empty? eql? fetch find_index first
      frozen? include? index join last length pack
      pretty_print pretty_print_cycle reverse_each rindex sample
      size to_a to_ary to_s
    ] + Enumerable.instance_methods).uniq.each do |method_sym|
      define_method(method_sym) do |*args, &block|
        @store.cache.cache_read(self)
        @data.send(method_sym, *args, &block)
      end
    end

    # These methods mutate the Array and return self.
    %i[
      << clear collect! compact! concat
      fill flatten! insert keep_if map! push
      reject! replace select! reverse! rotate! shuffle!
      slice! sort! sort_by! uniq!
    ].each do |method_sym|
      define_method(method_sym) do |*args, &block|
        @store.cache.cache_write(self)
        @data.send(method_sym, *args, &block)
        myself
      end
    end

    # These methods mutate the Array.
    %i[
      delete delete_at delete_if shift pop
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
    # @param arg1 [Integer or Array] The requested size of the Array or an
    #        Array to initialize
    # @param default [Any] The default value that is returned when no value is
    #        stored for a specific key.
    def initialize(p, arg1 = 0, default = nil)
      super(p)
      if arg1.is_a?(::Array)
        arg1.each { |v| _check_assignment_value(v) }
        @data = arg1.dup
      elsif block_given?
        @data = ::Array.new(arg1) do
          _check_assignment_value(yield)
        end
      else
        @data = ::Array.new(arg1, _check_assignment_value(default))
      end

      # Ensure that the newly created object will be pushed into the database.
      @store.cache.cache_write(self)
    end

    # Proxy for the assignment method.
    def []=(*args)
      if args.length == 2
        _check_assignment_value(args[1])
      else
        _check_assignment_value(args[2])
      end
      @store.cache.cache_write(self)
      @data.[]=(*args)
    end

    # Proxy for the unshift method.
    def unshift(val)
      _check_assignment_value(val)
      @store.cache.cache_write(self)
      @data.unshift(val)
    end

    # Return a list of all object IDs of all persistend objects that this Array
    # is referencing.
    # @return [Array of Integer] IDs of referenced objects
    def _referenced_object_ids
      @data.each.select do |v|
        v&.respond_to?(:is_poxreference?)
      end.map(&:id)
    end

    # This method should only be used during store repair operations. It will
    # delete all references to the given object ID.
    # @param id [Integer] targeted object ID
    def _delete_reference_to_id(id)
      @data.delete_if do |v|
        v && v.respond_to?(:is_poxreference?) && v.id == id
      end
      @store.cache.cache_write(self)
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Array] the actual Array object
    # @private
    def _deserialize(data)
      @data = data.map { |v| v.is_a?(POReference) ?
                         POXReference.new(@store, v.id) : v }
    end

    # Textual dump for debugging purposes
    # @return [String]
    def inspect
      "<#{self.class}:#{@_id}>\n[\n" +
      @data.map do |v|
        "  " + (v.respond_to?(:is_poxreference?) ?
                "<PEROBS::ObjectBase:#{v._id}>" : v.inspect)
      end.join(",\n") +
      "\n]\n"
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
            PEROBS.log.fatal 'A PEROBS::ObjectBase object escaped! ' +
              "It is stored in a PEROBS::Array at index #{@data.index(v)}. " +
              'Have you used self() instead of myself() to ' +
              "get the reference of this PEROBS object?\n" +
              v.inspect
          end
          v
        end
      end
    end
  end
end
