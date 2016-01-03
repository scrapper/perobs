# encoding: UTF-8
#
# = Object.rb -- Persistent Ruby Object Store
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

require 'time'

require 'perobs/ObjectBase'

module PEROBS

  # The PEROBS::Object class is the base class for user-defined objects to be
  # stored in the Store. It provides all the plumbing to define the class
  # attributes and to transparently load and store the instances of the class
  # in the database. You can use instance variables like normal instance
  # variables unless they refer to other PEROBS objects. In these cases you
  # must use the accessor methods for these instance variables. You must use
  # accessor methods for any read and write operation to instance variables
  # that hold or should hold PEROBS objects.
  class Object < ObjectBase

    # Modify the Metaclass of PEROBS::Object to add the attribute method and
    # instance variables to store the default values of the attributes.
    class << self

      attr_reader :attributes

      # This method can be used to define instance variable for
      # PEROBS::Object derived classes.
      # @param attributes [Symbol] Name of the instance variable
      def po_attr(*attributes)
        attributes.each do |attr_name|
          unless attr_name.is_a?(Symbol)
            raise ArgumentError, "name must be a symbol but is a " +
              "#{attr_name.class}"
          end

          # Create the attribute reader method with name of attr_name.
          define_method(attr_name.to_s) do
            _get(attr_name)
          end
          # Create the attribute writer method with name of attr_name.
          define_method(attr_name.to_s + '=') do |val|
            _set(attr_name, val)
          end

          # Store a list of the attribute names
          @attributes ||= []
          @attributes << attr_name unless @attributes.include?(attr_name)
        end
      end

    end

    attr_reader :attributes

    # New PEROBS objects must always be created by calling # Store.new().
    # PEROBS users should never call this method or equivalents of derived
    # methods directly.
    def initialize(store)
      super
    end

    # Initialize the specified attribute _attr_ with the value _val_ unless
    # the attribute has been initialized already. Use this method in the class
    # constructor to avoid overwriting values that have been set when the
    # object was reconstructed from the store.
    # @param attr [Symbol] Name of the attribute
    # @param val [Any] Value to be set
    # @return [true|false] True if the value was initialized, otherwise false.
    def init_attr(attr, val)
      if _all_attributes.include?(attr)
        _set(attr, val)
        return true
      end

      false
    end

    # Call this method to manually mark the object as modified. This is
    # necessary if you are using the '@' notation to access instance variables
    # during assignment operations (=, +=, -=, etc.). To avoid having to call
    # this method you can use the self. notation.
    #
    #   @foo = 42      # faster but requires call to mark_as_modified()
    #   self.foo = 42  # somewhat slower
    #
    # IMPORTANT: If you use @foo = ... and forget to call mark_as_modified()
    # your data will only be modified in memory but might not be persisted
    # into the database!
    def mark_as_modified
      @store.cache.cache_write(self)
    end

    # Return a list of all object IDs that the attributes of this instance are
    # referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def _referenced_object_ids
      ids = []
      _all_attributes.each do |attr|
        value = instance_variable_get(('@' + attr.to_s).to_sym)
        ids << value.id if value && value.respond_to?(:is_poxreference?)
      end
      ids
    end

    # This method should only be used during store repair operations. It will
    # delete all referenced to the given object ID.
    # @param id [Fixnum/Bignum] targeted object ID
    def _delete_reference_to_id(id)
      _all_attributes.each do |attr|
        ivar = ('@' + attr.to_s).to_sym
        value = instance_variable_get(ivar)
        if value && value.respond_to?(:is_poxreference?) && value.id == id
          instance_variable_set(ivar, nil)
        end
      end
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Hash] attribute values hashed by their name
    # @private
    def _deserialize(data)
      # Initialize all attributes with the provided values.
      data.each do |attr_name, value|
        value = POXReference.new(@store, value.id) if value.is_a?(POReference)
        instance_variable_set(('@' + attr_name).to_sym, value)
      end
    end

    # Textual dump for debugging purposes
    # @return [String]
    def inspect
      "{\n" +
      _all_attributes.map do |attr|
        ivar = ('@' + attr.to_s).to_sym
        if (value = instance_variable_get(ivar)).respond_to?('is_poxreference?')
          "  #{attr}=>#{value.class}:#{value._id}"
        else
          "  #{attr}=>#{value}"
        end
      end.join(",\n") +
      "\n}\n"
    end

    private

    # Return a single data structure that holds all persistent data for this
    # class.
    def _serialize
      attributes = {}
      _all_attributes.each do |attr|
        ivar = ('@' + attr.to_s).to_sym
        value = instance_variable_get(ivar)
        attributes[attr.to_s] = value.respond_to?(:is_poxreference?) ?
          POReference.new(value.id) : value
      end
      attributes
    end

    def _set(attr, val)
      if val.is_a?(ObjectBase)
        # References to other PEROBS::Objects must be handled somewhat
        # special.
        if @store != val.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        unless val.respond_to?(:is_poxreference?)
          raise ArgumentError, 'A PEROBS::ObjectBase object escaped! ' +
                               'Have you used self() instead of myself() to' +
                               'get the reference of the PEROBS object that ' +
                               'you are trying to assign here?'
        end
      end
      instance_variable_set(('@' + attr.to_s).to_sym, val)
      # Let the store know that we have a modified object.
      mark_as_modified

      val
    end

    def _get(attr)
      instance_variable_get(('@' + attr.to_s).to_sym)
    end

    def _all_attributes
      # PEROBS objects that don't have persistent attributes declared don't
      # really make sense.
      unless self.class.attributes
        raise StandardError
          "No persistent attributes have been declared for " +
          "class #{self.class}. Use 'po_attr' to declare them."
      end
      self.class.attributes
    end

  end

end

