# encoding: UTF-8
#
# = Object.rb -- Persistent Ruby Object Store
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
            raise ArgumentError, "attr_name must be a symbol but is a " +
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

    # Create a new PEROBS::Object object.
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
      if self.class.attributes.include?(attr)
        _set(attr, val)
        return true
      end

      false
    end

    # Return a list of all object IDs that the attributes of this instance are
    # referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def _referenced_object_ids
      ids = []
      self.class.attributes.each do |attr|
        value = instance_variable_get(('@' + attr.to_s).to_sym)
        ids << value.id if value && value.is_a?(POReference)
      end
      ids
    end

    # This method should only be used during store repair operations. It will
    # delete all referenced to the given object ID.
    # @param id [Fixnum/Bignum] targeted object ID
    def _delete_reference_to_id(id)
      self.class.attributes.each do |attr|
        ivar = ('@' + attr.to_s).to_sym
        value = instance_variable_get(ivar)
        if value && value.is_a?(POReference)  && value.id == id
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
        instance_variable_set(('@' + attr_name).to_sym, value)
      end
    end

    private

    # Return a single data structure that holds all persistent data for this
    # class.
    def _serialize
      attributes = {}
      self.class.attributes.each do |attr|
        ivar = ('@' + attr.to_s).to_sym
        if (value = instance_variable_get(ivar)).is_a?(ObjectBase)
          raise ArgumentError, "The instance variable #{ivar} contains a " +
                               "reference to a PEROBS::ObjectBase object! " +
                               "This is not allowed. You must use the " +
                               "accessor method to assign a reference to " +
                               "another PEROBS object."
        end
        attributes[attr] = value
      end
      attributes
    end

    def _set(attr, val)
      unless @store
        raise ArgumentError, 'The PEROBS::Object is not assigned to ' +
                             'any store yet.'
      end

      unless val.respond_to?('to_json')
        raise ArgumentError, "The object of class #{val.class} must have " +
                             "a to_json() method to be stored persistently."
      end

      ivar = ('@' + attr.to_s).to_sym
      if val.is_a?(ObjectBase)
        # References to other PEROBS::Objects must be handled somewhat
        # special.
        if @store != val.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        # To release the object from the Ruby object list later, we store the
        # PEROBS::Store ID of the referenced object instead of the actual
        # reference.
        instance_variable_set(ivar, POReference.new(val._id))
      else
        instance_variable_set(ivar, val)
      end
      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)

      val
    end

    def _get(attr)
      value = instance_variable_get(('@' + attr.to_s).to_sym)
      if value.is_a?(POReference)
        unless @store
          raise ArgumentError, "Cannot get references. Object is not " +
                               "stored in any store yet"
        end
        @store.object_by_id(value.id)
      else
        value
      end
    end

  end

end

