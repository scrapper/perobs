# encoding: UTF-8
#
# = PersistentObject.rb -- Persistent Ruby Object Store
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

require 'json'
require 'json/add/core'
require 'json/add/struct'
require 'time'

module PEROBS

  # This class is used to replace a direct reference to another Ruby object by
  # the Store ID. This makes object disposable by the Ruby garbage collector
  # since it's no longer referenced once it has been evicted from the
  # PEROBS::Store cache.
  class POReference < Struct.new(:id)
  end

  # The PersistentObject class is the base class for all objects to be stored
  # in the Store. It provides all the plumbing to define the class attributes
  # and to transparently load and store the instances of the class in the
  # database.
  class PersistentObject

    # Modify the Metaclass of PersistentObject to add the attribute method and
    # instance variables to store the default values of the attributes.
    class << self

      attr_reader :default_values

      # This method can be used to define instance variable for
      # PersistentObject derived classes.
      # @param attr_name [Symbol] Name of the instance variable
      # @param value Default value of the attribute
      def po_attr(attr_name, value = nil)
        unless attr_name.is_a?(Symbol)
          raise ArgumentError, "attr_name must be a symbol but is a " +
            "#{attr_name.class}"
        end

        # Create the attribute reader method with name of attr_name.
        define_method(attr_name.to_s) do
          get(attr_name)
        end
        # Create the attribute writer method with name of attr_name.
        define_method(attr_name.to_s + '=') do |val|
          set(attr_name, val)
        end

        @default_values ||= {}
        @default_values[attr_name] = value
      end

    end

    attr_reader :id, :store

    # Create a new PersistentObject object.
    def initialize(store)
      # The Store that this object is stored in.
      @store = store
      # The store-unique ID. This must be a Fixnum or Bignum.
      @id = @store.db.new_id
      # Create a Hash for the class attributes and initialize them with the
      # default values.
      @attributes = {}
      self.class.default_values.each do |attr_name, value|
        @attributes[attr_name] = value
      end
      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)
    end

    # Write the object into the backing store database.
    def sync
      db_obj = {
        :class => self.class,
        :data => @attributes
      }
      @store.db.put_object(db_obj, @id)
    end

    # Read an raw object with the specified ID from the backing store and
    # instantiate a new object of the specific type.
    def PersistentObject.read(store, id)
      # Read the object from database.
      db_obj = store.db.get_object(id)

      # Call the constructor of the specified class.
      obj = Object.const_get(db_obj['class']).new(store)
      # There is no public setter for ID since it should be immutable. To
      # restore the ID, we use this workaround.
      obj.send('instance_variable_set', :@id, id)
      # Initialize all attributes with the provided values.
      # TODO: Handle schema changes.
      db_obj['data'].each do |attr_name, value|
        # Call the set method for attr_name
        obj.send(attr_name + '=', value)
      end
      # The object restore caused the object to be added to the write cache.
      # To prevent an unnecessary flush to the back-end storage, we will
      # unregister it from the write cache.
      store.cache.unwrite(obj)

      obj
    end

    # Return a list of all object IDs that the attributes of this instance are
    # referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def referenced_object_ids
      ids = []
      @attributes.each do |name, value|
        ids << value.id if value && value.is_a?(POReference)
      end

      ids
    end

    private

    def set(attr, val)
      unless @store
        raise ArgumentError, 'The PersistentObject is not assigned to ' +
                             'any store yet.'
      end

      unless val.respond_to?('to_json')
        raise ArgumentError, "The object of class #{val.class} must have " +
                             "a to_json() method to be stored persistently."
      end

      if val.is_a?(PersistentObject)
        # References to other PersistentObjects must be handled somewhat
        # special.
        if @store != val.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        # To release the object from the Ruby object list later, we store the
        # PEROBS::Store ID of the referenced object instead of the actual
        # reference.
        @attributes[attr] = POReference.new(val.id)
      else
        @attributes[attr] = val
      end
      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)

      val
    end

    def get(attr)
      unless @store
        raise ArgumentError, 'The PersistentObject is not assigned to ' +
                             'any store yet.'
      end

      # Ensure that the object is part of the store working set.
      @store.cache.cache_read(self)

      if @attributes[attr].is_a?(POReference)
        unless @store
          raise ArgumentError, "Cannot get references. Object is not " +
                               "stored in any store yet"
        end
        @store.object_by_id(@attributes[attr].id)
      else
        @attributes[attr]
      end
    end

  end

end

