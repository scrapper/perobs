require 'json'
require 'json/add/core'
require 'json/add/struct'
require 'time'

module PEROBS

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

    attr_reader :id, :store, :access_time

    @@access_counter = 0

    # Create a new PersistentObject object.
    def initialize
      # The Store that this object is stored in.
      @store = nil
      # The store-unique ID. This must be a Fixnum or Bignum.
      @id = nil
      # Create a Hash for the class attributes and initialize them with the
      # default values.
      @attributes = {}
      self.class.default_values.each do |attr_name, value|
        @attributes[attr_name] = value
      end
      # This flag will be set to true if the object was modified but not yet
      # written to the Store.
      @modified = true
      # A counter snapshot from the last access to this object.
      @access_time = 0
    end

    # Write the object into the backing store database.
    def sync
      return unless @modified

      db_obj = {
        :class => self.class,
        :data => @attributes
      }
      @store.db.put_object(db_obj, @id)
      @modified = false
    end

    # Read an raw object with the specified ID from the backing store and
    # instantiate a new object of the specific type.
    def PersistentObject.read(store, id)
      @store = store
      # Read the object from database.
      db_obj = @store.db.get_object(id)

      # Call the constructor of the specified class.
      obj = Object.const_get(db_obj['class']).new
      # Register it with the PersistentRubyObjectStore using the specified ID.
      obj.register(store, id)
      # Initialize all attributes with the provided values.
      # TODO: Handle schema changes.
      db_obj['data'].each do |attr_name, value|
        # Call the set method for attr_name
        obj.send(attr_name + '=', value)
      end
      @access_time = @@access_counter

      obj
    end

    # Register the object with a Store. An object can only be registered with
    # one store at a time.
    # @param store [Store] the store to register with
    # @param id [Fixnum or Bignum] the ID of the object in the store
    def register(store, id = nil)
      if @store.nil?
        # Object has never been registered with a Store.
        @store = store
        @id = id || @store.db.new_id
      elsif @store == store
        # The object is already registered with this Store.
        if id && id != @id
          raise "Cannot change ID of an already registered object"
        end
      else
        # The object was registered with a different store before.
        # Unregister the object with the old store.
        @store.delete(@id) if @store && @store != store

        @store = store
        @id = id || @store.db.new_id
      end
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
        # Register the referenced object with the Store of the this object.
        val.register(@store)
        @attributes[attr] = POReference.new(val.id)
        @store.cache.cache_write(val)
      else
        @attributes[attr] = val
      end
      @modified = true
      @access_time = (@@access_counter += 1)
      # Ensure that the modified object is part of the store working set.
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
      @access_time = (@@access_counter += 1)

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

