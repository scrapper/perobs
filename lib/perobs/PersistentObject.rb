require 'json'
require 'time'

module PEROBS

  class PersistentObject

    # List of attribute types that we support.
    KNOWN_TYPES = %w( Boolean Integer Float Reference String Time )

    # Modify the Metaclass of PersistentObject to add the attribute method and
    # instance variables to store the types and default values of the
    # attributes.
    class << self

      attr_reader :types, :default_values

      def attribute(attr_type, attr_name, value = nil)
        unless attr_name.is_a?(Symbol)
          raise ArgumentError, "attr_name must be a symbol but is a " +
            "#{attr_name.class}"
        end


        # Check that the type is a supported type. These are PROBS specific
        # types that only loosely map to Ruby data types.
        unless PersistentObject::KNOWN_TYPES.include?(attr_type.to_s)
          raise ArgumentError, "attr_type is '#{attr_type}' but must be one " +
            "of #{PersistentObject::KNOWN_TYPES.join(', ')}"
        end

        define_method(attr_name.to_s) do
          get(attr_name)
        end
        define_method(attr_name.to_s + '=') do |val|
          set(attr_name, val)
        end

        @types ||= {}
        @types[attr_name] = attr_type
        @default_values ||= {}
        @default_values[attr_name] = value
      end

    end

    attr_reader :id, :store, :access_time

    @@access_counter = 0

    def initialize
      @store = nil
      @id = nil
      # Create a Hash for the class attributes and initialize them with the
      # default values.
      @attributes = {}
      self.class.types.each do |attr_name, type|
        @attributes[attr_name] = self.class.default_values[attr_name]
      end
      @modified = false
      @access_time = 0
    end

    def sync
      return unless @modified

      db_obj = {
        :class => self.class,
        :types => self.class.types,
        :data => @attributes
      }
      @store.db.put_object(db_obj, @id)
      @modified = false
    end

    # Register the object with a Store. An object can only be registered with
    # one store at a time.
    # @param store [Store] the store to register with
    # @param id [Fixnum or Bignum] the ID of the object in the store
    def register(store, id)
      # Unregister the object with the old store
      @store[@id] = nil if @store

      @store = store
      @id = id
    end

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
        obj.send(attr_name + '=', from_json(db_obj['types'][attr_name], value))
      end
      @access_time = @@access_counter

      obj
    end

    private

    def set(attr, val)
      if self.class.types[attr] == 'Reference'
        unless @store
          raise ArgumentError, "Cannot set references. Object is not " +
                               "stored in any store yet"
        end
        if val.nil?
          @attributes[attr] = nil
        elsif val.is_a?(Bignum) || val.is_a?(Fixnum)
          @attributes[attr] = val
        else
          unless val.is_a?(PersistentObject)
            raise ArgumentError, "val must be a PersistentObject but is a " +
              "#{val.class}."
          end
          unless (@attributes[attr] = val.id)
            raise ArgumentError, "The referenced object must be stored in " +
                                 "the same store as this object."
          end
        end
      else
        @attributes[attr] = val
      end
      @modified = true
      @access_time = (@@access_counter += 1)
      # Ensure that the modified object is part of the store working set.
      @store.add_to_working_set(self)

      val
    end

    def get(attr)
      # Ensure that the object is part of the store working set.
      @store.add_to_working_set(self)
      @access_time = (@@access_counter += 1)

      if self.class.types[attr] == 'Reference'
        unless @store
          raise ArgumentError, "Cannot get references. Object is not " +
                               "stored in any store yet"
        end
        return nil if @attributes[attr].nil?

        @store.get_object_by_id(@attributes[attr])
      else
        @attributes[attr]
      end
    end

    def PersistentObject::from_json(type, json_value)
      return nil if json_value.nil?

      unless PersistentObject::KNOWN_TYPES.include?(type)
        raise ArgumentError, "Unsupported object class '#{type}'"
      end

      # Deal with types that require special handling when converting them
      # from JSON format.
      case type
      when 'Time'
        Time.parse(json_value)
      else
        json_value
      end
    end

  end

end

