require 'json'
require 'time'

module PEROBS

  class PersistentObject

    # Modify the Metaclass of PersistentObject to add the attribute method and
    # instance variables to store the types and default values of the
    # attributes.
    class << self

      attr_reader :types, :default_values, :known_types

      def attribute(attr_type, attr_name, value = nil)
        unless attr_name.is_a?(Symbol)
          raise ArgumentError, "attr_name must be a symbol but is a " +
            "#{attr_name.class}"
        end

        # List of attribute types that we support.
        @known_types = %w( Boolean Integer Float Reference String Time )

        # Check that the type is a supported type. These are PROBS specific
        # types that only loosely map to Ruby data types.
        unless @known_types.include?(attr_type.to_s)
          raise ArgumentError, "attr_type is '#{attr_type}' but must be one " +
            "of #{@known_types.join(', ')}"
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

    def sync(file_name)
      return unless @modified

      obj = {
        :class => self.class,
        :types => self.class.types,
        :data => @attributes
      }
      File.write(file_name, obj.to_json)
      @modified = false
    end

    def register(store, id)
      @store = store
      @id = id
    end

    def PersistentObject.read(obj_file, store, id)
      @store = store
      # Read the object from disk.
      begin
        obj = JSON.parse(File.read(obj_file))
      rescue IOError
        raise "Cannot read object file #{obj_file}: #{$!}"
      end

      # Call the constructor of the specified class.
      new_obj = Object.const_get(obj['class']).new
      # Register it with the PersistentRubyObjectStore using the specified ID.
      new_obj.register(store, id)
      # Initialize all attributes with the provided values.
      # TODO: Handle schema changes.
      obj['data'].each do |attr_name, value|
        new_obj.send(attr_name + '=',
                     from_json(obj['types'][attr_name], value))
      end
      @access_time = @@access_counter

      new_obj
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
    end

    def get(attr)
      @access_time = (@@access_counter += 1)
      if self.class.types[attr] == 'Reference'
        unless @store
          raise ArgumentError, "Cannot get references. Object is not " +
                               "stored in any store yet"
        end
        return nil if @attributes[attr].nil?
        @store[@attributes[attr]]
      else
        @attributes[attr]
      end
    end

    def PersistentObject::from_json(type, json_value)
      return nil if json_value.nil?

      unless PersistentObject.class.known_types.include?(type)
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

