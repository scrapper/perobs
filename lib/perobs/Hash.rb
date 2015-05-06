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
      unless key.is_a?(String)
        raise ArgumentError, 'The Hash key must be of type String'
      end
      value = @data.include?(key) ? @data[key] : @default
      value.is_a?(POReference) ? @store.object_by_id(value.id) : value
    end

    # Associates the value given by value with the key given by key.
    # @param key [String] The key
    # @param value [Any] The value to store
    def []=(key, value)
      unless key.is_a?(String)
        raise ArgumentError, 'The Hash key must be of type String'
      end
      if value.is_a?(ObjectBase)
        # The value is a reference to another persistent object. Store the ID
        # of that object in a POReference object.
        if @store != value.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        @data[key] = POReference.new(value.id)
      else
        @data[key] = value
      end
      @store.cache.cache_write(self)

      value
    end

    # Equivalent to Hash::clear
    def clear
      @data.clear
    end

    # Equivalent to Hash::delete
    def delete(key)
      @data.delete(key)
    end

    # Equivalent to Hash::delete_if
    def delete_if
      @data.delete_if do |k, v|
        yield(k, v.is_a?(POReference) ? @store.object_by_id(v.id) : v)
      end
    end

    # Equivalent to Hash::each
    def each
      @data.each do |k, v|
        yield(k, v.is_a?(POReference) ? @store.object_by_id(v.id) : v)
      end
    end

    # Equivalent to Hash::each_key
    def each_key
      @data.each_key { |k| yield(k) }
    end

    # Equivalent to Hash::each_value
    def each_value
      @data.each_value do |v|
        yield(v.is_a?(POReference) ? @store.object_by_id(v.id) : v)
      end
    end

    # Equivalent to Hash::empty?
    def emtpy?
      @data.empty?
    end

    # Equivalent to Hash::empty?
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
        yield(k, v.is_a?(POReference) ? @store.object_by_id(v.id) : v)
      end
    end

    # Return a list of all object IDs of all persistend objects that this Hash
    # is referencing.
    # @return [Array of Fixnum or Bignum] IDs of referenced objects
    def referenced_objects_ids
      @data.each_value.select { |v| v && v.is_a?(POReference) }.map { |o| o.id }
    end

    # Restore the persistent data from a single data structure.
    # This is a library internal method. Do not use outside of this library.
    # @param data [Hash] the actual Hash object
    # @private
    def deserialize(data)
      @data = data
    end

    private

    def serialize
      @data
    end

  end

end

