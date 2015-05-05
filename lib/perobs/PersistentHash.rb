require 'perobs/PersistentObjectBase'

module PEROBS

  # A Hash that is transparently persisted in the back-end storage. It is very
  # similar to the Ruby built-in Hash class but has some additional
  # limitations. The hash key must always be a String.
  class PersistentHash < PersistentObjectBase

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
      @store.cache.cache_read(self)
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
      if value.is_a?(PersistentObjectBase)
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

