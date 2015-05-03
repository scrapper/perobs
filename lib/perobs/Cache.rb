module PEROBS

  # The Cache provides two functions for the PEROBS Store. It keeps some
  # amount of objects in memory to substantially reduce read access latencies.
  # It # also stores a list of objects that haven't been synced to the
  # permanent store yet to accelerate object writes.
  class Cache

    # Create a new Cache object.
    # @param store [Store] Reference to the PEROBS Store
    # @param bits [Fixnum] Number of bits for the cache index. This parameter
    #        heavilty affects the performance and memory consumption of the
    #        cache.
    def initialize(store, bits = 16)
      @store = store
      @bits = bits
      # The read and write caches are Arrays. We use the _bits_ least
      # significant bits of the PersistentObject ID to select the index in the
      # read or write cache Arrays.
      @reads = Array.new(2 ** bits)
      @writes = Array.new(2 ** bits)
      # This mask is used to access the _bits_ least significant bits of the
      # object ID.
      @mask = 2 ** bits - 1
    end

    # Add an PersistentObject to the read cache.
    # @param obj [PersistentObject]
    def cache_read(obj)
      unless obj.is_a?(PersistentObject)
        raise ArgumentError, "obj must be a PersistentObject"
      end
      @reads[index(obj)] = obj
    end

    # Add a PersistentObject to the write cache.
    # @param obj [PersistentObject]
    def cache_write(obj)
      unless obj.is_a?(PersistentObject)
        raise ArgumentError, "obj must be a PersistentObject"
      end
      idx = index(obj)
      if (old_obj = @writes[idx]) && old_obj.id != obj.id
        # There is another old object using this cache slot. Before we can
        # re-use the slot, we need to sync it to the permanent storage.
        old_obj.sync
      end
      @writes[idx] = obj
    end

    # Return the PersistentObject with the specified ID or nil if not found.
    # @param id [Fixnum or Bignum] ID of the cached PersistentObject
    def object_by_id(id)
      idx = id & @mask
      # The index is just a hash. We still need to check if the object IDs are
      # actually the same before we can return the object.
      if (obj = @writes[idx]) && obj.id == id
        # The object was in the write cache.
        return obj
      elsif (obj = @reads[idx]) && obj.id == id
        # The object was in the read cache.
        return obj
      end

      nil
    end

    # Flush all pending writes to the persistant storage back-end.
    def flush
      @writes.each { |w| w.sync if w }
      @writes = Array.new(2 ** @bits)
    end

    private

    def index(obj)
      obj.id & @mask
    end

  end

end

