# encoding: UTF-8
#
# = Cache.rb -- Persistent Ruby Object Store
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

require 'perobs/Store'

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
      # This mask is used to access the _bits_ least significant bits of the
      # object ID.
      @mask = 2 ** bits - 1
      # Initialize the read and write cache
      reset
    end

    # Add an PEROBS::Object to the read cache.
    # @param obj [PEROBS::ObjectBase]
    def cache_read(obj)
      @reads[index(obj)] = obj
    end

    # Add a PEROBS::Object to the write cache.
    # @param obj [PEROBS::ObjectBase]
    def cache_write(obj)
      idx = index(obj)
      if (old_obj = @writes[idx]) && old_obj._id != obj._id
        # There is another old object using this cache slot. Before we can
        # re-use the slot, we need to sync it to the permanent storage.
        old_obj._sync
      end
      @writes[idx] = obj
    end

    # Remove an object from the write cache. This will prevent a modified
    # object from being written to the back-end store.
    def unwrite(obj)
      idx = index(obj)
      if (old_obj = @writes[idx]).nil? || old_obj._id != obj._id
        raise RuntimeError, "Object to unwrite is not in cache"
      end
      @writes[idx] = nil
    end

    # Return the PEROBS::Object with the specified ID or nil if not found.
    # @param id [Fixnum or Bignum] ID of the cached PEROBS::ObjectBase
    def object_by_id(id)
      idx = id & @mask
      # The index is just a hash. We still need to check if the object IDs are
      # actually the same before we can return the object.
      if (obj = @writes[idx]) && obj._id == id
        # The object was in the write cache.
        return obj
      elsif (obj = @reads[idx]) && obj._id == id
        # The object was in the read cache.
        return obj
      end

      nil
    end

    # Flush all pending writes to the persistant storage back-end.
    def flush
      @writes.each { |w| w._sync if w }
      @writes = ::Array.new(2 ** @bits)
    end

    # Clear all cached entries. You must call flush before calling this
    # method. Otherwise unwritten objects will be lost.
    def reset
      # The read and write caches are Arrays. We use the _bits_ least
      # significant bits of the PEROBS::ObjectBase ID to select the index in
      # the read or write cache Arrays.
      @reads = ::Array.new(2 ** @bits)
      @writes = ::Array.new(2 ** @bits)
    end

    def inspect
    end

    private

    def index(obj)
      obj._id & @mask
    end

  end

end

