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
      if @transaction_stack.empty?
        idx = index(obj)
        if (old_obj = @writes[idx]) && old_obj._id != obj._id
          # There is another old object using this cache slot. Before we can
          # re-use the slot, we need to sync it to the permanent storage.
          old_obj._sync
        end
        @writes[idx] = obj
      else
        # When a transaction is active, we don't have a write cache. The read
        # cache is used to speed up access to recently used objects.
        cache_read(obj)
        # Push the reference of the modified object into the write buffer for
        # this transaction level.
        unless @transaction_stack.last.include?(obj)
          @transaction_stack.last << obj
        end
      end
    end

    # Remove an object from the write cache. This will prevent a modified
    # object from being written to the back-end store.
    def unwrite(obj)
      if @transaction_stack.empty?
        idx = index(obj)
        if (old_obj = @writes[idx]).nil? || old_obj._id != obj._id
          raise RuntimeError, "Object to unwrite is not in cache"
        end
        @writes[idx] = nil
      else
        unless @transaction_stack.last.include?(obj)
          raise RuntimeError, 'unwrite failed'
        end
        @transaction_stack.last.delete(obj)
      end
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

    # Returns true if the Cache is currently handling a transaction, false
    # otherwise.
    # @return [true/false]
    def in_transaction?
      !@transaction_stack.empty?
    end

    # Tell the cache to start a new transaction. If no other transaction is
    # active, the write cached is flushed before the transaction is started.
    def begin_transaction
      if @transaction_stack.empty?
        # This is the top-level transaction. Flush the write buffer to save
        # the current state of all objects.
        flush
      else
        @transaction_stack.last.each do |o|
          o._stash(@transaction_stack.length - 1)
        end
      end
      # Push a transaction buffer onto the transaction stack. This buffer will
      # hold a reference to all objects modified during this transaction.
      @transaction_stack.push(::Array.new)
    end

    # Tell the cache to end the currently active transaction. All write
    # operations of the current transaction will be synced to the storage
    # back-end.
    def end_transaction
      case @transaction_stack.length
      when 0
        raise RuntimeError, 'No ongoing transaction to end'
      when 1
        # All transactions completed successfully. Write all modified objects
        # into the backend storage.
        @transaction_stack.pop.each { |o| o._sync }
      else
        # A nested transaction completed successfully. We add the list of
        # modified objects to the list of the enclosing transaction.
        transactions = @transaction_stack.pop
        # Merge the two lists
        @transaction_stack.push(@transaction_stack.pop + transactions)
        # Ensure that each object is only included once in the list.
        @transaction_stack.last.uniq!
      end
    end

    # Tell the cache to abort the currently active transaction. All modified
    # objects will be restored from the storage back-end to their state before
    # the transaction started.
    def abort_transaction
      if @transaction_stack.empty?
        raise RuntimeError, 'No ongoing transaction to abort'
      end
      @transaction_stack.pop.each { |o| o._restore(@transaction_stack.length) }
    end

    # Clear all cached entries. You must call flush before calling this
    # method. Otherwise unwritten objects will be lost.
    def reset
      # The read and write caches are Arrays. We use the _bits_ least
      # significant bits of the PEROBS::ObjectBase ID to select the index in
      # the read or write cache Arrays.
      @reads = ::Array.new(2 ** @bits)
      @writes = ::Array.new(2 ** @bits)
      @transaction_stack = []
    end

    # Don't include the cache buffers in output of other objects that
    # reference Cache.
    def inspect
    end

    private

    def index(obj)
      obj._id & @mask
    end

  end

end

