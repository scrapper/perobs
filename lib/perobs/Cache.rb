# encoding: UTF-8
#
# = Cache.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016, 2019 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/Log'
require 'perobs/Store'

module PEROBS

  # The Cache provides two functions for the PEROBS Store. It keeps some
  # amount of objects in memory to substantially reduce read access latencies.
  # It also stores a list of objects that haven't been synced to the
  # permanent store yet to accelerate object writes.
  class Cache

    # Create a new Cache object.
    # @param bits [Integer] Number of bits for the cache index. This parameter
    #        heavilty affects the performance and memory consumption of the
    #        cache.
    def initialize(bits = 16)
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
      # This is just a safety check. It can probably be disabled in the future
      # to increase performance.
      if obj.respond_to?(:is_poxreference?)
        # If this condition triggers, we have a bug in the library.
        PEROBS.log.fatal "POXReference objects should never be cached"
      end
      @reads[index(obj)] = obj
    end

    # Add a PEROBS::Object to the write cache.
    # @param obj [PEROBS::ObjectBase]
    def cache_write(obj)
      # This is just a safety check. It can probably be disabled in the future
      # to increase performance.
      #if obj.respond_to?(:is_poxreference?)
      #  # If this condition triggers, we have a bug in the library.
      #  PEROBS.log.fatal "POXReference objects should never be cached"
      #end

      if @transaction_stack.empty?
        # We are not in transaction mode.
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
        unless @transaction_stack.last.include?(obj._id)
          @transaction_stack.last << obj._id
          @transaction_objects[obj._id] = obj
        end
      end
    end

    # Evict the object with the given ID from the cache.
    # @param id [Integer] ID of the cached PEROBS::ObjectBase
    # @return [True/False] True if object was stored in the cache. False
    #         otherwise.
    def evict(id)
      unless @transaction_stack.empty?
        PEROBS.log.fatal "You cannot evict entries during a transaction."
      end

      idx = id & @mask
      # The index is just a hash. We still need to check if the object IDs are
      # actually the same before we can return the object.
      if (obj = @writes[idx]) && obj._id == id
        # The object is in the write cache.
        @writes[idx] = nil
        return true
      elsif (obj = @reads[idx]) && obj._id == id
        # The object is in the read cache.
        @reads[idx] = nil
        return true
      end

      false
    end

    # Return the PEROBS::Object with the specified ID or nil if not found.
    # @param id [Integer] ID of the cached PEROBS::ObjectBase
    def object_by_id(id)
      idx = id & @mask

      if @transaction_stack.empty?
        # The index is just a hash. We still need to check if the object IDs are
        # actually the same before we can return the object.
        if (obj = @writes[idx]) && obj._id == id
          # The object was in the write cache.
          return obj
        end
      else
        # During transactions, the read cache is used to provide fast access
        # to modified objects. But it does not store all modified objects
        # since there can be hash collisions. So we also have to check all
        # transaction objects first.
        if (obj = @transaction_objects[id])
          return obj
        end
      end

      if (obj = @reads[idx]) && obj._id == id
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
    # active, the write cache is flushed before the transaction is started.
    def begin_transaction
      if @transaction_stack.empty?
        if @transaction_thread
          PEROBS.log.fatal 'transaction_thread must be nil'
        end
        @transaction_thread = Thread.current
        # The new transaction is the top-level transaction. Flush the write
        # buffer to save the current state of all objects.
        flush
      else
        # Nested transactions are currently only supported within the same
        # thread. If we are in another thread, raise TransactionInOtherThread
        # to pause the calling thread for a bit.
        if @transaction_thread != Thread.current
          raise TransactionInOtherThread
        end
        # Save a copy of all objects that were modified during the enclosing
        # transaction.
        @transaction_stack.last.each do |id|
          @transaction_objects[id]._stash(@transaction_stack.length - 1)
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
        PEROBS.log.fatal 'No ongoing transaction to end'
      when 1
        # All transactions completed successfully. Write all modified objects
        # into the backend storage.
        @transaction_stack.pop.each { |id| @transaction_objects[id]._sync }
        @transaction_objects = ::Hash.new
        @transaction_thread = nil
      else
        # A nested transaction completed successfully. We add the list of
        # modified objects to the list of the enclosing transaction.
        transactions = @transaction_stack.pop
        # Merge the two lists
        @transaction_stack.push(@transaction_stack.pop + transactions)
        # Ensure that each object ID is only included once in the list.
        @transaction_stack.last.uniq!
      end
    end

    # Tell the cache to abort the currently active transaction. All modified
    # objects will be restored from the storage back-end to their state before
    # the transaction started.
    def abort_transaction
      if @transaction_stack.empty?
        PEROBS.log.fatal 'No ongoing transaction to abort'
      end
      @transaction_stack.pop.each do |id|
        @transaction_objects[id]._restore(@transaction_stack.length)
      end
      @transaction_thread = nil
    end

    # Clear all cached entries. You must call flush before calling this
    # method. Otherwise unwritten objects will be lost.
    def reset
      # The read and write caches are Arrays. We use the _bits_ least
      # significant bits of the PEROBS::ObjectBase ID to select the index in
      # the read or write cache Arrays.
      @reads = ::Array.new(2 ** @bits)
      @writes = ::Array.new(2 ** @bits)
      @transaction_stack = ::Array.new
      @transaction_thread = nil
      @transaction_objects = ::Hash.new
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

