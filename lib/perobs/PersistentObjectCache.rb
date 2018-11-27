# encoding: UTF-8
#
# = PersistentObjectCache.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016, 2017 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/PersistentObjectCacheLine'

module PEROBS

  class PersistentObjectCache

    class Entry < Struct.new(:object, :modified, :flush_counter)
    end

    # This cache class manages the presence of objects that primarily live in
    # a backing store but temporarily exist in memory as well. To work with
    # these objects, direct references must be only very short lived. Indirect
    # references can be done via a unique ID that the object must provide. Due
    # to the indirect references the Ruby garbage collector can collect these
    # objects and the cache is notified via a finalizer that the objects must
    # provide. The finalizer must call the _collect() method. To reduce the
    # read and write latencies of the backing store this class keeps a subset
    # of the objects in memory which prevents them from being collected. All
    # references to the objects must be resolved via the get() method to
    # prevent duplicate instances in memory of the same in-store object. The
    # cache uses a least-recently-used (LRU) scheme to cache objects.
    # @param size [Integer] Minimum number of objects to be cached at a time
    # @param flush_delay [Integer] Determines how often non-forced flushes are
    #        ignored in a row before the flush is really done.
    # @param klass [Class] The class of the objects to be cached. Objects must
    #        provide a uid() method that returns a unique ID for every object.
    # @param collection [] The object collection the objects belong to. It
    #        must provide a ::load method.
    def initialize(size, flush_delay, klass, collection)
      @size = size
      @klass = klass
      @collection = collection
      @flush_delay = @flush_counter = flush_delay
      @flush_times = 0

      clear
    end

    # Insert an object into the cache.
    # @param object [Object] Object to cache
    # @param modified [Boolean] True if the object was modified, false otherwise
    def insert(object, modified = true)
      unless object.is_a?(@klass)
        raise ArgumentError, "You can insert only #{@klass} objects in this " +
          "cache. You have tried to insert a #{object.class} instead."
      end

      # Store the object via its Ruby object ID instead of a direct reference.
      # This allows the object to be collected by the garbage collector.
      index = object.uid % @size
      if (entry = @entries[index])
        if entry.object == object
          # We already have an entry for this object and it is marked as
          # modified. We have nothing to do here.
          return if entry.modified

          # The existing entry for the same object is not yet marked
          # modified.
          entry.modified = true if modified

          return
        else
          # The current cache entry is for different modified object. We have to
          # flush it out first.
          if entry.modified
            entry.object.save
          end
        end
      end
      @entries[index] = Entry.new(object, modified, @flush_times)

      nil
    end

    # Retrieve a object reference from the cache.
    # @param uid [Integer] uid of the object to retrieve.
    # @param ref [Object] optional reference to be used by the load method
    def get(uid, ref = nil)
      if (entry = @entries[uid % @size]) && (object = entry.object).uid == uid
        return object
      end

      @klass::load(@collection, uid, ref)
    end

    # Remove a object from the cache.
    # @param uid [Integer] unique ID of object to remove.
    def delete(uid)
      index = uid % @size
      if (entry = @entries[index]) && entry.object.uid == uid
        @entries[index] = nil
      end
    end

    # Remove a object from the in-memory list. This is an internal method
    # and should never be called from user code. It will be called from a
    # finalizer, so many restrictions apply!
    # @param address [Integer] Object address of the object to remove from
    #        the list
    # @param ruby_object_id [Integer] The Ruby object ID of the collected
    #        object
    def _collect(address, ruby_object_id)
    end

    # Write all excess modified objects into the backing store. If now is true
    # all modified objects will be written.
    # @param now [Boolean]
    def flush(now = false)
      if now || (@flush_counter -= 1) <= 0
        @entries.each do |entry|
          if entry && entry.modified
            entry.object.save
            entry.modified = false
          end
        end
        @flush_counter = @flush_delay
      end
      @flush_times += 1
    end

    # Remove all entries from the cache.
    def clear
      @entries = ::Array.new(@size)
    end

  end

end

