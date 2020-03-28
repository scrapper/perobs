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

    # This cache class manages the presence of objects that primarily live in
    # a backing store but temporarily exist in memory as well. To work with
    # these objects, direct references must be only very short lived. Indirect
    # references can be done via a unique ID that the object must provide. Due
    # to the indirect references the Ruby garbage collector can collect these
    # objects. To reduce the read and write latencies of the backing store
    # this class keeps a subset of the objects in memory which prevents them
    # from being collected. All references to the objects must be resolved via
    # the get() method to prevent duplicate instances in memory of the same
    # in-store object. The cache uses a least-recently-used (LRU) scheme to
    # cache objects.
    # @param size [Integer] Minimum number of objects to be cached at a time
    # @param flush_delay [Integer] Determines how often non-forced flushes are
    #        ignored in a row before the flush is really done. If flush_delay
    #        is smaller than 0 non-forced flushed will always be ignored.
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

      if modified
        @modified_entries[object.uid] = object
      else
        @unmodified_entries[object.uid % @size] = object
      end

      nil
    end

    # Retrieve a object reference from the cache.
    # @param uid [Integer] uid of the object to retrieve.
    # @param ref [Object] optional reference to be used by the load method
    def get(uid, ref = nil)
      # First check if it's a modified object.
      if (object = @modified_entries[uid])
        return object
      end

      # Then check the unmodified object list.
      if (object = @unmodified_entries[uid % @size]) && object.uid == uid
        return object
      end

      # If we don't have it in memory we need to load it.
      @klass::load(@collection, uid, ref)
    end

    # Remove a object from the cache.
    # @param uid [Integer] unique ID of object to remove.
    def delete(uid)
      @modified_entries.delete(uid)

      index = uid % @size
      if (object = @unmodified_entries[index]) && object.uid == uid
        @unmodified_entries[index] = nil
      end
    end

    # Write all excess modified objects into the backing store. If now is true
    # all modified objects will be written.
    # @param now [Boolean]
    def flush(now = false)
      if now || (@flush_delay >= 0 && (@flush_counter -= 1) <= 0)
        @modified_entries.each do |id, object|
          object.save
          # Add the object to the unmodified object cache. We might still need
          # it again soon.
          @unmodified_entries[object.uid % @size] = object
        end
        @modified_entries = ::Hash.new
        @flush_counter = @flush_delay
      end
      @flush_times += 1
    end

    # Remove all entries from the cache.
    def clear
      # This Array stores all unmodified entries. It has a fixed size and uses
      # a % operation to compute the index from the object ID.
      @unmodified_entries = ::Array.new(@size)

      # This Hash stores all modified entries. It can grow and shrink as
      # needed. A flush operation writes all modified objects into the backing
      # store.
      @modified_entries = ::Hash.new
    end

  end

end

