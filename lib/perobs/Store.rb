# encoding: UTF-8
#
# = Store.rb -- Persistent Ruby Object Store
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

require 'perobs/Cache'
require 'perobs/FileSystemDB'
require 'perobs/HashedBlobsDB'
require 'perobs/Object'
require 'perobs/Hash'
require 'perobs/Array'

# PErsistent Ruby OBject Store
module PEROBS

  # PEROBS::Store is a persistent storage system for Ruby objects. Regular
  # Ruby objects are transparently stored in a back-end storage and retrieved
  # when needed. It features a garbage collector that removes all objects that
  # are no longer in use.  A build-in cache keeps access latencies to recently
  # used objects low and lazily flushes modified objects into the persistend
  # back-end. Currently only filesystems are supported but it can easily be
  # extended to any key/value database.
  #
  # Persistent objects must be created by deriving your class from
  # PEROBS::Object. Only instance variables that are declared via
  # po_attr will be persistent. It is recommended that references to other
  # objects are all going to persistent objects again.
  class Store

    attr_reader :db, :cache

    # Create a new Store.
    # @param data_base [String] the name of the database
    # @param options [Hash] various options to affect the operation of the
    #        database. Currently the following options are supported:
    #        :engine     : The class that provides the back-end storage
    #                      engine. By default HashedBlobsDB is used. A user
    #                      can provide it's own storage engine that must
    #                      conform to the same API exposed by HashedBlobsDB.
    #        :cache_bits : the number of bits used for cache indexing. The
    #                      cache will hold 2 to the power of bits number of
    #                      objects. We have separate caches for reading and
    #                      writing. The default value is 16. It probably makes
    #                      little sense to use much larger numbers than that.
    #        :serializer : select the format used to serialize the data. There
    #                      are 3 different options:
    #                      :marshal : Native Ruby serializer. Fastest option
    #                      that can handle most Ruby data types. Big
    #                      disadvantate is the stability of the format. Data
    #                      written with one Ruby version may not be readable
    #                      with another version.
    #                      :json : About half as fast as marshal, but the
    #                      format is rock solid and portable between
    #                      languages. It only supports basic Ruby data types
    #                      like String, Fixnum, Float, Array, Hash. This is
    #                      the default option.
    #                      :yaml : Can also handle most Ruby data types and is
    #                      portable between Ruby versions (1.9 and later).
    #                      Unfortunately, it is 10x slower than marshal.
    def initialize(data_base, options = {})
      # Create a backing store handler
      @db = (options[:engine] || HashedBlobsDB).new(data_base, options)

      # The Cache reduces read and write latencies by keeping a subset of the
      # objects in memory.
      @cache = Cache.new(options[:cache_bits] || 16)

      # The named (global) objects IDs hashed by their name
      unless (@root_objects = object_by_id(0))
        @root_objects = Hash.new(self)
        # The root object hash always has the object ID 0.
        @root_objects._change_id(0)
        # The ID change removes it from the write cache. We need to add it
        # again.
        @cache.cache_write(@root_objects)
      end
    end

    # Store the provided object under the given name. Use this to make the
    # object a root or top-level object (think global variable). Each store
    # should have at least one root object. Objects that are not directly or
    # indirectly reachable via any of the root objects are no longer
    # accessible and will be garbage collected.
    # @param name [Symbol] The name to use.
    # @param obj [PEROBS::Object] The object to store
    # @return [PEROBS::Object] The stored object.
    def []=(name, obj)
      # If the passed object is nil, we delete the entry if it exists.
      if obj.nil?
        @root_objects.delete(name)
        return nil
      end

      # We only allow derivatives of PEROBS::Object to be stored in the
      # store.
      unless obj.is_a?(ObjectBase)
        raise ArgumentError, "Object must be of class PEROBS::Object but "
                             "is of class #{obj.class}"
      end

      unless obj.store == self
        raise ArgumentError, 'The object does not belong to this store.'
      end

      # Store the name and mark the name list as modified.
      @root_objects[name] = obj._id
      # Add the object to the in-memory storage list.
      @cache.cache_write(obj)

      obj
    end

    # Return the object with the provided name.
    # @param name [Symbol] A Symbol specifies the name of the object to be
    #        returned.
    # @return The requested object or nil if it doesn't exist.
    def [](name)
      # Return nil if there is no object with that name.
      return nil unless (id = @root_objects[name])

      object_by_id(id)
    end

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      if @cache.in_transaction?
        raise RuntimeError, 'You cannot call sync during a transaction'
      end
      @cache.flush
    end

    # Discard all objects that are not somehow connected to the root objects
    # from the back-end storage. The garbage collector is not invoked
    # automatically. Depending on your usage pattern, you need to call this
    # method periodically.
    def gc
      sync
      mark
      sweep
    end

    # Return the object with the provided ID. This method is not part of the
    # public API and should never be called by outside users. It's purely
    # intended for internal use.
    def object_by_id(id)
      if (obj = @cache.object_by_id(id))
        # We have the object in memory so we can just return it.
        return obj
      else
        # We don't have the object in memory. Let's find it in the storage.
        if @db.include?(id)
          # Great, object found. Read it into memory and return it.
          obj = ObjectBase::read(self, id)
          # Add the object to the in-memory storage list.
          @cache.cache_read(obj)

          return obj
        end
      end

      # The requested object does not exist. Return nil.
      nil
    end

    # This method can be used to check the database and optionally repair it.
    # The repair is a pure structural repair. It cannot ensure that the stored
    # data is still correct. E. g. if a reference to a non-existing or
    # unreadable object is found, the reference will simply be deleted.
    # @param repair [TrueClass/FalseClass] true if a repair attempt should be
    #        made.
    def check(repair = true)
      # Run basic consistency checks first.
      @db.check_db(repair)

      @db.clear_marks
      # A buffer to hold a working set of object IDs.
      stack = []
      # First we check the top-level objects. They are only added to the
      # working set if they are OK.
      @root_objects.each do |name, id|
        unless @db.check(id, repair)
          stack << id
        end
      end
      if repair
        # Delete any top-level object that is defective.
        stack.each { |id| @root_objects.delete(id) }
        # The remaining top-level objects are the initial working set.
        stack = @root_objects.values
      else
        # The initial working set must only be OK objects.
        stack = @root_objects.values - stack
      end
      stack.each { |id| @db.mark(id) }

      while !stack.empty?
        id = stack.pop
        (obj = object_by_id(id))._referenced_object_ids.each do |id|
          # Add all found references that have passed the check to the working
          # list for the next iterations.
          if @db.check(id, repair)
            unless @db.is_marked?(id)
              stack << id
              @db.mark(id)
            end
          elsif repair
            # Remove references to bad objects.
            obj._delete_reference_to_id(id)
          end
        end
      end
    end

    # This method will execute the provided block as an atomic transaction
    # regarding the manipulation of all objects associated with this Store. In
    # case the execution of the block generates an exception, the transaction
    # is aborted and all PEROBS objects are restored to the state at the
    # beginning of the transaction. The exception is passed on to the
    # enclosing scope, so you probably want to handle it accordingly.
    def transaction
      @cache.begin_transaction
      begin
        yield if block_given?
      rescue => e
        @cache.abort_transaction
        raise e
      end
      @cache.end_transaction
    end

    # Calls the given block once for each object, passing that object as a
    # parameter.
    def each
      @db.clear_marks
      # Start with the object 0 and the indexes of the root objects. Push them
      # onto the work stack.
      stack = [ 0 ] + @root_objects.values
      stack.each { |id| @db.mark(id) }
      while !stack.empty?
        # Get an object index from the stack.
        obj = object_by_id(id = stack.pop)
        yield(obj) if block_given?
        obj._referenced_object_ids.each do |id|
          unless @db.is_marked?(id)
            @db.mark(id)
            stack << id
          end
        end
      end
    end

    private

    # Mark phase of a mark-and-sweep garbage collector. It will mark all
    # objects that are reachable from the root objects.
    def mark
      each
    end

    # Sweep phase of a mark-and-sweep garbage collector. It will remove all
    # unmarked objects from the store.
    def sweep
      @db.delete_unmarked_objects
      @cache.reset
    end

  end

end

