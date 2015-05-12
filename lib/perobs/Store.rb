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
  # po_attr will be persistent. All objects that are stored in persitant
  # instance variables must provide a to_json method that generates JSON
  # syntax that can be parsed into their original object again. It is
  # recommended that references to other objects are all going to persistent
  # objects again.
  class Store

    attr_reader :db, :cache

    def initialize(data_base, options = {})
      # Create a backing store handler
      @db = FileSystemDB.new(data_base)

      # The Cache reduces read and write latencies by keeping a subset of the
      # objects in memory.
      @cache = Cache.new(self, options[:cache_bits] || 16)

      # The named (global) objects IDs hashed by their name
      @root_objects = {}
      # Flag that indicates that the root_object list differs from the
      # on-disk version.
      @root_objects_modified = false

      # Load the root object list from the back-end storage.
      @root_objects = @db.get_root_objects
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
      unless name.is_a?(Symbol)
        raise ArgumentError, "name '#{name}' must be a Symbol but is a " +
                             "#{name.class}"
      end

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
      @root_objects_modified = true
      # Add the object to the in-memory storage list.
      @cache.cache_write(obj)

      obj
    end

    # Return the object with the provided name.
    # @param name [Symbol] A Symbol specifies the name of the object to be
    #        returned.
    # @return The requested object or nil if it doesn't exist.
    def [](name)
      if name.is_a?(Symbol)
        # Return nil if there is no object with that name.
        return nil unless @root_objects.include?(name)

        # Find the object ID.
        id = @root_objects[name]
      else
        raise ArgumentError, "name '#{name_or_id}' must be a Symbol but is a " +
                             "#{name_or_id.class}"
      end

      object_by_id(id)
    end

    # Flush out all modified objects to disk and shrink the in-memory list if
    # needed.
    def sync
      # If we have modified the named objects list, write it to disk.
      if @root_objects_modified
        @db.put_root_objects(@root_objects)
        @root_objects_modified = false
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
              if stack.include?(id)
                raise "ID already on stack"
              end
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

    private

    def mark
      @db.clear_marks
      stack = @root_objects.values
      while !stack.empty?
        id = stack.pop
        unless @db.is_marked?(id)
          @db.mark(id)
          obj = object_by_id(id)
          stack += obj._referenced_object_ids
        end
      end
    end

    def sweep
      @db.delete_unmarked_objects
    end

  end

end

