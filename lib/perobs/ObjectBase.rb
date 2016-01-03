# encoding: UTF-8
#
# = ObjectBase.rb -- Persistent Ruby Object Store
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

require 'perobs/ClassMap'

module PEROBS

  # This class is used to replace a direct reference to another Ruby object by
  # the Store ID. This makes object disposable by the Ruby garbage collector
  # since it's no longer referenced once it has been evicted from the
  # PEROBS::Store cache. The POXReference objects function as a transparent
  # proxy for the objects they are referencing.
  class POXReference  < BasicObject

    attr_reader :store, :id

    def initialize(store, id)
      super()
      @store = store
      @id = id
    end

    # Proxy all calls to unknown methods to the referenced object.
    def method_missing(method_sym, *args, &block)
      unless (obj = _referenced_object)
        raise ::RuntimeError, "Internal consistency error. No object with " +
          "ID #{@id} found in the store"
      end
      if obj.respond_to?(:is_poxreference?)
        raise ::RuntimeError,
          "POXReference that references a POXReference found"
      end
      obj.send(method_sym, *args, &block)
    end

    # Proxy all calls to unknown methods to the referenced object. Calling
    # respond_to?(:is_poxreference?) is the only reliable way to find out if
    # the object is a POXReference or not as pretty much every method call is
    # proxied to the referenced object.
    def respond_to?(method_sym, include_private = false)
      (method_sym == :is_poxreference?) ||
        _referenced_object.respond_to?(method_sym, include_private)
    end

    # Just for completeness. We don't want to be caught lying.
    def is_poxreference?
      true
    end

    # @return [ObjectBase] Return the referenced object. This method should
    # not be used outside of the PEROBS library. Leaked references can cause
    # data corruption.
    def _referenced_object
      @store.object_by_id(@id)
    end

    # BasicObject provides a ==() method that prevents method_missing from
    # being called. So we have to pass the call manually to the referenced
    # object.
    # @param obj object to compare this object with.
    def ==(obj)
      _referenced_object == obj
    end

    # BasicObject provides a equal?() method that prevents method_missing from
    # being called. So we have to pass the call manually to the referenced
    # object.
    # @param obj object to compare this object with.
    def equal?(obj)
      if obj.respond_to?(:is_poxreference?)
        _referenced_object.equal?(obj._referenced_object)
      else
        _referenced_object.equal?(obj)
      end
    end

    # Shortcut to access the _id() method of the referenced object.
    def _id
      @id
    end

  end

  # This class is used to serialize the POXReference objects. It only holds
  # the ID of the referenced Object.
  class POReference < Struct.new(:id)
  end

  # Base class for all persistent objects. It provides the functionality
  # common to all classes of persistent objects.
  class ObjectBase

    attr_reader :_id, :store

    # New PEROBS objects must always be created by calling # Store.new().
    # PEROBS users should never call this method or equivalents of derived
    # methods directly.
    def initialize(store)
      @store = store
      unless @store.object_creation_in_progress
        raise ::RuntimeError,
          "All PEROBS objects must exclusively be created by calling " +
          "Store.new(). Never call the object constructor directly."
      end
      @_id = @store.db.new_id
      @_stash_map = nil

      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)
    end

    # If you want another persistent object to reference this object from
    # inside a member method you must call myself() instead of self().
    # myself() will return a proxy object instead of the real object so it can
    # be garbage collected when necessary.
    def myself
      POXReference.new(@store, @_id)
    end

    public

    # This method can be overloaded by derived classes to do some massaging on
    # the data after it has been restored from the database. This could either
    # be some sanity check or code to migrate the object from one version to
    # another.
    def post_restore
    end

    # Two objects are considered equal if their object IDs are the same.
    def ==(obj)
      return false unless obj.is_a?(ObjectBase)
      obj && @_id == obj._id
    end

    # Write the object into the backing store database.
    def _sync
      # Reset the stash map to ensure that it's reset before the next
      # transaction is being started.
      @_stash_map = nil

      db_obj = {
        'class_id' => @store.class_map.class_to_id(self.class.to_s),
        'data' => _serialize
      }
      @store.db.put_object(db_obj, @_id)
    end

    # Read an raw object with the specified ID from the backing store and
    # instantiate a new object of the specific type.
    def ObjectBase.read(store, id)
      # Read the object from database.
      db_obj = store.db.get_object(id)

      klass = store.class_map.id_to_class(db_obj['class_id'])
      # Call the constructor of the specified class.
      obj = store.construct_po(Object.const_get(klass))
      # The object gets created with a new ID by default. We need to restore
      # the old one.
      obj._change_id(id)
      obj._deserialize(db_obj['data'])
      obj.post_restore

      obj
    end

    # Restore the object state from the storage back-end.
    # @param level [Fixnum] the transaction nesting level
    def _restore(level)
      # Find the most recently stored state of this object. This could be on
      # any previous stash level or in the regular object DB. If the object
      # was created during the transaction, there is not previous state to
      # restore to.
      id = nil
      if @_stash_map
        (level - 1).downto(0) do |lvl|
          if @_stash_map[lvl]
            id = @_stash_map[lvl]
            break
          end
        end
      end
      unless id
        if @store.db.include?(@_id)
          id = @_id
        end
      end
      if id
        db_obj = store.db.get_object(id)
        _deserialize(db_obj['data'])
      end
    end

    # Save the object state for this transaction level to the storage
    # back-end. The object gets a new ID that is stored in @_stash_map to map
    # the stash ID back to the original data.
    def _stash(level)
      db_obj = {
        'class' => self.class.to_s,
        'data' => _serialize
      }
      @_stash_map = [] unless @_stash_map
      # Get a new ID to store this version of the object.
      @_stash_map[level] = stash_id = @store.db.new_id
      @store.db.put_object(db_obj, stash_id)
    end

    # Library internal method. Do not use outside of this library.
    # @private
    def _change_id(id)
      # Unregister the object with the old ID from the write cache to prevent
      # cache corruption. The objects are index by ID in the cache.
      @store.cache.unwrite(self)
      @_id = id
    end

  end

end

