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

    attr_reader :id

    def initialize(store, id)
      super()
      @store = store
      @id = id
    end

    # Proxy all calls to unknown methods to the referenced object.
    def method_missing(method_sym, *args, &block)
      @store.object_by_id(@id).send(method_sym, *args, &block)
    end

    # Proxy all calls to unknown methods to the referenced object. Calling
    # respond_to?(:is_poxreference?) is the only reliable way to find out if
    # the object is a POXReference or not as pretty much every method call is
    # proxied to the referenced object.
    def respond_to?(method_sym, include_private = false)
      method_sym == :is_poxreference? ||
        @store.object_by_id(@id).respond_to?(method_sym, include_private) ||
        super
    end

    # Just for completeness. We don't want to be caught lying.
    def is_poxreference?
      true
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

    # Create a new PEROBS::ObjectBase object.
    def initialize(store)
      @store = store
      @_id = @store.db.new_id
      @_stash_map = nil

      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)
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
      obj = Object.const_get(klass).new(store)
      # The object gets created with a new ID by default. We need to restore
      # the old one.
      obj._change_id(id)
      obj._deserialize(db_obj['data'])

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

    private

    def _dereferenced(v)
      v.respond_to?(:is_poxreference?) ? @store.object_by_id(v.id) : v
    end

    def _referenced(obj)
      if obj.is_a?(ObjectBase)
        # The obj is a reference to another persistent object. Store the ID
        # of that object in a POXReference object.
        if @store != obj.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        POXReference.new(@store, obj._id)
      else
        obj
      end
    end

  end

end

