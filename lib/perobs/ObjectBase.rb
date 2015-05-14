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

module PEROBS

  # This class is used to replace a direct reference to another Ruby object by
  # the Store ID. This makes object disposable by the Ruby garbage collector
  # since it's no longer referenced once it has been evicted from the
  # PEROBS::Store cache.
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

      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)
    end

    # Two objects are considered equal if their object IDs are the same.
    def ==(obj)
      obj && @_id == obj._id
    end

    # Write the object into the backing store database.
    def _sync
      db_obj = {
        'class' => self.class.to_s,
        'data' => _serialize
      }
      @store.db.put_object(db_obj, @_id)
    end

    # Read an raw object with the specified ID from the backing store and
    # instantiate a new object of the specific type.
    def ObjectBase.read(store, id)
      # Read the object from database.
      db_obj = store.db.get_object(id)

      # Call the constructor of the specified class.
      obj = Object.const_get(db_obj['class']).new(store)
      # The object gets created with a new ID by default. We need to restore
      # the old one.
      obj._change_id(id)
      obj._deserialize(db_obj['data'])

      obj
    end

    # Library internal method. Do not use outside of this library.
    # @private
    def _change_id(id)
      # Unregister the object with the old ID from the write cache to prevent
      # cache corruption. The objects are index by ID in the cache.
      store.cache.unwrite(self)
      @_id = id
    end

    private

    def _dereferenced(v)
      v.is_a?(POReference) ? @store.object_by_id(v.id) : v
    end

    def _referenced(obj)
      if obj.is_a?(ObjectBase)
        # The obj is a reference to another persistent object. Store the ID
        # of that object in a POReference object.
        if @store != obj.store
          raise ArgumentError, 'The referenced object is not part of this store'
        end
        POReference.new(obj._id)
      else
        obj
      end
    end

  end

end

