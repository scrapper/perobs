# encoding: UTF-8
#
# = PersistentObjectBase.rb -- Persistent Ruby Object Store
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
  class PersistentObjectBase

    attr_reader :id, :store

    # Create a new PersistentObjectBase object.
    def initialize(store)
      @store = store
      @id = @store.db.new_id

      # Let the store know that we have a modified object.
      @store.cache.cache_write(self)
    end

    # Write the object into the backing store database.
    def sync
      db_obj = {
        :class => self.class,
        :data => serialize
      }
      @store.db.put_object(db_obj, @id)
    end

    # Read an raw object with the specified ID from the backing store and
    # instantiate a new object of the specific type.
    def PersistentObjectBase.read(store, id)
      # Read the object from database.
      db_obj = store.db.get_object(id)

      # Call the constructor of the specified class.
      obj = Object.const_get(db_obj['class']).new(store)
      # There is no public setter for ID since it should be immutable. To
      # restore the ID, we use this workaround.
      obj.send('instance_variable_set', :@id, id)
      obj.deserialize(db_obj['data'])
      # The object restore caused the object to be added to the write cache.
      # To prevent an unnecessary flush to the back-end storage, we will
      # unregister it from the write cache.
      store.cache.unwrite(obj)

      obj
    end

  end

end

