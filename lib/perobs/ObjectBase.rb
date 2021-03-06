# encoding: UTF-8
#
# = ObjectBase.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/ClassMap'

module PEROBS

  # This class is used to replace a direct reference to another Ruby object by
  # the Store ID. This makes object disposable by the Ruby garbage collector
  # since it's no longer referenced once it has been evicted from the
  # PEROBS::Store cache. The POXReference objects function as a transparent
  # proxy for the objects they are referencing.
  class POXReference < BasicObject

    attr_reader :store, :id

    def initialize(store, id)
      super()
      @store = store
      @id = id
    end

    # Proxy all calls to unknown methods to the referenced object.
    def method_missing(method_sym, *args, &block)
      unless (obj = _referenced_object)
        ::PEROBS.log.fatal "Internal consistency error. No object with ID " +
          "#{@id} found in the store."
      end
      if obj.respond_to?(:is_poxreference?)
        ::PEROBS.log.fatal "POXReference that references a POXReference found."
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

    def eql?(obj)
      _referenced_object._id == obj._id
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

    # To allow POXReference objects to be used as Hash keys we need to
    # implement this function. Conveniently, we can just use the PEROBS object
    # ID since that is unique.
    def hash
      @id
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

    # This is a list of the native Ruby classes that are supported for
    # instance variable assignements in addition to other PEROBS objects.
    if RUBY_VERSION < '2.2'
      NATIVE_CLASSES = [
        NilClass, Integer, Bignum, Fixnum, Float, String, Time,
        TrueClass, FalseClass
      ]
    else
      NATIVE_CLASSES = [
        NilClass, Integer, Float, String, Time,
        TrueClass, FalseClass
      ]
    end

    attr_reader :_id, :store, :myself

    # New PEROBS objects must always be created by calling # Store.new().
    # PEROBS users should never call this method or equivalents of derived
    # methods directly.
    # @param p [PEROBS::Handle] PEROBS handle
    def initialize(p)
      _initialize(p)
    end

    # This is the real code for initialize. It is called from initialize() but
    # also when we restore objects from the database. In the later case, we
    # don't call the regular constructors. But this code must be exercised on
    # object creation with new() and on restore from DB.
    # param p [PEROBS::Handle] PEROBS handle
    def _initialize(p)
      @store = p.store
      @_id = p.id
      @store._register_in_memory(self, @_id)
      ObjectSpace.define_finalizer(
        self, ObjectBase._finalize(@store, @_id, object_id))
      @_stash_map = nil
      # Allocate a proxy object for this object. User code should only operate
      # on this proxy, never on self.
      @myself = POXReference.new(@store, @_id)
    end

    # This method generates the destructor for the objects of this class. It
    # is done this way to prevent the Proc object hanging on to a reference to
    # self which would prevent the object from being collected. This internal
    # method is not intended for users to call.
    def ObjectBase._finalize(store, id, ruby_object_id)
      proc { store._collect(id, ruby_object_id) }
    end

    # Library internal method to transfer the Object to a new store.
    # @param store [Store] New store
    def _transfer(store)
      @store = store
      # Remove the previously defined finalizer as it is attached to the old
      # store.
      ObjectSpace.undefine_finalizer(self)
      # Register the object as in-memory object with the new store.
      @store._register_in_memory(self, @_id)
      # Register the finalizer for the new store.
      ObjectSpace.define_finalizer(
        self, ObjectBase._finalize(@store, @_id, object_id))
      @myself = POXReference.new(@store, @_id)
    end

    # This method can be overloaded by derived classes to do some massaging on
    # the data after it has been restored from the database. This could either
    # be some sanity check or code to migrate the object from one version to
    # another. It is also the right place to initialize non-persistent
    # instance variables as initialize() will only be called when objects are
    # created for the first time.
    def restore
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

    #
    def _check_assignment_value(val)
      if val.respond_to?(:is_poxreference?)
        # References to other PEROBS::Objects must be handled somewhat
        # special.
        if @store != val.store
          PEROBS.log.fatal 'The referenced object is not part of this store'
        end
      elsif val.is_a?(ObjectBase)
        PEROBS.log.fatal 'A PEROBS::ObjectBase object escaped! ' +
          'Have you used self() instead of myself() to get the reference ' +
          'of the PEROBS object that you are trying to assign here?'
      elsif !NATIVE_CLASSES.include?(val.class)
        PEROBS.log.fatal "Assigning objects of class #{val.class} is not " +
          "supported. Only PEROBS objects or one of the following classes " +
          "are supported: #{NATIVE_CLASSES.join(', ')}"
      end
    end

    # Read an raw object with the specified ID from the backing store and
    # instantiate a new object of the specific type.
    def ObjectBase.read(store, id)
      # Read the object from database.
      db_obj = store.db.get_object(id)

      klass = store.class_map.id_to_class(db_obj['class_id'])
      # Call the constructor of the specified class.
      obj = Object.const_get(klass).allocate
      obj._initialize(Handle.new(store, id))
      obj._deserialize(db_obj['data'])
      obj.restore

      obj
    end

    # Restore the object state from the storage back-end.
    # @param level [Integer] the transaction nesting level
    def _restore(level)
      # Find the most recently stored state of this object. This could be on
      # any previous stash level or in the regular object DB. If the object
      # was created during the transaction, there is no previous state to
      # restore to.
      data = nil
      if @_stash_map
        (level - 1).downto(0) do |lvl|
          break if (data = @_stash_map[lvl])
        end
      end
      if data
        # We have a stashed version that we can restore from.
        _deserialize(data)
      elsif @store.db.include?(@_id)
        # We have no stashed version but can restore from the database.
        db_obj = store.db.get_object(@_id)
        _deserialize(db_obj['data'])
      end
    end

    # Save the object state for this transaction level to the storage
    # back-end. The object gets a new ID that is stored in @_stash_map to map
    # the stash ID back to the original data.
    def _stash(level)
      @_stash_map ||= ::Array.new
      # Get a new ID to store this version of the object.
      @_stash_map[level] = _serialize
    end

  end

end

