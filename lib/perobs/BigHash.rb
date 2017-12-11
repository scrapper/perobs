# encoding: UTF-8
#
# = BigHash.rb -- Persistent Ruby Object Store
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

require 'perobs/Object'
require 'perobs/BigTree'
require 'perobs/Array'

module PEROBS

  # The BigHash is similar to the Hash object in that it provides a simple
  # hash functionality. The difference is that this class scales to much
  # larger data sets essentially limited to the amount of space available on
  # your backing store. The data is persisted immediately and uses
  # transactions to ensure the data consistent. It only provides a small
  # subset of the methods provided by the native Hash class that make sense
  # for giant data sets.
  class BigHash < PEROBS::Object

    # Internally this class uses BigTree to store the values by the hashed
    # key. We are using a 64 bit hash space so collisions are fairly unlikely
    # but not impossible. Therefor we have to store the originial key with the
    # value to ensure that we got the right value. The key and value are
    # stored in an Entry object.
    #
    # In case we have a collision we need to store multiple values for the
    # same hashed key. In that case we store the Entry objects for the same
    # hashed key in a PEROBS::Array object instead of storing the Entry
    # directly in the BigTree.
    class Entry < PEROBS::Object

      attr_persist :key, :value

      def initialize(p, key, value)
        super(p)
        self.key = key
        self.value = value
      end

    end

    attr_persist :btree

    # Create a new BigHash object.
    # @param p [Handle] Store handle
    def initialize(p)
      super(p)
      self.btree = @store.new(PEROBS::BigTree)
    end

    # Insert a value that is associated with the given key. If a value for
    # this key already exists, the value will be overwritten with the newly
    # provided value.
    # @param key [Integer or String]
    # @param value [Any PEROBS storable object]
    def []=(key, value)
      hashed_key = hash_key(key)
      @store.transaction do
        entry = @store.new(Entry, key, value)

        if (existing_entry = @btree.get(hashed_key))
          # There is already an existing entry for this hashed key.
          if existing_entry.is_a?(PEROBS::Array)
            # Find the right index to insert the new entry. If there is
            # already an entry with the same key overwrite that entry.
            index_to_insert = 0
            existing_entry.each do |ae|
              break if ae.key == key
              index_to_insert += 1
            end
            existing_entry[index_to_insert] = entry
          elsif existing_entry.key == key
            # The existing value is for the identical key. We can safely
            # overwrite
            @btree.insert(hashed_key, entry)
          else
            # There is a single existing entry, but for a different key. Create
            # a new PEROBS::Array and store both entries.
            array_entry = @store.new(PEROBS::Array)
            array_entry << existing_entry
            array_entry << entry
            @btree.insert(hashed_key, array_entry)
          end
        else
          # No existing entry. Insert the new entry.
          @btree.insert(hashed_key, entry)
        end
      end
    end

    # Retrieve the value for the given key. If no value for the key is found
    # nil is returned.
    # @param key [Integer or String]
    # @return [Any PEROBS storable object]
    def [](key)
      hashed_key = hash_key(key)
      unless (entry = @btree.get(hashed_key))
        return nil
      end

      if entry.is_a?(PEROBS::Array)
        entry.each do |ae|
          return ae.value if ae.key == key
        end
      else
        return entry.value if entry.key == key
      end

      nil
    end

    # Check if the is a value stored for the given key.
    # @param key [Integer or String]
    # @return [TrueClass or FalseClass]
    def has_key?(key)
      hashed_key = hash_key(key)
      unless (entry = @btree.get(hashed_key))
        return false
      end

      if entry.is_a?(PEROBS::Array)
        entry.each do |ae|
          return true if ae.key == key
        end
      else
        return true if entry.key == key
      end

      false
    end

    # Return the number of entries stored in the hash.
    # @return [Integer]
    def length
      @btree.length
    end

    alias size length

    # Calls the given block for each key/value pair.
    # @yield(key, value)
    def each(&block)
      @btree.each(&block)
    end

    private

    def hash_key(key)
      key.hash
    end

  end

end
