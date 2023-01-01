# encoding: UTF-8
#
# = ClassMap.rb -- Persistent Ruby Object Store
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

require 'perobs/DataBase'

module PEROBS

  # PEROBS will usually store objects with a relatively small number of
  # classes. Rather than storing the class name with each object, we map the
  # class name to a numerical ID that represents the class in the store. This
  # class handles the mapping and can convert class names into IDs and vice
  # versa.
  class ClassMap

    # Create a ClassMap object for a given data base.
    # @param db [DataBase]
    def initialize(db)
      @db = db
      @by_class = {}
      @by_id = []
      read_map
    end

    # Get the ID for a given class.
    # @param klass [String] Class
    # @return [Integer] ID. If klass is not yet known a new ID will be
    #         allocated.
    def class_to_id(klass)
      @by_class[klass] || new_id(klass)
    end

    # Get the klass for a given ID.
    # @param id [Integer]
    # @return [String] String version of the class
    def id_to_class(id)
      @by_id[id]
    end

    # Get a list of all classes used in the Store.
    # @return [Array] list of Ruby classes
    def classes
      @by_class.keys
    end

    # Rename a set of classes to new names.
    # @param rename_map [Hash] Hash that maps old names to new names
    def rename(rename_map)
      @by_id.each.with_index do |klass, id|
        # Some entries can be nil. Ignore them.
        next unless klass

        if (new_name = rename_map[klass])
          # We have a rename request. Update the current @by_id entry.
          @by_id[id] = new_name
          # Remove the old class name from @by_class hash.
          @by_class.delete(klass)
          # Insert the new one with the current ID.
          @by_class[new_name] = id
        end
      end
    end

    # Delete all classes unless they are contained in _classes_.
    # @param classes [Array of String] List of the class names
    def keep(classes)
      @by_id.each.with_index do |klass, id|
        unless classes.include?(klass)
          # Delete the class from the @by_id list by setting the entry to nil.
          @by_id[id] = nil
          # Delete the corresponding @by_class entry as well.
          @by_class.delete(klass)
        end
      end
    end

    private

    def new_id(klass)
      # Find the first 'nil' entry and return the index.
      idx = @by_id.find_index(nil) || @by_id.length

      # Insert the new class/ID touple into the hash and reverse map.
      @by_class[klass] = idx
      @by_id[idx] = klass
      # Write the updated version into the data base.
      write_map

      # Return the new ID.
      idx
    end

    def read_map
      # Get the hash from the data base
      @by_class = @db.get_hash('class_map')
      # Build the reverse map from the hash.
      @by_class.each do |klass, id|
        @by_id[id] = klass
      end
    end

    def write_map
      @db.put_hash('class_map', @by_class)
    end

  end

end
