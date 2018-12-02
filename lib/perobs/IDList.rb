# encoding: UTF-8
#
# = IDList.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2018 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/IDListPageFile'
require 'perobs/IDListPageRecord'

module PEROBS

  # This class stores a list of 64 bit values. Values can be added to the list
  # and the presence of a certain value can be checked. It can hold up to 2^64
  # values. It tries to keep values in memory but can store them in a file if
  # needed. A threshold for the in-memory values can be set in the
  # constructor. The stored values are grouped in pages. Each page can hold up
  # to page_size entries.
  class IDList

    # Create a new IDList object. The data that can't be kept in memory will
    # be stored in the specified directory under the given name.
    # @param dir [String] Path of the directory
    # @param name [String] Name of the file
    # @param max_in_memory [Integer] Specifies the maximum number of values
    #        that will be kept in memory. If the list is larger, values will
    #        be cached in the specified file.
    # @param page_size [Integer] The number of values per page. The default
    #        value is 32 which was found the best performing config in  tests.
    def initialize(dir, name, max_in_memory, page_size = 32)
      # The page_file manages the pages that store the values.
      @page_file = IDListPageFile.new(self, dir, name,
                                      max_in_memory, page_size)
      clear
    end

    # Insert a new value into the list.
    # @param id [Integer] The value to add
    def insert(id)
      # Find the index of the page that should hold ID.
      index = @page_records.bsearch_index { |pr| pr.max_id >= id }
      # Get the corresponding IDListPageRecord object.
      page = @page_records[index]

      # In case the page is already full we'll have to create a new page.
      # There is no guarantee that a split will yield an page with space as we
      # split by ID range, not by distributing the values evenly across the
      # two pages.
      while page.is_full?
        new_page = page.split
        # Store the newly created page into the page_records list.
        @page_records.insert(index + 1, new_page)
        if id >= new_page.min_id
          # We need to insert the ID into the newly created page. Adjust index
          # and page reference accordingly.
          index += 1
          page = new_page
        end
      end

      # Insert the ID into the page.
      page.insert(id)
    end

    # Check if a given value is already stored in the list.
    # @param id [Integer] The value to check for
    def include?(id)
      @page_records.bsearch { |pr| pr.max_id >= id }.include?(id)
    end

    # Clear the list and empty the filesystem cache file.
    def clear
      @page_file.clear
      @page_records = [ IDListPageRecord.new(@page_file, 0, 2 ** 64) ]
    end

    # Erase the list including the filesystem cache file. The IDList is no
    # longer usable after this call but the cache file is removed from the
    # filesystem.
    def erase
      @page_file.erase
      @page_records = nil
    end

    # Perform some consistency checks on the internal data structures. Raises
    # a RuntimeError in case a problem is found.
    def check
      last_max = -1
      unless (min_id = @page_records.first.min_id) == 0
        raise RuntimeError, "min_id of first record (#{min_id}) " +
          "must be 0."
      end

      @page_records.each do |pr|
        unless pr.min_id == last_max + 1
          raise RuntimeError, "max_id of previous record (#{last_max}) " +
            "must be exactly 1 smaller than current record (#{pr.min_id})."
        end
        last_max = pr.max_id
        pr.check
      end

      unless last_max == 2 ** 64
        raise RuntimeError, "max_id of last records " +
          "(#{@page_records.last.max_id}) must be #{2 ** 64})."
      end
    end

    def to_a
      a = []
      @page_records.each { |pr| a += pr.values }
      a
    end

    # Print a human readable form of the tree that stores the list. This is
    # only meant for debugging purposes and does not scale for larger trees.
    def to_s
      "\n" + @root.to_s
    end

  end

end

