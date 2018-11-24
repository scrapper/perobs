# encoding: UTF-8
#
# = IDListPageRecord.rb -- Persistent Ruby Object Store
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

module PEROBS

  # The IDListPageRecord class models the elements of the IDList. Each page
  # holds up to a certain number of IDs that can be cached into a file if
  # needed. Each page holds IDs within a given interval. The cache is managed
  # by the IDListPageFile object.
  class IDListPageRecord

    attr_reader :min_id, :max_id, :page_idx
    attr_accessor :page_entries

    # Create a new IDListPageRecord object.
    # @param page_file [IDListPageFile] The page file that manages the cache.
    # @param min_id [Integer] The smallest ID that can be stored in this page
    # @param max_id [Integer] the largest ID that can be stored in this page
    # @param values [Array] An array of IDs to be stored in this page
    def initialize(page_file, min_id, max_id, values = [])
      @page_file = page_file
      @min_id = min_id
      @max_id = max_id
      @page_entries = 0
      @page_idx = @page_file.new_page(self, values)
    end

    # Check if the given ID is included in this page.
    # @param id [Integer]
    # @return [True of False] Return true if found, false otherwise.
    def include?(id)
      page.include?(id)
    end

    # Check if the page is full and can't store any more IDs.
    # @return [True or False]
    def is_full?
      page.is_full?
    end

    # Insert an ID into the page.
    # @param ID [Integer] The ID to store
    def insert(id)
      page.insert(id)
    end

    # Split the current page. This split is done by splitting the ID range in
    # half. This page will keep the first half, the newly created page will
    # get the second half. This may not actually yield an empty page as all
    # values could remain with one of the pages. In this case further splits
    # need to be issued by the caller.
    # @return [IDListPageRecord] A new IDListPageRecord object.
    def split
      # Determine the new max_id for the old page.
      max_id = @min_id + (@max_id - @min_id) / 2
      # Create a new page that stores the upper half of the ID range. Remove
      # all IDs from this page that now belong into the new page and transfer
      # them.
      new_page_record = IDListPageRecord.new(@page_file, max_id + 1, @max_id,
                                             page.delete(max_id))
      # Adjust the max_id of the current page.
      @max_id = max_id

      new_page_record
    end

    def values
      page.values
    end

    def <=>(pr)
      @min_id <=> pr.min_id
    end

    def check
      unless @min_id < @max_id
        raise RuntimeError, "min_id must be smaller than max_id"
      end

      p = page
      values = p.values
      unless @page_entries == values.length
        raise RuntimeError, "Mismatch between node page_entries " +
          "(#{@page_entries}) and number of values (#{@p.values.length})"
      end

      values.each do |v|
        if v < @min_id
          raise RuntimeError, "Page value #{v} is smaller than min_id " +
            "#{@min_id}"
        end
        if v > @max_id
          raise RuntimeError, "Page value ${v} is larger than max_id #{@max_id}"
        end
      end

      p.check
    end

    private

    def page
      # The leaf pages reference the IDListPage objects only by their index.
      # This method will convert the index into a reference to the actual
      # object. These references should be very short-lived as a life
      # reference prevents the page object from being collected.
      @page_file.page(@page_idx)
    end
  end

end
