# encoding: UTF-8
#
# = IDListPageFile.rb -- Persistent Ruby Object Store
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

require 'perobs/IDListPage'
require 'perobs/IDListPageRecord'
require 'perobs/Log'
require 'perobs/PersistentObjectCache'

module PEROBS

  # The IDListPageFile class provides filesystem based cache for the
  # IDListPage objects. The IDListRecord objects only hold the index of the
  # page in this cache. This allows the pages to be garbage collected and
  # swapped to the file. If accessed, the pages will be swaped in again. While
  # this process is similar to the demand paging of the OS it has absolutely
  # nothing to do with it.
  class IDListPageFile

    attr_reader :page_size, :pages

    # Create a new IDListPageFile object that uses the given file in the given
    # directory as cache file.
    # @param list [IDList] The IDList object that caches pages here
    # @param dir [String] An existing directory
    # @param name [String] A file name (without path)
    # @param max_in_memory [Integer] Maximum number of pages to keep in memory
    # @param page_size [Integer] The number of values in each page
    def initialize(list, dir, name, max_in_memory, page_size)
      @list = list
      @file_name = File.join(dir, name + '.cache')
      @page_size = page_size
      open
      @pages = PersistentObjectCache.new(max_in_memory, max_in_memory / 2,
                                         IDListPage, self)
      @page_counter = 0
    end

    # Load the IDListPage from the cache file.
    # @param index [Integer] Index in the cache file.
    # @return [IDListPage] The loaded values
    def load(index)
      begin
        # All pages have the same size indipendently of their actual number of
        # entries.
        @f.seek(index * @page_size * 8)
        # Read the first page entry. It will allow us to find the
        # corresponding IDListPageRecord.
        if (word = @f.read(8)).nil?
          PEROBS.log.fatal "Entry with index #{index} does not exist " +
            "in #{@file_name}"
        end
        page_finder_id = word.unpack('Q').first
      rescue IOError => e
        PEROBS.log.fatal "Cannot read cache file #{@file_name}: #{e.message}"
      end

      # We use the first value to find the corresponding IDListPage.
      record = @list.record(page_finder_id)
      unless record.page_idx == index
        PEROBS.log.fatal "Page index of found record (#{record.page_idx}) " +
          "and the target index (#{index}) don't match"
      end

      # The IDListPageRecord will tell us the actual number of values stored
      # in this page.
      if (page_entries = record.page_entries) == 0
        # The page is empty.
        values = []
      else
        begin
          # Rewind and read all values.
          @f.seek(-8, IO::SEEK_CUR)
          values = @f.read(page_entries * 8).unpack("Q#{page_entries}")
        rescue => e
          PEROBS.log.fatal "Cannot read cache file #{@file_name}: #{e.message}"
        end
      end

      # Create the IDListPage object with the given values.
      page = IDListPage.new(self, record, index, values)
      @pages.insert(page, false)

      page
    end

    # Return the number of registered pages.
    def page_count
      @page_counter
    end

    # Create a new IDListPage and register it.
    # @param record [IDListPageRecord] The corresponding record.
    # @param values [Array of Integer] The values stored in the page
    # @return [IDListPage]
    def new_page(record, values = [])
      idx = @page_counter
      @page_counter += 1
      @pages.insert(IDListPage.new(self, record, idx, values))
      idx
    end

    # Return the IDListPage object with the given index.
    # @param index [Integer] index of the page to get
    # @return [IDListPage] The page corresponding to the index.
    def page(index)
      @pages.get(index) || load(index)
    end

    # Mark a page as modified. This means it has to be written into the cache
    # before it is removed from memory.
    # @param page [IDListPage] page reference
    def mark_page_as_modified(page)
      @pages.insert(page)
      @pages.flush
    end

    # Discard all pages and erase the cache file.
    def erase
      @pages.clear
      @page_counter = 0
      begin
        @f.close
        File.delete(@file_name) if File.exist?(@file_name)
      rescue IOError => e
        PEROBS.log.fatal "Cannot erase cache file #{@file_name}: #{e.message}"
      end
      @f = nil
    end

    # Save the given IDListPage into the cache file.
    # @param page [IDListPage] page to store
    def save_page(page)
      if page.record.page_entries != page.values.length
        raise RuntimeError, "page_entries mismatch for node #{page.uid}"
      end
      begin
        @f.seek(page.uid * @page_size * 8)
        # Pages can sometimes be empty. Since we need a value to find the
        # corresponding IDListNode again in the load method we store the
        # base_id as the first value. Since the IDListNode has the page entry
        # count this value will be discarded again during the load operation.
        ary = page.values.empty? ? [ page.record.min_id ] : page.values
        @f.write(ary.pack('Q*'))
      rescue IOError => e
        PEROBS.log.fatal "Cannot write cache file #{@file_name}: #{e.message}"
      end
    end

    private

    def open
      begin
        # Create a new file by writing a new header.
        @f = File.open(@file_name, 'wb+')
      rescue IOError => e
        PEROBS.log.fatal "Cannot open cache file #{@file_name}: #{e.message}"
      end
    end

  end

end

