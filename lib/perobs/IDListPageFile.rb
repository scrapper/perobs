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
      @pages = PersistentObjectCache.new(max_in_memory, max_in_memory,
                                         IDListPage, self)
      @page_counter = 0
    end

    # Load the IDListPage from the cache file.
    # @param page_idx [Integer] The page index in the page file
    # @param record [IDListPageRecord] the corresponding IDListPageRecord
    # @return [IDListPage] The loaded values
    def load(page_idx, record)
      # The IDListPageRecord will tell us the actual number of values stored
      # in this page.
      values = []
      unless (entries = record.page_entries) == 0
        begin
          @f.seek(page_idx * @page_size * 8)
          values = @f.read(entries * 8).unpack("Q#{entries}")
        rescue IOError => e
          PEROBS.log.fatal "Cannot read cache file #{@file_name}: #{e.message}"
        end
      end

      # Create the IDListPage object with the given values.
      p = IDListPage.new(self, record, page_idx, values)
      @pages.insert(p, false)

      p
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
      mark_page_as_modified(IDListPage.new(self, record, idx, values))
      idx
    end

    # Return the IDListPage object with the given index.
    # @param record [IDListPageRecord] the corresponding IDListPageRecord
    # @return [IDListPage] The page corresponding to the index.
    def page(record)
      p = @pages.get(record.page_idx, record) || load(record.page_idx, record)
      unless p.uid == record.page_idx
        raise RuntimeError, "Page reference mismatch. Record " +
          "#{record.page_idx} points to page #{p.uid}"
      end

      p
    end

    # Mark a page as modified. This means it has to be written into the cache
    # before it is removed from memory.
    # @param p [IDListPage] page reference
    def mark_page_as_modified(p)
      @pages.insert(p)
      @pages.flush
    end

    # Clear all pages, erase the cache and re-open it again.
    def clear
      @pages.clear
      @page_counter = 0
      begin
        @f.truncate(0)
      rescue IOError => e
        raise RuntimeError, "Cannote truncate cache file #{@file_name}: " +
          e.message
      end
    end

    # Discard all pages and erase the cache file.
    def erase
      @pages.clear
      @page_counter = 0
      close
    end

    # Save the given IDListPage into the cache file.
    # @param p [IDListPage] page to store
    def save_page(p)
      if p.record.page_entries != p.values.length
        raise RuntimeError, "page_entries mismatch for node #{p.uid}"
      end
      begin
        @f.seek(p.uid * @page_size * 8)
        @f.write(p.values.pack('Q*'))
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

    def close
      begin
        @f.close
        File.delete(@file_name) if File.exist?(@file_name)
      rescue IOError => e
        PEROBS.log.fatal "Cannot erase cache file #{@file_name}: #{e.message}"
      end
      @f = nil
    end

  end

end

