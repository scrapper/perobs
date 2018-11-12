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
require 'perobs/Log'
require 'perobs/PersistentObjectCache'

module PEROBS

  class IDListPageFile

    attr_reader :page_size

    def initialize(list, dir, name, max_in_memory, page_size = 512)
      @list = list
      @file_name = File.join(dir, name + '.cache')
      @page_size = page_size
      open
      @pages = PersistentObjectCache.new(max_in_memory, max_in_memory / 2,
                                         IDListPage, self)
      @page_counter = 0
    end

    def load(index)
      begin
        @f.seek(index * @page_size * 8)
        values = @f.read(@page_size * 8).unpack("Q#{@page_size}")
      rescue IOError => e
        PEROBS.log.fatal "Cannot read cache file #{@file_name}: #{e.message}"
      end
      # We use the first value to find the corresponding IDListNode.
      node = @list.node(values[0])
      unless node.page_idx == index
        PEROBS.log.fatal "Page index of found node (#{node.page_idx}) and " +
          "the target index (#{index}) don't match"
      end
      page_entries = node.page_entries
      page = IDListPage.new(self, node, index, values.slice(0, page_entries))
      @pages.insert(page, false)
      page.check

      page
    end

    def page_count
      @page_counter
    end

    def new_page(node)
      idx = @page_counter
      @pages.insert(IDListPage.new(self, node, idx))
      @page_counter += 1
      idx
    end

    def page(index)
      @pages.get(index) || load(index)
    end

    def mark_page_as_modified(page)
      @pages.insert(page)
      @pages.flush
    end

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

    def save_page(page)
      if page.node.page_entries != page.values.length
        raise RuntimeError, "page_entries mismatch for node #{page.uid}"
      end
      begin
        @f.seek(page.uid * @page_size * 8)
        # Pages can sometimes be empty. Since we need a value to find the
        # corresponding IDListNode again in the load method we store the
        # base_id as the first value. Since the IDListNode has the page entry
        # count this value will be discarded again during the load operation.
        if page.values.empty?
          ary = [ page.node.base_id ] + ::Array.new(@page_size - 1, 0)
        else
          ary = page.values + ::Array.new(@page_size - page.values.length, 0)
        end
        @f.write(ary.pack("Q#{@page_size}"))
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

