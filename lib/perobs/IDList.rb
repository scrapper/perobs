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
require 'perobs/IDListNode'

module PEROBS

  # This class stores a list of 64 bit values. Values can be added to the list
  # and the presence of a certain value can be checked. It can hold up to 2^64
  # values. It tries to keep values in memory but can store them in a file if
  # needed. A threshold for the in-memory values can be set in the
  # constructor. The stored values are grouped in pages. Each page can hold up
  # to page_size entries. The pages are stored in a binary tree to speed up
  # accesses.
  class IDList

    # Create a new IDList object. The data that can't be kept in memory will
    # be stored in the specified directory under the given name.
    # @param dir [String] Path of the directory
    # @param name [String] Name of the file
    # @param max_in_memory [Integer] Specifies the maximum number of values
    #        that will be kept in memory. If the list is larger, values will
    #        be cached in the specified file.
    # @param page_size [Integer] The number of values per page. The default
    #        value is 512 which then matches the 4 Kbytes block size used on
    #        many file systems.
    def initialize(dir, name, max_in_memory, page_size = 512)
      # The page_file manages the pages that store the values.
      @page_file = IDListPageFile.new(self, dir, name,
                                      max_in_memory / page_size, page_size)
      # The root node of the binary tree that provides quick access to the
      # respective page.
      @root = IDListNode.new(@page_file, 0, 0)
    end

    # Insert a new value into the list.
    # @param id [Integer] The value to add
    def insert(id)
      @root.insert(id)
    end

    # Check if a given value is already stored in the list.
    # @param id [Integer] The value to check for
    def include?(id)
      @root.include?(id)
    end

    # Find the IDListNode that corresponds to the given ID.
    # @param id [Integer] value to look for
    # @return [IDListNode]
    def node(id)
      @root.node(id)
    end

    # Erase the list including the filesystem cache file.
    def erase
      @page_file.erase
      @root = IDListNode.new(@page_file, 0, 0)
    end

    # Perform some consistency checks on the internal data structures. Raises
    # a RuntimeError in case a problem is found.
    def check
      @root.check
    end

  end

end

