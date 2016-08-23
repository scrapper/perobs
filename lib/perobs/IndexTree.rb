# encoding: UTF-8
#
# = IndexTree.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2016 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/FixedSizeBlobFile'
require 'perobs/IndexTreeNode'

module PEROBS

  class IndexTree

    attr_reader :nodes, :ids

    def initialize(db_dir)
      @db_dir = db_dir
      @nodes = FixedSizeBlobFile.new(db_dir, 'database_index', 4 + 16 * 8)
      @ids = FixedSizeBlobFile.new(db_dir, 'object_id_index', 2 * 8)
    end

    def open
      @nodes.open
      @ids.open
      @root = IndexTreeNode.new(self, 0, 0)
    end

    def close
      @ids.close
      @nodes.close
    end

    def put_value(id, value)
      @root.put_value(id, value)
    end

    def get_value(id)
      @root.get_value(id)
    end

    def delete_value(id)
      @root.delete_value(id)
    end

    def inspect
      @root.inspect
    end

  end

end

