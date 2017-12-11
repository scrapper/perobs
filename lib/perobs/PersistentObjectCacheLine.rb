# encoding: UTF-8
#
# = PersistentObjectCacheLine.rb -- Persistent Ruby Object Store
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

module PEROBS

  class PersistentObjectCacheLine

    # Utility class to store persistent objects and their
    # modified/not-modified state.
    class Entry < Struct.new(:obj, :modified)
    end

    WATERMARK = 4

    def initialize
      @entries = []
    end

    def insert(object, modified)
      @entries.each do |e|
        if e.obj.uid == object.uid
          if modified && !e.modified
            e.modified = true
          end
          return
        end
      end

      # Insert the new entry at the beginning of the line.
      @entries.unshift(Entry.new(object, modified))
    end

    def get(uid)
      @entries.each do |e|
        return e if e.obj.uid == uid
      end

      nil
    end

    # Delete the entry that matches the given UID
    # @param uid [Integer]
    def delete(uid)
      @entries.delete_if { |e| e.obj.uid == uid }
    end

    # Save all modified entries and delete all but the most recently added.
    def flush(now)
      if now || @entries.length > WATERMARK
        @entries.each do |e|
          if e.modified
            e.obj.save
            e.modified = false
          end
        end

        # Delete all but the first WATERMARK entry.
        @entries = @entries[0..WATERMARK - 1] if @entries.length > WATERMARK
      end
    end

  end

end

