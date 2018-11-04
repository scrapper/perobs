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

    # This defines the minimum size of the cache line. If it is too large, the
    # time to find an entry will grow too much. If it is too small the number
    # of cache lines will be too large and create more store overhead. By
    # running benchmarks it turned out that 8 is a pretty good compromise.
    WATERMARK = 8

    def initialize
      @entries = []
    end

    def insert(object, modified)
      if (index = @entries.find_index{ |e| e.obj.uid == object.uid })
        # We have found and removed an existing entry for this particular
        # object. If the modified flag is set, ensure that the entry has it
        # set as well.
        entry = @entries.delete_at(index)
        entry.modified = true if modified && !entry.modified
      else
        # There is no existing entry for this object. Create a new one.
        entry = Entry.new(object, modified)
      end

      # Insert the entry at the beginning of the line.
      @entries.unshift(entry)
    end

    def get(uid)
      if (index = @entries.find_index{ |e| e.obj.uid == uid })
        if index > 0
          # Move the entry to the front.
          @entries.unshift(@entries.delete_at(index))
        end
        @entries.first
      else
        nil
      end
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

