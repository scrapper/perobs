# encoding: UTF-8
#
# = StackFile.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'

module PEROBS

  # This class implements a file based stack. All entries must have the same
  # size.
  class StackFile

    # Create a new stack file in the given directory with the given file name.
    # @param dir [String] Directory
    # @param name [String] File name
    # @param entry_bytes [Fixnum] Number of bytes each entry must have
    def initialize(dir, name, entry_bytes)
      @file_name = File.join(dir, name + '.stack')
      @entry_bytes = entry_bytes
      @f = nil
    end

    # Open the stack file.
    def open
      begin
        if File.exist?(@file_name)
          @f = File.open(@file_name, 'rb+')
        else
          @f = File.open(@file_name, 'wb+')
        end
      rescue => e
        PEROBS.log.fatal "Cannot open stack file #{@file_name}: #{e.message}"
      end
    end

    # Close the stack file. This method must be called before the program is
    # terminated to avoid data loss.
    def close
      begin
        @f.flush
        @f.close
      rescue => e
        raise IOError, "Cannot close stack file #{@file_name}: #{e.message}"
      end
    end

    # Push the given bytes onto the stack file.
    # @param bytes [String] Bytes to write.
    def push(bytes)
      if bytes.length != @entry_bytes
        raise ArgumentError, "All stack entries must be #{@entry_bytes} " +
                             "long. This entry is #{bytes.length} bytes long."
      end
      begin
        @f.seek(0, IO::SEEK_END)
        @f.write(bytes)
      rescue => e
        raise IOError, "Cannot push to stack file #{@file_name}: #{e.message}"
      end
    end

    # Pop the last entry from the stack file.
    # @return [String or nil] Popped entry or nil if stack is already empty.
    def pop
      begin
        return nil if @f.size == 0

        @f.seek(-@entry_bytes, IO::SEEK_END)
        bytes = @f.read(@entry_bytes)
        @f.truncate(@f.size - @entry_bytes)
        @f.flush
      rescue => e
        raise IOError, "Cannot pop from stack file #{@file_name}: #{e.message}"
      end

      bytes
    end

    # Remove all entries from the stack.
    def clear
      @f.truncate(0)
      @f.flush
    end

    # Iterate over all entries in the stack and call the given block for the
    # bytes.
    def each
      @f.seek(0)
      while !@f.eof
        yield(@f.read(@entry_bytes))
      end
    end

    # Return the content of the stack as an Array.
    # @return [Array]
    def to_ary
      a = []
      each { |bytes| a << bytes }
      a
    end

  end

end
