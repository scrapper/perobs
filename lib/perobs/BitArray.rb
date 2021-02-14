# encoding: UTF-8
#
# = BitArray.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2020 by Chris Schlaeger <chris@taskjuggler.org>
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

  class BitArray

    def initialize(size)
      @size = size / 8
      @byte_array = 0.chr * size
      #@word_array = []
    end

    def set(bit_number)
      byte_index, bit_index = bit_number_to_index(bit_number)
      @byte_array[byte_index] = (@byte_array[byte_index].ord | (1 << bit_index)).chr
      #word_index, bit_index = bit_number_to_index(bit_number)
      #@word_array[word_index] = 0 if @word_array[word_index].nil?
      #@word_array[word_index] |= (1 << bit_index)
    end

    def test(bit_number)
      byte_index, bit_index = bit_number_to_index(bit_number)
      return false if byte_index >= @size
      @byte_array[byte_index].ord & (1 << bit_index) != 0
      #word_index, bit_index = bit_number_to_index(bit_number)
      #return false if word_index >= @word_array.length
      #(@word_array[word_index] || 0) & (1 << bit_index) != 0
    end

    def to_i
      int = 0
      i = 0
      @byte_array.each_byte do |c|
        int += c.ord << 8 * i
        i += 1
      end

      int

      #int = 0
      #@word_array.each_with_index do |w, i|
      #  int += (w || 0) << (64 * i)
      #end

      #int
    end

    private

    def bit_number_to_index(bit_number)
      byte_index = bit_number / 8
      if byte_index >= @size
        raise ArgumentError, "Byte index #{byte_index} too large. " +
          "Must be smaller than #{@size}."
      end
      bit_index = bit_number & 0x7
      [ byte_index, bit_index ]
      #[ bit_number >> 6, bit_number & 0x3F ]
    end

  end

end

