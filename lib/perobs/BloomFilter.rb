# encoding: UTF-8
#
# = BloomFilter.rb -- Persistent Ruby Object Store
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

require 'perobs/BitArray'
require 'perobs/FNV_Hash_1a_64'

module PEROBS

  # This is a simple implementation of a Bloom filter. It can take a set of 64
  # bit numbers and tell with high probability if a given number is already in
  # the set. It may produce false positives, but not false negatives. The
  # larger the bitmap_size (2 ** bitmap_size) to set size ratio is the lower
  # the probability of false positives is.
  class BloomFilter

    def initialize(bit_count, seed = rand(2 ** 64))
      mask_bit_count = Math.log(bit_count * 2, 2).ceil
      @bitmap_size = 2 ** mask_bit_count
      @mask = (2 ** mask_bit_count) - 1
      @seed = seed
      clear
    end

    def insert(value)
      xored_value = value ^ @seed
      @bitmap.set(hash1(xored_value))
      @bitmap.set(hash2(xored_value))
    end

    def include?(value)
      xored_value = value ^ @seed
      @bitmap.test(hash1(xored_value)) && @bitmap.test(hash2(xored_value))
    end

    def clear
      @bitmap = BitArray.new(@bitmap_size)
    end

    #private

    def hash1(value)
      FNV_Hash_1a_64.digest(value) & @mask
    end

    def hash2(value)
      reversed_value = 0
      64.times do
        reversed_value = (reversed_value << 1) | (value & 1)
        value >>= 1
      end

      FNV_Hash_1a_64.digest(reversed_value) & @mask
    end

  end

end

