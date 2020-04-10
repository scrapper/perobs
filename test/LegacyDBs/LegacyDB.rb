# encoding: UTF-8
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
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
#
$:.unshift File.join(File.dirname(__FILE__), '..', '..', 'lib')

require 'perobs'

# This class creates and manages a simple DB with some toy data to check the
# conversion routines for legacy DB formats.
class LegacyDB

  class Fragment < PEROBS::Object

    attr_persist :str, :pred, :succ

    def initialize(p, str, pred = nil)
      super(p)
      self.str = str
      self.pred = pred
      self.succ = nil
    end

  end

  N1 = 293
  N2 = 427

  def initialize(name)
    @name = name
    @store = nil
  end

  def create
    @store = PEROBS::Store.new(@name)
    @store['fragments'] = @store.new(PEROBS::Array)
    @store['metadata'] = @store.new(PEROBS::Hash)
    @store['by_length'] = @store.new(PEROBS::Hash)

    # Create a long string of digits.
    number = (N1**N2).to_s
    # Find a suitable digit that we can use a separator to split the long
    # string into smaller strings.
    separator = find_separator(number)
    @store['metadata']['separator'] = separator
    pred = nil
    # Store all the fragments in the @store['fragments'] array.
    number.split(separator).each do |fragment|
      @store['fragments'] << (f = @store.new(Fragment, fragment, pred))
      # Additionally, we create the doubly-linked list of the fragments.
      pred.succ = f if pred
      pred = f
      # And we store the fragments hashed by their length.
      length = fragment.length.to_s
      if @store['by_length'][length].nil?
        @store['by_length'][length] = @store.new(PEROBS::Array)
      end
      @store['by_length'][length] << f
    end
    @store.exit
  end

  def open
    @store = PEROBS::Store.new(@name)
  end

  def check
    # Recreate the original number from the @store['fragments'] list.
    number = @store['fragments'].map { |f| f.str }.
      join(@store['metadata']['separator'])
    if number.to_i != N1 ** N2
      raise RuntimeError, "Number mismatch\n#{number}\n#{N1 ** N2}"
    end

    # Check the total number of digits based on the bash by length.
    fragment_counter = 0
    total_fragment_length = 0
    @store['by_length'].each do |length, fragments|
      fragment_counter += fragments.length
      total_fragment_length += length.to_i * fragments.length
    end
    if number.length != total_fragment_length + fragment_counter - 1
      raise RuntimeError, "Number length mismatch"
    end

    # Recreate the original number from the linked list forward traversal.
    number = ''
    f = @store['fragments'][0]
    while f
      number += @store['metadata']['separator'] unless number.empty?
      number += f.str
      f = f.succ
    end
    if number.to_i != N1 ** N2
      raise RuntimeError, "Number mismatch\n#{number}\n#{N1 ** N2}"
    end

    # Recreate the original number from the linked list backwards traversal.
    number = ''
    f = @store['fragments'][-1]
    while f
      number = @store['metadata']['separator'] + number unless number.empty?
      number = f.str + number
      f = f.pred
    end
    if number.to_i != N1 ** N2
      raise RuntimeError, "Number mismatch\n#{number}\n#{N1 ** N2}"
    end

    true
  end

  def repair
    @store.check(true)
  end

  private

  def find_separator(str)
    0.upto(9) do |digit|
      c = digit.to_s
      return c if str[0] != c && str[-1] != c
    end

    raise RuntimeError, "Could not find separator"
  end

end

#db = LegacyDB.new('test')
#db.create
#db.open
#db.check

