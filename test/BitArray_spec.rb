# encoding: UTF-8
#
# Copyright (c) 2020 by Chris Schlaeger <chris@taskjuggler.org>
#
# This file contains tests for Array that are similar to the tests for the
# Array implementation in MRI. The ideas of these tests were replicated in
# this code.
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

require 'spec_helper'

require 'perobs/BitArray'

describe PEROBS::BitArray do

  it 'should have all 0 bits after init' do
    a = PEROBS::BitArray.new(1024)
    0.upto(1023) do |i|
      expect(a.test(i)).to be false
    end
  end

  it 'should store a bit' do
    a = PEROBS::BitArray.new(128)
    expect(a.to_i).to eql(0)
    a.set(0)
    expect(a.to_i).to eql(1)
    a.set(1)
    expect(a.to_i).to eql(3)
    a.set(64)
    expect(a.to_i).to eql(2**64 + 3)
  end

  it 'should store and check individual bits' do
    a = PEROBS::BitArray.new(256)
    0.upto(255) do |i|
      a.set(i)
      expect(a.test(i)).to be true
    end
    expect(a.to_i).to eql(115792089237316195423570985008687907853269984665640564039457584007913129639935)
  end

end

