# encoding: UTF-8
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs'
require 'perobs/BigArray'

NODE_ENTRIES = 4

describe PEROBS::BigArray do

  before(:all) do
    @db_name = generate_db_name(__FILE__)
    @store = PEROBS::Store.new(@db_name)
  end

  before(:each) do
    @store['array'] = @a = @store.new(PEROBS::BigArray, NODE_ENTRIES)
  end

  after(:each) do
    @store['array'] = @a = nil
    @store.gc
  end

  after(:all) do
    @store.delete_store
  end

  it 'should be empty on create' do
    expect(@a.empty?).to be true
    expect(@a.length).to eq(0)
    expect(@a.check).to be true
    expect(@a[0]).to be nil
  end

  it 'should append the first element' do
    @a << 0
    expect(@a.empty?).to be false
    expect(@a[0]).to eq(0)
    expect(@a.length).to eq(1)
    expect(@a.check).to be true
  end

  it 'should fill 10 nodes with appends' do
    (10 * NODE_ENTRIES).times do |i|
      @a << i
      expect(@a.check).to be true
      expect(@a.length).to eq(i + 1)
    end
  end

  it 'should insert at 0' do
    @a.insert(0, 0)
    expect(@a.empty?).to be false
  end

  it 'should insert at end' do
    0.upto(3 * NODE_ENTRIES) do |i|
      @a.insert(i, i)
      expect(@a.check).to be true
      expect(@a.length).to eq(i + 1)
    end
  end

  it 'should insert in the middle' do
    0.upto(NODE_ENTRIES - 1) do |i|
      @a << 9999
    end
    0.upto(3 * NODE_ENTRIES) do |i|
      @a.insert(i, i)
      expect(@a.check).to be true
      expect(@a.length).to eq(i + 1 + NODE_ENTRIES)
    end
  end

  it 'should convert to a Ruby Array' do
    expect(@a.to_a).to eql([])
    (3 * NODE_ENTRIES).times do |i|
      @a << i
    end
    expect(@a.to_a).to eql((0..3 * NODE_ENTRIES - 1).to_a)
  end

  it 'should support the [] operator' do
    expect(@a[0]).to be nil
    expect{@a[-1]}.to raise_error(IndexError)
    @a[0] = 0
    expect(@a[0]).to eq(0)
    1.upto(3 * NODE_ENTRIES) do |i|
      @a.insert(i, i)
    end
    0.upto(3 * NODE_ENTRIES) do |i|
      expect(@a[i]).to eq(i)
    end
    expect(@a[3 * NODE_ENTRIES + 1]).to be nil
    0.upto(3 * NODE_ENTRIES) do |i|
      expect(@a[-3 * NODE_ENTRIES + i - 1]).to eq(i)
    end
    expect{@a[-3 * NODE_ENTRIES - 2]}.to raise_error(IndexError)
    (3 * NODE_ENTRIES + 1).upto(4 * NODE_ENTRIES) do |i|
      expect(@a[i]).to be nil
    end
  end

  it 'should delete elements' do
    expect(@a.delete_at(0)).to be nil
    expect(@a.length).to eq(0)
    expect(@a.check).to be true
    expect(@a.delete_at(-1)).to be nil
    expect(@a.length).to eq(0)
    expect(@a.check).to be true
    @a << 0
    expect(@a.delete_at(0)).to eql(0)
    expect(@a.length).to eq(0)
    expect(@a.check).to be true

    n = 3 * NODE_ENTRIES
    0.upto(n) { |i| @a.insert(i, i) }
    0.upto(n) do |i|
      expect(@a.delete_at(0)).to eql(i)
      expect(@a.check).to be true
    end

    0.upto(n) { |i| @a.insert(i, i) }
    n.downto(0) do |i|
      expect(@a.delete_at(-1)).to eql(i)
      expect(@a.check).to be true
    end

    n = 11 * NODE_ENTRIES
    0.upto(n - 1) { |i| @a.insert(i, i) }
    expect(@a.delete_at(n + 2)).to be nil
    expect(@a.delete_at(-(n + 2))).to be nil
    expect(@a.size).to eql(n)

    n.times do |i|
      @a.delete_at(rand(@a.size))
      expect(@a.size).to be (n - 1 - i)
      expect(@a.check).to be true
    end
    expect(@a.size).to eql(0)
  end

  it 'should iterate over all values' do
    n = 3 * NODE_ENTRIES
    0.upto(n) { |i| @a.insert(i, i) }

    i = 0
    @a.each do |v|
      expect(v).to eql(i)
      i += 1
    end
  end

  it 'should iterate over all values in reverse order' do
    n = 3 * NODE_ENTRIES
    0.upto(n) { |i| @a.insert(i, i) }

    i = 0
    @a.reverse_each do |v|
      expect(v).to eql(n - i)
      i += 1
    end
  end

  it 'should persist the data' do
    db_name = generate_db_name(__FILE__ + "_persist")
    store = PEROBS::Store.new(db_name)
    store['array'] = a = store.new(PEROBS::BigArray, NODE_ENTRIES)

    (3 * NODE_ENTRIES).times do |i|
      a.insert(i, i)
    end
    expect(a.length).to eq(3 * NODE_ENTRIES)
    store.exit

    store = PEROBS::Store.new(db_name)
    a = store['array']
    (3 * NODE_ENTRIES).times do |i|
      expect(a[i]).to eql(i)
    end
    expect(a.length).to eq(3 * NODE_ENTRIES)
    store.delete_store
  end

end
