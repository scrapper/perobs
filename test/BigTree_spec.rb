# encoding: UTF-8
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

require 'fileutils'

require 'spec_helper'
require 'perobs/Store'
require 'perobs/BigTree'

describe PEROBS::BigTree do

  ORDER = 7

  before(:all) do
    @db_name = generate_db_name(__FILE__)
    @store = PEROBS::Store.new(@db_name)
    @t = @store.new(PEROBS::BigTree, ORDER)
  end

  after(:all) do
    @store.delete_store
  end

  it 'should be empty' do
    expect(@t.empty?).to be true
    expect(@t.length).to eql(0)
    s = @t.statistics
    expect(s.leaf_nodes).to eql(1)
    expect(s.branch_nodes).to eql(0)
    expect(s.min_depth).to eql(1)
    expect(s.max_depth).to eql(1)
  end

  it 'should deal with requests for unknown keys' do
    expect(@t.has_key?(42)).to be false
    expect(@t.get(42)).to be_nil
  end

  it 'should support adding sequential key/value pairs' do
    0.upto(ORDER ** 3) do |i|
      @t.insert(i, 3 * i)
      expect(@t.check).to be true
      expect(@t.length).to eql(i + 1)
      expect(@t.has_key?(i)).to be true
      expect(@t.get(i)).to eql(3 * i)
    end
  end

  it 'should iterate over the stored key and value pairs' do
    i = 0
    @t.each do |k, v|
      expect(k).to eql(i)
      expect(v).to eql(3 * i)
      i += 1
    end
    expect(i).to eql(ORDER ** 3 + 1)
  end

  it 'should iterate over segments of the stored key and value pairs' do
    [ 0, 6, 7, 11, 23 ].each do |start_key|
      i = start_key
      @t.each(start_key, 11) do |k, v|
        expect(k).to eql(i)
        expect(v).to eql(3 * i)
        i += 1
      end
      expect(i).to eql(start_key + 11)
    end
  end

  it 'should iterate in reverse order over the stored key and value pairs' do
    i = ORDER ** 3
    @t.reverse_each do |k, v|
      expect(k).to eql(i)
      expect(v).to eql(3 * i)
      i -= 1
    end
    expect(i).to eql(-1)
  end

  it 'should yield the key/value pairs on check' do
    i = 0
    @t.check do |k, v|
      expect(k).to eql(k)
      expect(v).to eql(3 * k)
      i += 1
    end
    expect(i).to eql(ORDER ** 3 + 1)
  end

  it 'should support overwriting existing entries' do
    0.upto(ORDER ** 3) do |i|
      @t.insert(i, 7 * i)
      expect(@t.check).to be true
      expect(@t.length).to eql(ORDER ** 3 + 1)
      expect(@t.has_key?(i)).to be true
      expect(@t.get(i)).to eql(7 * i)
    end
  end

  it 'should support clearing the tree' do
    @t.clear
    expect(@t.check).to be true
    expect(@t.empty?).to be true
    expect(@t.length).to eql(0)
    i = 0
    @t.each { |k, v| i += 1 }
    expect(i).to eql(0)
  end

  it 'should support adding random key/value pairs' do
    (1..ORDER ** 3).to_a.shuffle.each do |i|
      @t.insert(i, i * 100)
    end
    expect(@t.check).to be true
    (1..ORDER ** 3).to_a.shuffle.each do |i|
      expect(@t.get(i)).to eql(i * 100)
    end
  end

  it 'should support removing keys in random order' do
    @t.clear
    (1..ORDER ** 3).to_a.shuffle.each do |i|
      @t.insert(i, i * 100)
    end
    expect(@t.length).to eql(ORDER ** 3)
    (1..ORDER ** 3).to_a.shuffle.each do |i|
      expect(@t.remove(i)).to eql(i * 100)
      expect(@t.check).to be true
    end
    expect(@t.length).to eql(0)
  end

  it 'should support removing keys in increasing order' do
    @t.clear
    (1..ORDER ** 3).to_a.shuffle.each do |i|
      @t.insert(i, i * 100)
    end
    expect(@t.length).to eql(ORDER ** 3)
    (1..ORDER ** 3).to_a.each do |i|
      expect(@t.remove(i)).to eql(i * 100)
      expect(@t.check).to be true
    end
    expect(@t.length).to eql(0)
  end

  it 'should support removing keys in reverse order' do
    @t.clear
    (1..ORDER ** 3).to_a.shuffle.each do |i|
      @t.insert(i, i * 100)
    end
    expect(@t.length).to eql(ORDER ** 3)
    (1..ORDER ** 3).to_a.reverse_each do |i|
      expect(@t.remove(i)).to eql(i * 100)
      expect(@t.check).to be true
    end
    expect(@t.length).to eql(0)
  end

  it 'should persist the data' do
    db_name = generate_db_name(__FILE__ + '_persist')
    store = PEROBS::Store.new(db_name)
    store['bigtree'] = t = store.new(PEROBS::BigTree, 4)
    10.times do |i|
      t.insert(i, i)
    end
    10.times do |i|
      expect(t.get(i)).to eql(i)
    end
    store.exit

    store = PEROBS::Store.new(db_name)
    t = store['bigtree']
    10.times do |i|
      expect(t.get(i)).to eql(i)
    end
    store.delete_store
  end

  it 'should delete all entries matching a condition' do
    @t.clear
    (1..50).to_a.shuffle.each do |i|
      @t.insert(i, i)
    end
    @t.delete_if { |k, v| v % 7 == 0 }
    expect(@t.check).to be true
    @t.each do |k, v|
      expect(v % 7).to be > 0, "failed for #{v}"
    end
    expect(@t.length).to eql(43)
    @t.delete_if { |k, v| v % 2 == 0 }
    expect(@t.check).to be true
    @t.each do |k, v|
      expect(v % 2).to be > 0
    end
    expect(@t.length).to eql(21)
    @t.delete_if { |k, v| true }
    expect(@t.check).to be true
    expect(@t.empty?).to be true
  end

  it 'should survive a real-world usage test' do
    @t.clear
    ref = {}
    0.upto(1000) do
      case rand(5)
      when 0
        0.upto(2) do
          key = rand(100000)
          value = key * 10
          @t.insert(key, value)
          ref[key] = value
        end
      when 1
        if ref.length > 0
          key = ref.keys[rand(ref.keys.length)]
          expect(@t.remove(key)).to eql(ref[key])
          ref.delete(key)
        end
      when 2
        if ref.length > 0
          0.upto(3) do
            key = ref.keys[rand(ref.keys.length)]
            expect(@t.get(key)).to eql(ref[key])
          end
        end
      when 3
        if ref.length > 0
          key = ref.keys[rand(ref.keys.length)]
          value = ref[key] + 1
          @t.insert(key, value)
          ref[key] = value
        end
      when 4
        if rand(50) == 0
          expect(@t.check).to be true
        end
      end
    end

    i = 0
    @t.each do |k, v|
      expect(ref[k]).to eql(v)
      i += 1
    end
    expect(i).to eql(ref.length)
  end

end
