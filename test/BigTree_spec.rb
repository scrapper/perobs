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

  before(:all) do
    @db_name = generate_db_name(__FILE__)
    @store = PEROBS::Store.new(@db_name)
    @t = @store.new(PEROBS::BigTree, 15)
  end

  after(:all) do
    @store.delete_store
  end

  it 'should support adding sequential key/value pairs' do
    0.upto(100) do |i|
      @t.insert(i, 3 * i)
      expect(@t.check).to be true
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
    expect(i).to eql(101)
  end

  it 'should yield the key/value pairs on check' do
    i = 0
    @t.check do |k, v|
      expect(k).to eql(k)
      expect(v).to eql(3 * k)
      i += 1
    end
    expect(i).to eql(101)
  end

  it 'should support clearing the tree' do
    @t.clear
    expect(@t.check).to be true
    i = 0
    @t.each { |k, v| i += 1 }
    expect(i).to eql(0)
  end

  it 'should support adding random key/value pairs' do
    (1..1000).to_a.shuffle.each do |i|
      @t.insert(i, i * 100)
      expect(@t.check).to be true
    end
    (1..1000).to_a.shuffle.each do |i|
      expect(@t.get(i)).to eql(i * 100)
    end
  end

  it 'should support removing keys' do
    @t.clear
    @t.insert(1, 1)
    expect(@t.remove(1)).to eql(1)
    expect(@t.check).to be true
    expect(@t.length).to eql(0)

    (1..100).to_a.shuffle.each do |i|
      @t.insert(i, i * 100)
    end
    (1..100).to_a.shuffle.each do |i|
      expect(@t.remove(i)).to eql(i * 100)
      expect(@t.check).to be true
    end
  end

  it 'should survive a real-world usage test' do
    @t.clear
    ref = {}
    0.upto(5000) do
      case rand(4)
      when 0
        0.upto(2) do
          key = rand(100000)
          value = rand(10000000)
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
          value = rand(10000000)
          @t.insert(key, value)
          ref[key] = value
        end
      when 4
        if rand(50) == 0
          expect(@t.check).to be true
        end
      end
    end
  end

end
