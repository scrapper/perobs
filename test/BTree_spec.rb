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
require 'perobs/BTree'

describe PEROBS::BTree do

  before(:all) do
    @db_dir = generate_db_name('BTree')
    FileUtils.mkdir_p(@db_dir)
    @m = PEROBS::BTree.new(@db_dir, 'btree', 11)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should gracefully handle calling close when not open' do
    @m.close
  end

  it 'should open the BTree' do
    @m.open
    expect(@m.to_s).to eql("o--- @1\n")
    #expect(@m.to_a).to eql([])
  end

  it 'should support adding sequential key/value pairs' do
    0.upto(100) do |i|
      @m.insert(i, 2 * i)
      expect(@m.check).to be true
      expect(@m.get(i)).to eql(2 * i)
    end
  end

  it 'should persist the data' do
    @m.close
    @m.open
    expect(@m.check).to be true
    0.upto(100) do |i|
      expect(@m.get(i)).to eql(2 * i)
    end
  end

  it 'should iterate over the stored key and value pairs' do
    i = 0
    @m.each do |k, v|
      expect(k).to eql(i)
      expect(v).to eql(2 * i)
      i += 1
    end
    expect(i).to eql(101)
  end

  it 'should yield the key/value pairs on check' do
    i = 0
    @m.check do |k, v|
      expect(k).to eql(k)
      expect(v).to eql(2 * k)
      i += 1
    end
    expect(i).to eql(101)
  end

  it 'should support clearing the tree' do
    @m.clear
    expect(@m.check).to be true
  end

  it 'should support erasing the backing store' do
    @m.close
    @m.erase
    @m.open
    expect(@m.check).to be true
  end

  it 'should support adding random key/value pairs' do
    (1..1000).to_a.shuffle.each do |i|
      @m.insert(i, i * 100)
      expect(@m.check).to be true
    end
    (1..1000).to_a.shuffle.each do |i|
      expect(@m.get(i)).to eql(i * 100)
    end
  end

  it 'should support removing keys' do
    @m.clear
    @m.insert(1, 1)
    expect(@m.remove(1)).to eql(1)
    expect(@m.check).to be true
    expect(@m.to_s).to eql("o--- @1\n")

    (1..100).to_a.shuffle.each do |i|
      @m.insert(i, i * 100)
    end
    (1..100).to_a.shuffle.each do |i|
      expect(@m.remove(i)).to eql(i * 100)
      expect(@m.check).to be true
    end
  end

  it 'should survive a real-world usage test' do
    @m.clear
    ref = {}
    0.upto(20000) do
      case rand(5)
      when 0
        0.upto(2) do
          key = rand(100000)
          value = rand(10000000)
          @m.insert(key, value)
          ref[key] = value
        end
      when 1
        if ref.length > 0
          key = ref.keys[rand(ref.keys.length)]
          expect(@m.remove(key)).to eql(ref[key])
          ref.delete(key)
        end
      when 2
        if ref.length > 0
          0.upto(3) do
            key = ref.keys[rand(ref.keys.length)]
            expect(@m.get(key)).to eql(ref[key])
          end
        end
      when 3
        if ref.length > 0
          key = ref.keys[rand(ref.keys.length)]
          value = rand(10000000)
          @m.insert(key, value)
          ref[key] = value
        end
      when 4
        if rand(50) == 0
          expect(@m.check).to be true
        end
      when 5
        if rand(50) == 0
          @m.close
          @m.open
        end
      end
    end
  end

end
