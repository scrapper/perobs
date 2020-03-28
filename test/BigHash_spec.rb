# encoding: UTF-8
#
# Copyright (c) 2017, 2019 by Chris Schlaeger <chris@taskjuggler.org>
#
# This file contains tests for Hash that are similar to the tests for the
# Hash implementation in MRI. The ideas of these tests were replicated in
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
require 'perobs/BigHash'

class PEROBS::BigHash

  # Redefine the hash method to make collisions very likely. This will help to
  # test the collision handling with small amounts of data.
  #def hash_key(key)
  #  key.hash & 0xF
  #end

end

ENTRIES = 200

describe PEROBS::Hash do

  before(:all) do
    @db_name = generate_db_name(__FILE__)
  end

  before(:each) do
    @store = PEROBS::Store.new(@db_name)
    @h = @store.new(PEROBS::BigHash)
    @store['hash'] = @h
  end

  after(:each) do
    @store.delete_store
  end

  it 'should support storing and retrieving an object' do
    expect(@h.check).to be true
    expect(@h.length).to eql(0)
    expect(@h.empty?).to be true
    expect(@h.keys).to eql([])
    @h['foo'] = 'bar'
    expect(@h.check).to be true
    expect(@h['foo']).to eql('bar')
    expect(@h.length).to eql(1)
    expect(@h.keys).to eql([ 'foo' ])
  end

  it 'should store a few objects' do
    20.times do |i|
      @h["bar#{i}"] = "foo#{i}"
    end
    expect(@h.size).to eql(20)
  end

  it 'should return nil for unknown objects' do
    expect(@h['bar']).to be_nil
  end

  it 'should be able to store values with hash collisions' do
    ENTRIES.times do |i|
      @h["key#{i}"] = i
    end
    expect(@h.check).to be true
    expect(@h.length).to eql(ENTRIES)

    ENTRIES.times do |i|
      expect(@h["key#{i}"]).to eql(i)
    end
  end

  it 'should replace existing entries' do
    ENTRIES.times do |i|
      @h["key#{i}"] = 2 * i
    end
    expect(@h.check).to be true
    expect(@h.length).to eql(ENTRIES)

    ENTRIES.times do |i|
      expect(@h["key#{i}"]).to eql(2 * i)
    end
    expect(@h.length).to eql(ENTRIES)
  end

  it 'should fail to delete a non-existing entry' do
    expect(@h.delete('foo')).to be_nil
    expect(@h.check).to be true
  end

  it 'should delete existing entries' do
    (1..ENTRIES).to_a.shuffle.each do |i|
      @h["key#{i}"] = 2 * i
    end
    expect(@h.check).to be true
    expect(@h.length).to eql(ENTRIES)
    (1..ENTRIES).to_a.shuffle.each do |i|
      expect(@h.delete("key#{i}")).to eql(2 * i)
    end
  end

  it 'should persist all objects' do
    db_name = generate_db_name(__FILE__ + "_persist")
    store = PEROBS::Store.new(db_name)
    h = store['hash'] = store.new(PEROBS::BigHash)
    n = ENTRIES
    n.times do |i|
      h["key#{i}"] = 2 * i
    end
    expect(h.check).to be true
    expect(h.length).to eql(n)
    store.exit

    store = PEROBS::Store.new(db_name)
    expect(store.check).to eql(0)
    h = store['hash']
    n.times do |i|
      expect(h["key#{i}"]).to eql(2 * i)
    end
    expect(h.check).to be true
    expect(h.length).to eql(n)
    store.delete_store
  end

end

