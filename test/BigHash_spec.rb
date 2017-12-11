# encoding: UTF-8
#
# Copyright (c) 2017 by Chris Schlaeger <chris@taskjuggler.org>
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
  def hash_key(key)
    key.hash & 0xF
  end

end

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

  it 'should support storing and retriebing an object' do
    @h['foo'] = 'bar'
    expect(@h['foo']).to eql('bar')
  end

  it 'should return nil for unknown objects' do
    expect(@h['bar']).to be_nil
  end

  it 'should be able to store values with hash collisions' do
    20.times do |i|
      @h["key#{i}"] = i
    end

    20.times do |i|
      expect(@h["key#{i}"]).to eql(i)
    end
  end

  it 'should replace existing entries' do
    20.times do |i|
      @h["key#{i}"] = 2 * i
    end

    20.times do |i|
      expect(@h["key#{i}"]).to eql(2 * i)
    end
  end

end
