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

require 'time'

require 'spec_helper'
require 'perobs/BTreeDB'

describe PEROBS::BTreeDB do

  before(:all) do
    PEROBS::BTreeDB::delete_db('fs_test')
  end

  after(:each) do
    PEROBS::BTreeDB::delete_db('fs_test')
  end

  it 'should create database' do
    @db = PEROBS::BTreeDB.new('fs_test')
    expect(Dir.exists?('fs_test')).to be true
  end

  it 'should write and read a simple Hash' do
    @db = PEROBS::BTreeDB.new('fs_test')
    expect(@db.get_hash('test')).to eq({})
    h = { 'A' => 1, 'B' => 2, 'D' => 4 }
    @db.put_hash('test', h)
    expect(@db.get_hash('test')).to eq(h)
  end

  it 'should support object insertion and retrieval' do
    @db = PEROBS::BTreeDB.new('fs_test')
    expect(@db.include?(0)).to be false
    h = {
          'String' => 'What god has wrought',
          'Fixnum' => 42,
          'Float' => 3.14,
          'True' => true,
          'False' => false,
          'nil' => nil,
          'Array' => [ 0, 1, 2, 3 ]
        }
    @db.put_object(h, 0)
    expect(@db.include?(0)).to be true
    expect(@db.check(0, false)).to be true
    expect(@db.get_object(0)).to eq(h)
  end

  it 'should support most Ruby objects types' do
    [ :marshal, :yaml ].each do |serializer|
      @db = PEROBS::BTreeDB.new('fs_test', { :serializer => serializer })
      expect(@db.include?(0)).to be false
      h = {
        'String' => 'What god has wrought',
        'Fixnum' => 42,
        'Float' => 3.14,
        'True' => true,
        'False' => false,
        'nil' => nil,
        'Array' => [ 0, 1, 2, 3 ],
        'Time' => Time.parse('2015-05-14-13:52:17'),
        'Struct' => UStruct.new("Where's", 'your', 'towel?')
      }
      @db.put_object(h, 0)
      expect(@db.include?(0)).to be true
      expect(@db.check(0, false)).to be true
      expect(@db.get_object(0)).to eq(h)
      PEROBS::BTreeDB::delete_db('fs_test')
    end
  end

  it 'should put and get multiple objects in same dir' do
    @db = PEROBS::BTreeDB.new('fs_test')
    0.upto(10) do |i|
      @db.put_object({ "foo #{i}" => i }, i)
    end
    0.upto(10) do |i|
      expect(@db.get_object(i)["foo #{i}"]).to eq(i)
    end
  end

  it 'should handle deleted objects propery' do
    @db = PEROBS::BTreeDB.new('fs_test')
    @db.put_object({ 'a' => 'a' * 257 }, 0)
    @db.put_object({ 'b' => 'b' * 513 }, 1)
    @db.put_object({ 'c' => 'c' * 129 }, 2)
    @db.put_object({ 'd' => 'd' * 1025 }, 3)
    # Delete some objects
    @db.clear_marks
    @db.mark(0)
    @db.mark(2)
    @db.delete_unmarked_objects

    @db.put_object({ 'A' => 'a' * 257 }, 4)
    @db.put_object({ 'B' => 'b' * 513 }, 5)
    @db.put_object({ 'C' => 'c' * 129 }, 6)
    @db.put_object({ 'D' => 'd' * 1025 }, 7)

    expect(@db.get_object(0)).to eq({ 'a' => 'a' * 257 })
    expect(@db.get_object(2)).to eq({ 'c' => 'c' * 129 })
    expect(@db.get_object(4)).to eq({ 'A' => 'a' * 257 })
    expect(@db.get_object(5)).to eq({ 'B' => 'b' * 513 })
    expect(@db.get_object(6)).to eq({ 'C' => 'c' * 129 })
    expect(@db.get_object(7)).to eq({ 'D' => 'd' * 1025 })
  end

  it 'should mark objects and detect markings' do
    @db = PEROBS::BTreeDB.new('fs_test')
    h = { 'a' => 'z' }
    @db.put_object(h, 1)
    @db.put_object(h, 2)
    @db.clear_marks
    expect(@db.is_marked?(1)).to be false
    @db.mark(1)
    expect(@db.is_marked?(1)).to be true

    expect(@db.include?(2)).to be true
    @db.delete_unmarked_objects
    expect(@db.include?(1)).to be true
    expect(@db.include?(2)).to be false
  end

end
