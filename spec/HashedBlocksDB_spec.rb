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

$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'fileutils'
require 'time'

require 'perobs/HashedBlocksDB'

class TStruct < Struct.new(:first, :second, :third)
end

describe PEROBS::HashedBlocksDB do

  before(:all) do
    FileUtils.rm_rf('fs_test')
  end

  after(:each) do
    FileUtils.rm_rf('fs_test')
  end

  it 'should create database' do
    @db = PEROBS::HashedBlocksDB.new('fs_test')
    Dir.exists?('fs_test').should be_true
  end

  it 'should support object insertion and retrieval' do
    @db = PEROBS::HashedBlocksDB.new('fs_test')
    @db.include?(0).should be_false
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
    @db.include?(0).should be_true
    @db.check(0, false).should be_true
    @db.get_object(0).should == h
  end

  it 'should support most Ruby objects types' do
    [ :marshal, :yaml ].each do |serializer|
      @db = PEROBS::HashedBlocksDB.new('fs_test', { :serializer => serializer })
      @db.include?(0).should be_false
      h = {
        'String' => 'What god has wrought',
        'Fixnum' => 42,
        'Float' => 3.14,
        'True' => true,
        'False' => false,
        'nil' => nil,
        'Array' => [ 0, 1, 2, 3 ],
        'Time' => Time.parse('2015-05-14-13:52:17'),
        'Struct' => TStruct.new("Where's", 'your', 'towel?')
      }
      @db.put_object(h, 0)
      @db.include?(0).should be_true
      @db.check(0, false).should be_true
      @db.get_object(0).should == h
      FileUtils.rm_rf('fs_test')
    end
  end

  it 'should put and get multiple objects in same dir' do
    @db = PEROBS::HashedBlocksDB.new('fs_test')
    0.upto(10) do |i|
      @db.put_object({ "foo #{i}" => i }, i)
    end
    0.upto(10) do |i|
      @db.get_object(i)["foo #{i}"].should == i
    end
  end

  it 'should handle deleted objects propery' do
    @db = PEROBS::HashedBlocksDB.new('fs_test')
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

    @db.get_object(0).should == { 'a' => 'a' * 257 }
    @db.get_object(2).should == { 'c' => 'c' * 129 }
    @db.get_object(4).should == { 'A' => 'a' * 257 }
    @db.get_object(5).should == { 'B' => 'b' * 513 }
    @db.get_object(6).should == { 'C' => 'c' * 129 }
    @db.get_object(7).should == { 'D' => 'd' * 1025 }
  end

  it 'should mark objects and detect markings' do
    @db = PEROBS::HashedBlocksDB.new('fs_test')
    h = { 'a' => 'z' }
    @db.put_object(h, 1)
    @db.put_object(h, 2)
    @db.clear_marks
    @db.is_marked?(1).should be_false
    @db.mark(1)
    @db.is_marked?(1).should be_true

    @db.include?(2).should be_true
    @db.delete_unmarked_objects
    @db.include?(1).should be_true
    @db.include?(2).should be_false
  end

end
