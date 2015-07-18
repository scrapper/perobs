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

require 'perobs/ClassMap'
require 'perobs/BTreeDB'

describe PEROBS::ClassMap do

  before(:all) do
    FileUtils.rm_rf('fs_test')
    @db = PEROBS::BTreeDB.new('fs_test')
    @map = PEROBS::ClassMap.new(@db)
  end

  after(:all) do
    FileUtils.rm_rf('fs_test')
  end

  it 'should return nil for an unknown ID' do
    @map.id_to_class(0).should be_nil
  end

  it 'should add a class' do
    @map.class_to_id('Foo').should == 0
  end

  it 'should find the class again' do
    @map.id_to_class(0).should == 'Foo'
  end

  it 'should still return nil for an unknown ID' do
    @map.id_to_class(1).should be_nil
  end

end
