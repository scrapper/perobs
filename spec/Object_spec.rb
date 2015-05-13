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
require 'perobs'

class O1 < PEROBS::Object

  po_attr :a1

  def initialize(store)
    super
  end

end

class O2 < PEROBS::Object

  po_attr :a1, :a2, :a3, :a4

  def initialize(store)
    super
    init_attr(:a1, 'a1')
    init_attr(:a2, nil)
    init_attr(:a4, 42)
  end

end

describe PEROBS::Store do

  before(:all) do
    FileUtils.rm_rf('test_db')
  end

  after(:each) do
    FileUtils.rm_rf('test_db')
  end

  it 'should initialize attributes with default values' do
    store = PEROBS::Store.new('test_db')
    store['o1'] = o1 = O1.new(store)
    store['o2'] = o2 = O2.new(store)
    o2.a1.should == 'a1'
    o2.a2.should be_nil
    o2.a3.should be_nil
    o2.a4.should == 42
  end

  it 'should assign values to attributes' do
    store = PEROBS::Store.new('test_db')
    store['o1'] = o1 = O1.new(store)
    store['o2'] = o2 = O2.new(store)
    o1.a1 = 'a1'
    o2.a1 = nil
    o2.a3 = o1

    o1.a1.should == 'a1'
    o2.a1.should be_nil
    o2.a3.should == o1
    o2.a4.should == 42
  end

  it 'should persist assigned values' do
    store = PEROBS::Store.new('test_db')
    store['o1'] = o1 = O1.new(store)
    store['o2'] = o2 = O2.new(store)
    o1.a1 = 'a1'
    o2.a1 = nil
    o2.a3 = o1
    store.sync

    store = PEROBS::Store.new('test_db')
    o1 = store['o1']
    o2 = store['o2']
    o1.a1.should == 'a1'
    o2.a1.should be_nil
    o2.a3.should == o1
    o2.a4.should == 42
  end

end
