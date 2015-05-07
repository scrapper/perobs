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

class Person < PEROBS::Object

  po_attr :name, ''
  po_attr :zip
  po_attr :bmi, 22.2
  po_attr :married, false
  po_attr :related
  po_attr :relatives

  def initialize(store)
    super
  end

end

describe PEROBS::Store do

  before(:all) do
    FileUtils.rm_rf('test_db')
  end

  after(:each) do
    FileUtils.rm_rf('test_db')
  end

  it 'should store simple objects' do
    store = PEROBS::Store.new('test_db')
    store[:john] = john = Person.new(store)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    store[:jane] = jane = Person.new(store)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    john.name.should == 'John'
    john.zip.should == 4060
    john.bmi.should == 25.5
    john.married.should be_false
    john.related.should be_nil
    jane = store[:jane]
    jane.name.should == 'Jane'
    jane.related.should == john
    jane.married.should be_true
  end

  it 'should store and retrieve simple objects' do
    store = PEROBS::Store.new('test_db')
    store[:john] = john = Person.new(store)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    store[:jane] = jane = Person.new(store)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    store.sync

    store = PEROBS::Store.new('test_db')
    john = store[:john]
    john.name.should == 'John'
    john.zip.should == 4060
    john.bmi.should == 25.5
    john.married.should be_false
    john.related.should be_nil
    jane = store[:jane]
    jane.name.should == 'Jane'
    jane.related.should == john
    jane.married.should be_true
  end

  it 'should flush cached objects when necessary' do
    store = PEROBS::Store.new('test_db', :cache_bits => 3)
    last_obj = nil
    0.upto(20) do |i|
      store[":person#{i}".to_sym] = obj = Person.new(store)
      obj.name = "Person #{i}"
      obj.related = last_obj if last_obj
      last_obj = obj
    end
    0.upto(20) do |i|
      store[":person#{i}".to_sym].name.should == "Person #{i}"
    end
  end

  it 'should detect modification to non-working objects' do
    store = PEROBS::Store.new('test_db', :cache_bits => 3)
    0.upto(20) do |i|
      store[":person#{i}".to_sym] = obj = Person.new(store)
      obj.name = "Person #{i}"
    end
    0.upto(20) do |i|
      store[":person#{i}".to_sym].name = "New Person #{i}"
    end
    store.sync
    store = PEROBS::Store.new('test_db')
    0.upto(20) do |i|
      store[":person#{i}".to_sym].name.should == "New Person #{i}"
    end
  end

  it 'should garbage collect unlinked objects' do
    store = PEROBS::Store.new('test_db')
    store[:person1] = obj = Person.new(store)
    id1 = obj.id
    store[:person2] = obj = Person.new(store)
    id2 = obj.id
    obj.related = obj = Person.new(store)
    id3 = obj.id
    store.sync
    store[:person1] = nil
    store.gc
    store = PEROBS::Store.new('test_db')
    store.object_by_id(id1).should be_nil
    store[:person2].id.should == id2
    store[:person2].related.id.should == id3
  end

end
