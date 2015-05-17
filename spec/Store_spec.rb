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

class POSError < RuntimeError
end

class Person < PEROBS::Object

  po_attr :name, :zip, :bmi, :married, :related, :relatives

  def initialize(store)
    super
    init_attr(:name, '')
    init_attr(:bmi, 22.2)
    init_attr(:married, false)
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
    store = PEROBS::Store.new('test_db', { :serializer => :yaml })
    store['john'] = john = Person.new(store)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    store['jane'] = jane = Person.new(store)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    john.name.should == 'John'
    john.zip.should == 4060
    john.bmi.should == 25.5
    john.married.should be_false
    john.related.should be_nil
    jane = store['jane']
    jane.name.should == 'Jane'
    jane.related.should == john
    jane.married.should be_true
  end

  it 'should store and retrieve simple objects' do
    [ :marshal, :json, :yaml ].each do |serializer|
      store = PEROBS::Store.new('test_db', { :serializer => serializer })
      store['john'] = john = Person.new(store)
      john.name = 'John'
      john.zip = 4060
      john.bmi = 25.5
      store['jane'] = jane = Person.new(store)
      jane.name = 'Jane'
      jane.related = john
      jane.married = true
      jane.relatives = 'test'

      store.sync

      store = PEROBS::Store.new('test_db', { :serializer => serializer })
      john = store['john']
      john.name.should == 'John'
      john.zip.should == 4060
      john.bmi.should == 25.5
      john.married.should be_false
      john.related.should be_nil
      jane = store['jane']
      jane.name.should == 'Jane'
      jane.related.should == john
      jane.married.should be_true
    end
  end

  it 'should flush cached objects when necessary' do
    store = PEROBS::Store.new('test_db', :cache_bits => 3)
    last_obj = nil
    0.upto(20) do |i|
      store["person#{i}"] = obj = Person.new(store)
      store["person#{i}"].should == obj
      obj.name = "Person #{i}"
      obj.name.should == "Person #{i}"
      obj.related = last_obj
      obj.related.should == last_obj
      last_obj = obj
    end
    0.upto(20) do |i|
      store["person#{i}"].name.should == "Person #{i}"
    end
  end

  it 'should detect modification to non-working objects' do
    store = PEROBS::Store.new('test_db', :cache_bits => 3)
    0.upto(20) do |i|
      store["person#{i}"] = obj = Person.new(store)
      obj.name = "Person #{i}"
    end
    0.upto(20) do |i|
      store["person#{i}"].name = "New Person #{i}"
    end
    store.sync
    store = PEROBS::Store.new('test_db')
    0.upto(20) do |i|
      store["person#{i}"].name.should == "New Person #{i}"
    end
  end

  it 'should garbage collect unlinked objects' do
    store = PEROBS::Store.new('test_db')
    store['person1'] = obj = Person.new(store)
    id1 = obj._id
    store['person2'] = obj = Person.new(store)
    id2 = obj._id
    obj.related = obj = Person.new(store)
    id3 = obj._id
    store.sync
    store['person1'] = nil
    store.gc
    store = PEROBS::Store.new('test_db')
    store.object_by_id(id1).should be_nil
    store['person2']._id.should == id2
    store['person2'].related._id.should == id3
  end

  it 'should handle cyclicly linked objects' do
    store = PEROBS::Store.new('test_db')
    store['person0'] = p0 = Person.new(store)
    id0 = p0._id
    p1 = Person.new(store)
    id1 = p1._id
    p2 = Person.new(store)
    id2 = p2._id
    p1.related = p2
    p2.related = p1
    p0.related = p1
    store.sync
    store.gc
    store = PEROBS::Store.new('test_db')
    store['person0']._id.should == id0
    store['person0'].related._id.should == id1
    store['person0'].related.related._id.should == id2

    store['person0'].related = nil
    store.gc
    store.object_by_id(id1).should be_nil
    store.object_by_id(id2).should be_nil

    store = PEROBS::Store.new('test_db')
    store.object_by_id(id1).should be_nil
    store.object_by_id(id2).should be_nil
  end

  it 'should support a successful transaction' do
    store = PEROBS::Store.new('test_db')
    store.transaction do
      store['person0'] = p0 = Person.new(store)
      p0.name = 'Jimmy'
    end
    store['person0'].name.should == 'Jimmy'
  end

  it 'should handle a failed transaction 1' do
    store = PEROBS::Store.new('test_db')
    begin
      store.transaction do
        store['person0'] = p0 = Person.new(store)
        p0.name = 'Jimmy'
        raise POSError
      end
    rescue POSError
    end
    store['person0'].should be_nil
  end

  it 'should handle a failed transaction 2' do
    store = PEROBS::Store.new('test_db')
    store['person1'] = p1 = Person.new(store)
    p1.name = 'Joe'
    begin
      store.transaction do
        store['person0'] = p0 = Person.new(store)
        p0.name = 'Jimmy'
        raise POSError
      end
    rescue POSError
    end
    store['person1'].name.should == 'Joe'
    store['person0'].should be_nil
  end

  it 'should support a successful nested transaction' do
    store = PEROBS::Store.new('test_db')
    store.transaction do
      store['person0'] = p0 = Person.new(store)
      p0.name = 'Jimmy'
      store.transaction do
        store['person1'] = p1 = Person.new(store)
        p1.name = 'Joe'
      end
    end
    store['person0'].name.should == 'Jimmy'
    store['person1'].name.should == 'Joe'
  end

  it 'should handle a failed nested transaction 1' do
    store = PEROBS::Store.new('test_db')
    begin
      store.transaction do
        store['person0'] = p0 = Person.new(store)
        p0.name = 'Jimmy'
        begin
          store.transaction do
            store['person1'] = p1 = Person.new(store)
            p1.name = 'Joe'
            raise POSError
          end
        rescue POSError
        end
      end
    rescue POSError
    end
    store['person0'].name.should == 'Jimmy'
    store['person1'].should be_nil
  end

  it 'should handle a failed nested transaction 2' do
    store = PEROBS::Store.new('test_db')
    begin
      store.transaction do
        store['person0'] = p0 = Person.new(store)
        p0.name = 'Jimmy'
        store.transaction do
          store['person1'] = p1 = Person.new(store)
          p1.name = 'Joe'
        end
        raise POSError
      end
    rescue POSError
    end
    store['person0'].should be_nil
    store['person1'].should be_nil
  end

  it 'should support a successful 2-level nested transaction' do
    store = PEROBS::Store.new('test_db')
    store.transaction do
      store['person0'] = p0 = Person.new(store)
      p0.name = 'Jimmy'
      store.transaction do
        store['person1'] = p1 = Person.new(store)
        p1.name = 'Joe'
        store.transaction do
          store['person2'] = p2 = Person.new(store)
          p2.name = 'Jane'
        end
      end
    end
    store['person0'].name.should == 'Jimmy'
    store['person1'].name.should == 'Joe'
    store['person2'].name.should == 'Jane'
  end

  it 'should handle a failed 2-level nested transaction 1' do
    store = PEROBS::Store.new('test_db')
    store.transaction do
      store['person0'] = p0 = Person.new(store)
      p0.name = 'Jimmy'
      store.transaction do
        store['person1'] = p1 = Person.new(store)
        p1.name = 'Joe'
        begin
          store.transaction do
            store['person2'] = p2 = Person.new(store)
            p2.name = 'Jane'
            raise POSError
          end
        rescue POSError
        end
      end
    end
    store['person0'].name.should == 'Jimmy'
    store['person1'].name.should == 'Joe'
    store['person2'].should be_nil
  end

  it 'should handle a failed 2-level nested transaction 2' do
    store = PEROBS::Store.new('test_db')
    store.transaction do
      store['person0'] = p0 = Person.new(store)
      p0.name = 'Jimmy'
      store.transaction do
        store['person1'] = p1 = Person.new(store)
        p1.name = 'Joe'
        begin
          store.transaction do
            store['person2'] = p2 = Person.new(store)
            p2.name = 'Jane'
            raise POSError
          end
        rescue POSError
        end
        p1.name = 'Jane'
      end
    end
    store['person0'].name.should == 'Jimmy'
    store['person1'].name.should == 'Jane'
    store['person2'].should be_nil
  end

end
