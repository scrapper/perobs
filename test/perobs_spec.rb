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

require 'spec_helper'
require 'perobs'

class Person < PEROBS::Object

  attr_persist :name, :zip, :bmi, :married, :related, :relatives

  def initialize(store)
    super
    attr_init(:name, '')
    attr_init(:bmi, 22.2)
    attr_init(:married, false)
  end

end

describe PEROBS::Store do

  before(:all) do
    @db_name = File.join(Dir.tmpdir, "perobs_spec.#{rand(2**32)}")
  end

  after(:each) do
    FileUtils.rm_rf(@db_name)
  end

  it 'should store simple objects' do
    store = PEROBS::Store.new(@db_name)
    store['john'] = john = store.new(Person)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    store['jane'] = jane = store.new(Person)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    expect(john.name).to eq('John')
    expect(john.zip).to eq(4060)
    expect(john.bmi).to eq(25.5)
    expect(john.married).to be false
    expect(john.related).to be_nil
    jane = store['jane']
    expect(jane.name).to eq('Jane')
    expect(jane.related).to eq(john)
    expect(jane.married).to be true
  end

  it 'should store and retrieve simple objects' do
    store = PEROBS::Store.new(@db_name)
    store['john'] = john = store.new(Person)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    store['jane'] = jane = store.new(Person)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    store.exit

    store = PEROBS::Store.new(@db_name)
    john = store['john']
    expect(john.name).to eq('John')
    expect(john.zip).to eq(4060)
    expect(john.bmi).to eq(25.5)
    expect(john.married).to be false
    expect(john.related).to be_nil
    jane = store['jane']
    expect(jane.name).to eq('Jane')
    expect(jane.related).to eq(john)
    expect(jane.married).to be true
  end

  it 'should flush cached objects when necessary' do
    store = PEROBS::Store.new(@db_name, :cache_bits => 3)
    last_obj = nil
    0.upto(20) do |i|
      store["person#{i}"] = obj = store.new(Person)
      expect(store["person#{i}"]).to eq(obj)
      obj.name = "Person #{i}"
      expect(obj.name).to eq("Person #{i}")
      obj.related = last_obj
      expect(obj.related).to eq(last_obj)
      last_obj = obj
    end
    0.upto(20) do |i|
      expect(store["person#{i}"].name).to eq("Person #{i}")
    end
    store.exit
  end

  it 'should detect modification to non-working objects' do
    store = PEROBS::Store.new(@db_name, :cache_bits => 3)
    0.upto(20) do |i|
      store["person#{i}"] = obj = store.new(Person)
      obj.name = "Person #{i}"
    end
    0.upto(20) do |i|
      store["person#{i}"].name = "New Person #{i}"
    end
    store.exit
    store = PEROBS::Store.new(@db_name)
    0.upto(20) do |i|
      expect(store["person#{i}"].name).to eq("New Person #{i}")
    end
    store.exit
  end

  it 'should garbage collect unlinked objects' do
    store = PEROBS::Store.new(@db_name)
    store['person1'] = obj = store.new(Person)
    id1 = obj._id
    store['person2'] = obj = store.new(Person)
    id2 = obj._id
    obj.related = obj = store.new(Person)
    id3 = obj._id
    store.sync
    store['person1'] = nil
    store.gc
    store.exit
    store = PEROBS::Store.new(@db_name)
    expect(store.object_by_id(id1)).to be_nil
    expect(store['person2']._id).to eq(id2)
    expect(store['person2'].related._id).to eq(id3)
    store.exit
  end

end
