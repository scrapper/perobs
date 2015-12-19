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

class PersonN < PEROBS::Object

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
    @db_file = generate_db_name(__FILE__)
  end

  after(:each) do
    @store.gc
    expect { @store.check }.to_not raise_error
    expect { @store.delete_store }.to_not raise_error
  end

  after(:all) do
    FileUtils.rm_rf(@db_file)
  end

  it 'should @store simple objects' do
    @store = PEROBS::Store.new(@db_file, { :serializer => :yaml })
    @store['john'] = john = @store.new(Person)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    @store['jane'] = jane = @store.new(Person)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    expect(john.name).to eq('John')
    expect(john.zip).to eq(4060)
    expect(john.bmi).to eq(25.5)
    expect(john.married).to be false
    expect(john.related).to be_nil
    jane = @store['jane']
    expect(jane.name).to eq('Jane')
    expect(jane.related).to eq(john)
    expect(jane.married).to be true
  end

  it 'should @store and retrieve simple objects' do
    [ :marshal, :json, :yaml ].each do |serializer|
      FileUtils.rm_rf(@db_file)
      @store = PEROBS::Store.new(@db_file, { :serializer => serializer })
      @store['john'] = john = @store.new(Person)
      john.name = 'John'
      john.zip = 4060
      john.bmi = 25.5
      @store['jane'] = jane = @store.new(Person)
      jane.name = 'Jane'
      jane.related = john
      jane.married = true
      jane.relatives = 'test'

      @store.sync

      @store = PEROBS::Store.new(@db_file)
      john = @store['john']
      expect(john.name).to eq('John')
      expect(john.zip).to eq(4060)
      expect(john.bmi).to eq(25.5)
      expect(john.married).to be false
      expect(john.related).to be_nil
      jane = @store['jane']
      expect(jane.name).to eq('Jane')
      expect(jane.related).to eq(john)
      expect(jane.married).to be true
    end
  end

  it 'should not allow calls to BasicObject.new()' do
    @store = PEROBS::Store.new(@db_file)
    expect { Person.new(@store) }.to raise_error RuntimeError
  end

  it 'should flush cached objects when necessary' do
    @store = PEROBS::Store.new(@db_file, :cache_bits => 3)
    last_obj = nil
    0.upto(20) do |i|
      @store["person#{i}"] = obj = @store.new(Person)
      expect(@store["person#{i}"]).to eq(obj)
      obj.name = "Person #{i}"
      expect(obj.name).to eq("Person #{i}")
      obj.related = last_obj
      expect(obj.related).to eq(last_obj)
      last_obj = obj
    end
    0.upto(20) do |i|
      expect(@store["person#{i}"].name).to eq("Person #{i}")
    end
  end

  it 'should support renaming of classes' do
    @store = PEROBS::Store.new(@db_file)
    @store['john'] = john = @store.new(Person)
    john.name = 'John'
    john.zip = 4060
    john.bmi = 25.5
    @store['jane'] = jane = @store.new(Person)
    jane.name = 'Jane'
    jane.related = john
    jane.married = true
    jane.relatives = 'test'

    @store.sync

    @store = PEROBS::Store.new(@db_file)
    @store.rename_classes({ 'Person' => 'PersonN' })
    john = @store['john']
    expect(john.name).to eq('John')
    expect(john.zip).to eq(4060)
    expect(john.bmi).to eq(25.5)
    expect(john.married).to be false
    expect(john.related).to be_nil
    jane = @store['jane']
    expect(jane.name).to eq('Jane')
    expect(jane.related).to eq(john)
    expect(jane.married).to be true
  end

  it 'should detect modification to non-working objects' do
    @store = PEROBS::Store.new(@db_file, :cache_bits => 3)
    0.upto(20) do |i|
      @store["person#{i}"] = obj = @store.new(Person)
      obj.name = "Person #{i}"
    end
    0.upto(20) do |i|
      @store["person#{i}"].name = "New Person #{i}"
    end
    @store.sync
    @store = PEROBS::Store.new(@db_file)
    0.upto(20) do |i|
      expect(@store["person#{i}"].name).to eq("New Person #{i}")
    end
  end

  it 'should garbage collect unlinked objects' do
    @store = PEROBS::Store.new(@db_file)
    @store['person1'] = obj = @store.new(Person)
    id1 = obj._id
    @store['person2'] = obj = @store.new(Person)
    id2 = obj._id
    obj.related = obj = @store.new(Person)
    id3 = obj._id
    @store.sync
    @store['person1'] = nil
    @store.gc
    @store = PEROBS::Store.new(@db_file)
    expect(@store.object_by_id(id1)).to be_nil
    expect(@store['person2']._id).to eq(id2)
    expect(@store['person2'].related._id).to eq(id3)
  end

  it 'should handle cyclicly linked objects' do
    @store = PEROBS::Store.new(@db_file)
    @store['person0'] = p0 = @store.new(Person)
    id0 = p0._id
    p1 = @store.new(Person)
    id1 = p1._id
    p2 = @store.new(Person)
    id2 = p2._id
    p1.related = p2
    p2.related = p1
    p0.related = p1
    @store.sync
    @store.gc
    @store = PEROBS::Store.new(@db_file)
    expect(@store['person0']._id).to eq(id0)
    expect(@store['person0'].related._id).to eq(id1)
    expect(@store['person0'].related.related._id).to eq(id2)

    @store['person0'].related = nil
    @store.gc
    expect(@store.object_by_id(id1)).to be_nil
    expect(@store.object_by_id(id2)).to be_nil

    @store = PEROBS::Store.new(@db_file)
    expect(@store.object_by_id(id1)).to be_nil
    expect(@store.object_by_id(id2)).to be_nil
  end

  it 'should support a successful transaction' do
    @store = PEROBS::Store.new(@db_file)
    @store.transaction do
      @store['person0'] = p0 = @store.new(Person)
      p0.name = 'Jimmy'
    end
    expect(@store['person0'].name).to eq('Jimmy')
  end

  it 'should handle a failed transaction 1' do
    @store = PEROBS::Store.new(@db_file)
    begin
      @store.transaction do
        @store['person0'] = p0 = @store.new(Person)
        p0.name = 'Jimmy'
        raise POSError
      end
    rescue POSError
    end
    expect(@store['person0']).to be_nil
  end

  it 'should handle a failed transaction 2' do
    @store = PEROBS::Store.new(@db_file)
    @store['person1'] = p1 = @store.new(Person)
    p1.name = 'Joe'
    begin
      @store.transaction do
        @store['person0'] = p0 = @store.new(Person)
        p0.name = 'Jimmy'
        raise POSError
      end
    rescue POSError
    end
    expect(@store['person1'].name).to eq('Joe')
    expect(@store['person0']).to be_nil
  end

  it 'should support a successful nested transaction' do
    @store = PEROBS::Store.new(@db_file)
    @store.transaction do
      @store['person0'] = p0 = @store.new(Person)
      p0.name = 'Jimmy'
      @store.transaction do
        @store['person1'] = p1 = @store.new(Person)
        p1.name = 'Joe'
      end
    end
    expect(@store['person0'].name).to eq('Jimmy')
    expect(@store['person1'].name).to eq('Joe')
  end

  it 'should handle a failed nested transaction 1' do
    @store = PEROBS::Store.new(@db_file)
    begin
      @store.transaction do
        @store['person0'] = p0 = @store.new(Person)
        p0.name = 'Jimmy'
        begin
          @store.transaction do
            @store['person1'] = p1 = @store.new(Person)
            p1.name = 'Joe'
            raise POSError
          end
        rescue POSError
        end
      end
    rescue POSError
    end
    expect(@store['person0'].name).to eq('Jimmy')
    expect(@store['person1']).to be_nil
  end

  it 'should handle a failed nested transaction 2' do
    @store = PEROBS::Store.new(@db_file)
    begin
      @store.transaction do
        @store['person0'] = p0 = @store.new(Person)
        p0.name = 'Jimmy'
        @store.transaction do
          @store['person1'] = p1 = @store.new(Person)
          p1.name = 'Joe'
        end
        raise POSError
      end
    rescue POSError
    end
    expect(@store['person0']).to be_nil
    expect(@store['person1']).to be_nil
  end

  it 'should support a successful 2-level nested transaction' do
    @store = PEROBS::Store.new(@db_file)
    @store.transaction do
      @store['person0'] = p0 = @store.new(Person)
      p0.name = 'Jimmy'
      @store.transaction do
        @store['person1'] = p1 = @store.new(Person)
        p1.name = 'Joe'
        @store.transaction do
          @store['person2'] = p2 = @store.new(Person)
          p2.name = 'Jane'
        end
      end
    end
    expect(@store['person0'].name).to eq('Jimmy')
    expect(@store['person1'].name).to eq('Joe')
    expect(@store['person2'].name).to eq('Jane')
  end

  it 'should handle a failed 2-level nested transaction 1' do
    @store = PEROBS::Store.new(@db_file)
    @store.transaction do
      @store['person0'] = p0 = @store.new(Person)
      p0.name = 'Jimmy'
      @store.transaction do
        @store['person1'] = p1 = @store.new(Person)
        p1.name = 'Joe'
        begin
          @store.transaction do
            @store['person2'] = p2 = @store.new(Person)
            p2.name = 'Jane'
            raise POSError
          end
        rescue POSError
        end
      end
    end
    expect(@store['person0'].name).to eq('Jimmy')
    expect(@store['person1'].name).to eq('Joe')
    expect(@store['person2']).to be_nil
  end

  it 'should handle a failed 2-level nested transaction 2' do
    @store = PEROBS::Store.new(@db_file)
    @store.transaction do
      @store['person0'] = p0 = @store.new(Person)
      p0.name = 'Jimmy'
      @store.transaction do
        @store['person1'] = p1 = @store.new(Person)
        p1.name = 'Joe'
        begin
          @store.transaction do
            @store['person2'] = p2 = @store.new(Person)
            p2.name = 'Jane'
            raise POSError
          end
        rescue POSError
        end
        p1.name = 'Jane'
      end
    end
    expect(@store['person0'].name).to eq('Jimmy')
    expect(@store['person1'].name).to eq('Jane')
    expect(@store['person2']).to be_nil
  end

  it 'should survive a real world usage test' do
    options = { :engine => PEROBS::BTreeDB, :dir_bits => 4 }
    @store = PEROBS::Store.new(@db_file, options)
    ref = {}

    0.upto(2000) do |i|
      key = "o#{i}"
      case i % 8
      when 0
        value = 'A' * rand(512)
        @store[key] = p = @store.new(Person)
        p.name = value
        ref[key] = value
        @store.sync
      when 1
        value = 'B' * rand(128)
        @store[key] = p = @store.new(Person)
        p.name = value
        ref[key] = value
      when 2
        index = i - rand(20)
        if index >= 0
          key = "o#{i - rand(20)}"
          @store[key] = nil
          ref.delete(key)
        end
      when 3
        @store.gc if rand(30) == 0
      when 4
        if rand(15) == 0
          @store.sync
          @store = PEROBS::Store.new(@db_file, options)
        end
      when 5
        index = i - rand(10)
        if rand(3) == 0 && index >= 0
          key = "o#{i - rand(10)}"
          value = 'C' * rand(1024)
          @store[key] = p = @store.new(Person)
          p.name = value
          ref[key] = value
        end
      when 6
        if rand(50) == 0
          @store.sync
          @store.check(false)
        end
      when 7
        index = rand(i)
        if ref[key]
          expect(@store[key].name).to eq(ref[key])
        end
      end

      if ref[key]
        expect(@store[key].name).to eq(ref[key])
      end
    end

    ref.each do |k, v|
      expect(@store[k].name).to eq(v)
    end
  end

end
