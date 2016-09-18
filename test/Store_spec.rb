# encoding: UTF-8
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
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
    attr_init(:name, '')
    attr_init(:bmi, 22.2)
    attr_init(:married, false)
  end

end

class PersonN < PEROBS::Object

  po_attr :name, :zip, :bmi, :married, :related, :relatives

  def initialize(store)
    super
    attr_init(:name, '')
    attr_init(:bmi, 22.2)
    attr_init(:married, false)
  end

end

class O0 < PEROBS::Object

  po_attr :child

  def initialize(store)
    super
    self.child = @store.new(O1, myself)
  end

end
class O1 < PEROBS::Object

  po_attr :parent

  def initialize(store, p = nil)
    super(store)
    self.parent = p
  end

end

describe PEROBS::Store do

  before(:all) do
    @db_file = generate_db_name(__FILE__)
    @db_file_new = @db_file + '-new'
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
    expect(@store.check).to eq(0)
    expect(@store.gc).to eq(0)
    p0 = p1 = p2 = nil
    GC.start
    @store = PEROBS::Store.new(@db_file)
    expect(@store['person0']._id).to eq(id0)
    expect(@store['person0'].related._id).to eq(id1)
    expect(@store['person0'].related.related._id).to eq(id2)

    @store['person0'].related = nil
    expect(@store.gc).to eq(2)
    GC.start
    expect(@store.object_by_id(id1)).to be_nil
    expect(@store.object_by_id(id2)).to be_nil

    @store = PEROBS::Store.new(@db_file)
    expect(@store.check).to eq(0)
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

  it 'should track in-memory objects properly' do
    @store = PEROBS::Store.new(@db_file)
    expect(@store.statistics[:in_memory_objects]).to eq(1)
    @store['person'] = @store.new(Person)
    # We have the root hash and the Person object.
    expect(@store.statistics[:in_memory_objects]).to eq(2)
    @store.sync
    GC.start
    # Now the Person should be gone from memory.
    expect(@store.statistics[:in_memory_objects]).to eq(1)
  end

  it 'should handle nested constructors' do
    @store = PEROBS::Store.new(@db_file)
    @store['root'] = @store.new(O0)
    @store.sync
    expect(@store.check).to eq(0)

    @store = PEROBS::Store.new(@db_file)
    expect(@store.check).to eq(0)
    expect(@store['root'].child.parent).to eq(@store['root'])
  end

  it 'should survive a real world usage test' do
    options = { :engine => PEROBS::FlatFileDB }
    @store = PEROBS::Store.new(@db_file, options)
    ref = {}

    deletions_since_last_gc = 0
    0.upto(15000) do |i|
      key = "o#{i}"
      case rand(8)
      when 0
        # Add 'A' person
        value = 'A' * rand(512)
        @store[key] = p = @store.new(Person)
        p.name = value
        ref[key] = value
      when 1
        # Add 'B' person
        value = 'B' * rand(128)
        @store[key] = p = @store.new(Person)
        p.name = value
        ref[key] = value
      when 2
        # Delete a root entry
        if ref.keys.length > 11
          key = ref.keys[(ref.keys.length / 11).to_i]
          expect(@store[key]).not_to be_nil
          @store[key] = nil
          ref.delete(key)
          deletions_since_last_gc += 1
        end
      when 3
        # Call garbage collector
        if rand(60) == 0
          @store.gc
          stats = @store.statistics
          expect(stats.marked_objects).to eq(ref.length)
          expect(stats.swept_objects).to eq(deletions_since_last_gc)
          deletions_since_last_gc = 0
          expect(@store.gc).to eq(deletions_since_last_gc)
        end
      when 4
        # Sync store and reload
        if rand(15) == 0
          @store.sync
          @store = PEROBS::Store.new(@db_file, options)
        end
      when 5
        # Replace an entry with 'C' person
        if ref.keys.length > 13
          key = ref.keys[(ref.keys.length / 13).to_i]
          value = 'C' * rand(1024)
          @store[key] = p = @store.new(Person)
          p.name = value
          ref[key] = value
          deletions_since_last_gc += 1
        end
      when 6
        # Sync and check store
        if rand(50) == 0
          @store.sync
          expect(@store.check(false)).to eq(0)
        end
      when 7
        # Compare a random entry with reference entry
        if ref.keys.length > 0
          key = ref.keys[rand(ref.keys.length - 1)]
          expect(@store[key].name).to eq(ref[key])
        end
      end
    end

    ref.each do |k, v|
      expect(@store[k].name).to eq(v)
    end
  end

  it 'should copy the database' do
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
    p3 = @store.new(PEROBS::Array)
    @store['persons'] = p3
    p3 << p0
    p3 << p1
    p3 << p2
    p0 = p1 = p2 = p3 = nil
    expect(@store['person0']._id).to eq(id0)
    expect(@store['person0'].related._id).to eq(id1)
    expect(@store['person0'].related.related._id).to eq(id2)

    @store.copy(@db_file_new, { :engine => PEROBS::FlatFileDB })
    @store.delete_store

    @store = PEROBS::Store.new(@db_file_new, { :engine => PEROBS::FlatFileDB })
    expect(@store['person0']._id).to eq(id0)
    expect(@store['person0'].related._id).to eq(id1)
    expect(@store['person0'].related.related._id).to eq(id2)
    expect(@store['persons'][0]).to eq(@store['person0'])
    expect(@store['persons'][1]).to eq(@store['person0'].related)
    expect(@store['persons'][2]).to eq(@store['person0'].related.related)
  end

end
