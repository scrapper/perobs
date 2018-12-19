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

  attr_persist :name, :zip, :bmi, :married, :related, :relatives

  def initialize(store)
    super
    attr_init(:name, '')
    attr_init(:bmi, 22.2)
    attr_init(:married, false)
  end

end

class PersonN < PEROBS::Object

  attr_persist :name, :zip, :bmi, :married, :related, :relatives

  def initialize(store)
    super
    attr_init(:name, '')
    attr_init(:bmi, 22.2)
    attr_init(:married, false)
  end

end

class O0 < PEROBS::Object

  attr_persist :child

  def initialize(store)
    super
    self.child = @store.new(O1, myself)
  end

end
class O1 < PEROBS::Object

  attr_persist :parent

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
    capture_io { @store.gc }
    capture_io { expect { @store.check }.to_not raise_error }
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

      capture_io { @store.exit }

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

    capture_io { @store.exit }

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
    capture_io { @store.exit }
    @store = PEROBS::Store.new(@db_file)
    0.upto(20) do |i|
      expect(@store["person#{i}"].name).to eq("New Person #{i}")
    end
  end

  it 'should garbage collect unlinked objects' do
    @store = PEROBS::Store.new(@db_file)
    persons = []
    0.upto(20) do |i|
      persons[i] = obj = @store.new(Person)
      obj.name = "person#{i}"
      if i < 3
        @store["person#{i}"] = obj
      else
        persons[i - 3].related = obj
      end
    end
    @store.sync
    expect(@store.size).to eq(21)

    @store['person0'] = nil
    capture_io { @store.gc }
    expect(@store.size).to eq(14)
    capture_io { expect { @store.check }.to_not raise_error }
    capture_io { @store.exit }
    @store = PEROBS::Store.new(@db_file)
    capture_io { expect { @store.check }.to_not raise_error }

    person = @store['person1']
    i = 0
    while (person = person.related) do
      i += 1
    end
    expect(i).to eq(6)
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
    capture_io { expect(@store.check).to eq(0) }
    capture_io { expect(@store.gc).to eq(0) }
    p0 = p1 = p2 = nil
    capture_io { @store.exit }
    GC.start
    @store = PEROBS::Store.new(@db_file)
    expect(@store['person0']._id).to eq(id0)
    expect(@store['person0'].related._id).to eq(id1)
    expect(@store['person0'].related.related._id).to eq(id2)

    @store['person0'].related = nil
    capture_io { expect(@store.gc).to eq(2) }
    GC.start
    expect(@store.object_by_id(id1)).to be_nil
    expect(@store.object_by_id(id2)).to be_nil
    capture_io { @store.exit }

    @store = PEROBS::Store.new(@db_file)
    capture_io { expect(@store.check).to eq(0) }
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
    # Ruby 2.3 and later has changed the GC so that this does not work
    # reliably anymore. The GC seems to operate lazyly.
    #expect(@store.statistics[:in_memory_objects]).to eq(1)
  end

  it 'should handle nested constructors' do
    @store = PEROBS::Store.new(@db_file)
    @store['root'] = @store.new(O0)
    @store.sync
    capture_io { expect(@store.check).to eq(0) }
    capture_io { @store.exit }

    @store = PEROBS::Store.new(@db_file)
    capture_io { expect(@store.check).to eq(0) }
    expect(@store['root'].child.parent).to eq(@store['root'])
  end

  it 'should handle frequent updates of objects' do
    @store = PEROBS::Store.new(@db_file)
    count = 10000
    0.upto(count) do |i|
      key = "Obj#{i}"
      @store[key] = p = @store.new(Person)
      p.name = "0:#{i}:" + 'X' * rand(64)
    end

    0.upto(10) do |iteration|
      0.upto(count) do |i|
        key = "Obj#{i}"
        p = @store[key]
        p.name = "#{iteration}:#{i}:" + 'X' * rand(64)
      end
      0.upto(count) do |i|
        key = "Obj#{i}"
        p = @store[key]
        o_it, o_i, o_x = p.name.split(':')
        if o_it.to_i != iteration
          $stderr.puts "Mismatch of #{p._id} with value #{o_it}:#{i}"
        end
        expect(o_it.to_i).to eql(iteration)
        expect(o_i.to_i).to eql(i)
      end
      capture_io { expect(@store.check).to eql(0) }
    end
  end

  it 'should survive a real world usage test' do
    options = { :engine => PEROBS::FlatFileDB }
    @store = PEROBS::Store.new(@db_file, options)
    ref = {}

    deletions_since_last_gc = 0
    0.upto(10000) do |i|
      key = "o#{i}"
      case rand(9)
      when 0
        # Add 'A' person
        value = key + 'A' * rand(512)
        @store[key] = p = @store.new(Person)
        p.name = value
        ref[key] = value
      when 1
        # Add 'B' person
        value = key + 'B' * rand(32)
        @store[key] = p = @store.new(Person)
        p.name = value
        ref[key] = value
      when 2
        # Delete a root entry
        if ref.keys.length > 11
          key = ref.keys[rand(ref.keys.length)]
          expect(@store[key]).not_to be_nil
          @store[key] = nil
          ref.delete(key)
          deletions_since_last_gc += 1
        end
      when 3
        # Update a person entry
        if ref.keys.length > 0
          key = ref.keys[rand(ref.keys.length)]
          expect(@store[key]).not_to be_nil
          value = key + 'C' * rand(996)
          p = @store[key]
          p.name = value
          ref[key] = value
        end
      when 4
        # Call garbage collector
        if rand(60) == 0
          capture_io { @store.gc }
          stats = @store.statistics
          expect(stats.marked_objects).to eq(ref.length)
          expect(stats.swept_objects).to eq(deletions_since_last_gc)
          deletions_since_last_gc = 0
          capture_io { expect(@store.gc).to eq(deletions_since_last_gc) }
        end
      when 5
        # Sync store and reload
        if rand(15) == 0
          capture_io { @store.exit }
          @store = PEROBS::Store.new(@db_file, options)
        end
      when 6
        # Replace an entry with 'C' person
        if ref.keys.length > 13
          key = ref.keys[(ref.keys.length / 13).to_i]
          value = key + 'D' * rand(1024)
          @store[key] = p = @store.new(Person)
          p.name = value
          ref[key] = value
          deletions_since_last_gc += 1
        end
      when 7
        # Sync and check store
        if rand(50) == 0
          #@store.sync
          capture_io { expect(@store.check(false)).to eq(0) }
        end
      when 8
        # Compare a random entry with reference entry
        if ref.keys.length > 0
          key = ref.keys[rand(ref.keys.length)]
          expect(@store[key].name).to eq(ref[key])
        end
      end
      #ref.each do |k, v|
      #  expect(@store[k].name).to eq(v), "failure in mode #{i}"
      #end
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
