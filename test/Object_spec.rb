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
    attr_init(:a1, 'a1')
    attr_init(:a2, nil)
    attr_init(:a4, 42)
  end

  def a3_deref
    @a3.a1
  end

  def get_self
    self # Never do this in real user code!
  end

end

class O3 < PEROBS::Object

  def initialize(store)
    super
  end

end

describe PEROBS::Store do

  before(:all) do
    @db_name = File.join(Dir.tmpdir, "Object_spec.#{rand(2**32)}")
  end

  before(:each) do
    @store = PEROBS::Store.new(@db_name)
  end

  after(:each) do
    FileUtils.rm_rf(@db_name)
  end

  it 'should initialize attributes with default values' do
    @store['o1'] = o1 = @store.new(O1)
    @store['o2'] = o2 = @store.new(O2)
    expect(o2.a1).to eq('a1')
    expect(o2.a2).to be_nil
    expect(o2.a3).to be_nil
    expect(o2.a4).to eq(42)
  end

  it 'should assign values to attributes' do
    @store['o1'] = o1 = @store.new(O1)
    @store['o2'] = o2 = @store.new(O2)
    o1.a1 = 'a1'
    o2.a1 = nil
    o2.a3 = o1

    expect(o1.a1).to eq('a1')
    expect(o2.a1).to be_nil
    expect(o2.a3).to eq(o1)
    expect(o2.a4).to eq(42)
    @store.sync
  end

  it 'should persist assigned values' do
    @store['o1'] = o1 = @store.new(O1)
    @store['o2'] = o2 = @store.new(O2)
    o1.a1 = 'a1'
    o2.a1 = nil
    o2.a3 = o1
    o2.a4 = @store.new(PEROBS::Array)
    o2.a4 += [ 0, 1, 2 ]
    @store.transaction do
      @store['o3'] = o3 = @store.new(O1)
      o3.a1 = @store.new(PEROBS::Array)
    end
    @store.sync
    @store = nil
    GC.start

    @store = PEROBS::Store.new(@db_name)
    o1 = @store['o1']
    o2 = @store['o2']
    o3 = @store['o3']
    expect(o1.a1).to eq('a1')
    expect(o2.a1).to be_nil
    expect(o2.a3).to eq(o1)
    expect(o2.a4).to eq([ 0, 1, 2 ])
    expect(o3.a1).to eq([])
  end

  it 'should transparently access a referenced object' do
    @store['o1'] = o1 = @store.new(O1)
    @store['o2'] = o2 = @store.new(O2)
    o1.a1 = 'a1'
    o2.a3 = o1
    expect(o2.a3_deref).to eq('a1')
  end

  it 'should always return a POXReference for a PEROBS object' do
    @store['o1'] = o1 = @store.new(O1)
    o1.a1 = @store.new(O2)
    expect(@store['o1'].respond_to?(:is_poxreference?)).to be true
    expect(o1.a1.respond_to?(:is_poxreference?)).to be true
  end

  it 'should catch a leaked PEROBS::ObjectBase object' do
    @store['a'] = a = @store.new(O1)
    o = @store.new(O2)
    expect { a.a1 = o.get_self }.to raise_error(ArgumentError)
  end

  it 'should raise an error when no attributes are defined' do
    @store['o3'] = @store.new(O3)
    expect { @store.sync }.to raise_error(StandardError)
  end

end
