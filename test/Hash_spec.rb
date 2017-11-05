# encoding: UTF-8
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
#
# This file contains tests for Hash that are similar to the tests for the
# Hash implementation in MRI. The ideas of these tests were replicated in
# this code.
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


class PO < PEROBS::Object

  po_attr :name

  def initialize(store, name = nil)
    super(store)
    @name = name
  end

  def get_self
    self # Never do this in real user code!
  end

end

describe PEROBS::Hash do

  before(:all) do
    @db_name = generate_db_name(__FILE__)
  end

  before(:each) do
    @store = PEROBS::Store.new(@db_name)
  end

  after(:each) do
    @store.delete_store
  end

  it 'should store simple objects persistently' do
    @store['h'] = h = @store.new(PEROBS::Hash)
    h['a'] = 'A'
    h['b'] = 'B'
    h['po'] = po = @store.new(PO)
    po.name = 'foobar'
    h['b'] = 'B'

    expect(h['a']).to eq('A')
    expect(h['b']).to eq('B')
    @store.exit

    @store = PEROBS::Store.new(@db_name)
    h = @store['h']
    expect(h['a']).to eq('A')
    expect(h['b']).to eq('B')
    expect(h['po'].name).to eq('foobar')
  end

  it 'should have an each method to iterate' do
    @store['h'] = h = @store.new(PEROBS::Hash)
    h['a'] = 'A'
    h['b'] = 'B'
    h['c'] = 'C'
    vs = []
    h.each { |k, v| vs << k + v }
    expect(vs.sort.join).to eq('aAbBcC')
    @store.exit

    @store = PEROBS::Store.new(@db_name)
    @store['h'] = h = @store.new(PEROBS::Hash)
    h['a'] = @store.new(PO, 'A')
    h['b'] = @store.new(PO, 'B')
    h['c'] = @store.new(PO, 'C')
    vs = []
    h.each { |k, v| vs << k + v.name }
    expect(vs.sort.join).to eq('aAbBcC')
  end

  # Utility method to create a PEROBS::Hash from a normal Hash.
  def cph(hash = nil, id = 'a')
    a = @store.new(PEROBS::Hash)
    a.replace(hash) unless hash.nil?
    @store[id] = a
  end

  def pcheck
    yield
    @store.exit
    @store = PEROBS::Store.new(@db_name)
    yield
  end

  it 'should support reading method' do
    expect(cph({ [1] => [2] }).flatten).to eq([ [1], [2] ])

    a = cph({ 1 => "one", 2 => [ 2, "two" ], 3 => [ 3, [ "three" ] ] })
    expect(a.flatten).to eq([ 1, "one", 2, [ 2, "two" ], 3, [ 3, ["three"] ] ])
    expect(a.flatten(0)).to eq([
      [ 1, "one" ],
      [ 2, [ 2, "two" ] ],
      [ 3, [ 3, [ "three" ] ] ]
    ])
    expect(a.has_key?(2)).to be true
  end

  it 'should support Enumberable methods' do
    h = cph({ 1 => 'a', 2 => 'b' })
    expect(h.first).to eq([ 1, 'a' ])
  end

  it 'should support rewriting methods' do
    h = cph({ 1 => 'a', 2 => 'b' })
    h.clear
    expect(h.size).to eq(0)
    expect(h[1]).to be_nil

    h = cph({ 1 => 'a', 2 => 'b' })
    expect(h.delete_if { |k, v| k == 1 }.size).to eq(1)
  end

  it 'should support merge!' do
    h1 = cph({ '1' => 2, '2' => 3, '3' => 4 }, 'h1')
    h2 = cph({ '2' => 'two', '4' => 'four' }, 'h2')

    ha = { '1' => 2, '2' => 'two', '3' => 4, '4' => 'four' }
    hb = { '1' => 2, '2' => 3, '3' => 4, '4' => 'four' }

    expect(h1.update(h2)).to eq(ha)
    pcheck { expect(@store['h1'].to_hash).to eq(ha) }

    h1 = cph({ '1' => 2, '2' => 3, '3' => 4 }, 'h1')
    h2 = cph({ '2' => 'two', '4' => 'four' }, 'h2')

    expect(h2.update(h1)).to eq(hb)
    pcheck { expect(@store['h2'].to_hash).to eq(hb) }
  end

  it 'should support inspect' do
    h1 = cph({ 1 => 2 })
    h2 = cph({ 1 => 2, 'h1' => h1 })
    expect(h1.inspect).to eq("<PEROBS::Hash:#{h1._id}>\n{\n  1 => 2\n}\n")
    expect(h2.inspect).to eq("<PEROBS::Hash:#{h2._id}>\n{\n  1 => 2,\n  \"h1\" => <PEROBS::ObjectBase:#{h1._id}>\n}\n")
  end

  it 'should catch a leaked PEROBS::ObjectBase object' do
    @store['a'] = a = @store.new(PEROBS::Hash)
    o = @store.new(PO)
    a['a'] = o.get_self
    PEROBS.log.open(StringIO.new)
    expect { @store.sync }.to raise_error(PEROBS::FatalError)
    PEROBS.log.open($stderr)
  end

end
