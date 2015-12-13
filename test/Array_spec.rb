# encoding: UTF-8
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
#
# This file contains tests for Array that are similar to the tests for the
# Array implementation in MRI. The ideas of these tests were replicated in
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
    _set(:name, name)
  end

end

describe PEROBS::Array do

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
    @store['a'] = a = @store.new(PEROBS::Array)
    a[0] = 'A'
    a[1] = 'B'
    a[2] = po = @store.new(PO)
    po.name = 'foobar'

    expect(a[0]).to eq('A')
    expect(a[1]).to eq('B')
    expect(a[2].name).to eq('foobar')
    @store.sync

    @store = PEROBS::Store.new(@db_name)
    a = @store['a']
    expect(a[0]).to eq('A')
    expect(a[1]).to eq('B')
    expect(a[2].name).to eq('foobar')
  end

  it 'should have an each method to iterate' do
    @store['a'] = a = @store.new(PEROBS::Array)
    a[0] = 'A'
    a[1] = 'B'
    a[2] = 'C'
    vs = ''
    a.each { |v| vs << v }
    expect(vs).to eq('ABC')
    @store.sync

    @store = PEROBS::Store.new(@db_name)
    a = @store['a']
    vs = ''
    a[3] = @store.new(PO, 'D')
    a.each { |v| vs << (v.is_a?(String) ? v : v.name) }
    expect(vs).to eq('ABCD')
  end

  # Utility method to create a PEROBS::Array from a normal Array.
  def cpa(ary = nil)
    a = @store.new(PEROBS::Array)
    a.replace(ary) unless ary.nil?
    @store['a'] = a
  end

  def pcheck
    yield
    @store.sync
    @store = PEROBS::Store.new(@db_name)
    yield
  end

  it 'should support reading methods' do
    (cpa([ 1, 1, 3, 5 ]) & cpa([ 1, 2, 3 ])).should == [ 1, 3 ]
    (cpa & cpa([ 1, 2, 3 ])).should == []

    expect(cpa.empty?).to be true
    expect(cpa([ 0 ]).empty?).to be false

    x = cpa([ 'it', 'came', 'to', 'pass', 'that', '...'])
    x = x.sort.join(' ')
    expect(x).to eq('... came it pass that to')
  end

  it 'should support Enumberable methods' do
    x = cpa([ 2, 5, 3, 1, 7 ])
    expect(x.find { |e| e == 4 }).to be_nil
    expect(x.find { |e| e == 3 }).to eq(3)
  end

  it 'should support re-writing methods' do
    x = cpa([2, 5, 3, 1, 7])
    x.sort!{ |a, b| a <=> b }
    pcheck { expect(x).to eq([ 1, 2, 3, 5, 7 ]) }
    x.sort!{ |a, b| b - a }
    pcheck { expect(x).to eq([ 7, 5, 3, 2, 1 ]) }

    x.clear
    pcheck { expect(x).to eq([]) }
  end

  it 'should support <<()' do
    a = cpa([ 0, 1, 2 ])
    a << 4
    pcheck { expect(a).to eq([ 0, 1, 2, 4 ]) }
  end

  it 'should support []=' do
    a = cpa([ 0, nil, 2 ])
    a[1] = 1
    pcheck { expect(a).to eq([ 0, 1, 2 ]) }
  end

  it 'should support collect!()' do
    a = cpa([ 1, 'cat', 1..1 ])
    expect(a.collect! { |e| e.class }).to eq([ Fixnum, String, Range ])
    pcheck { expect(a).to eq([ Fixnum, String, Range ]) }

    a = cpa([ 1, 'cat', 1..1 ])
    expect(a.collect! { 99 }).to eq([ 99, 99, 99])
    pcheck { expect(a).to eq([ 99, 99, 99]) }
  end

  it 'should support map!()' do
    a = cpa([ 1, 'cat', 1..1 ])
    expect(a.map! { |e| e.class }).to eq([ Fixnum, String, Range ])
    pcheck { expect(a).to eq([ Fixnum, String, Range ]) }

    a = cpa([ 1, 'cat', 1..1 ])
    expect(a.map! { 99 }).to eq([ 99, 99, 99])
    pcheck { expect(a).to eq ([ 99, 99, 99]) }
  end

  it 'should support fill()' do
    pcheck { expect(cpa([]).fill(99)).to eq([]) }
    pcheck { expect(cpa([]).fill(99, 0)).to eq([]) }
    pcheck { expect(cpa([]).fill(99, 0, 1)).to eq([ 99 ]) }
  end

  it 'should support flatten!()' do
    a1 = cpa([ 1, 2, 3])
    a2 = cpa([ 5, 6 ])
    a3 = cpa([ 4, a2 ])
    a4 = cpa([ a1, a3 ])
    pcheck { expect(a4.flatten).to eq([ 1, 2, 3, 4, 5, 6 ]) }
  end

  it 'should support replace()' do
    a = cpa([ 1, 2, 3])
    a_id = a.__id__
    expect(a.replace(cpa([4, 5, 6]))).to eq([ 4, 5, 6 ])
    pcheck { expect(a).to eq ([ 4, 5, 6 ]) }
  end

  it 'should support insert()' do
    a = cpa([ 0 ])
    a.insert(1)
    pcheck { expect(a).to eq([ 0 ]) }
    a.insert(1, 1)
    pcheck { expect(a).to eq([ 0, 1]) }
  end

  it 'should support push()' do
    a = cpa([ 1, 2, 3 ])
    a.push(4, 5)
    pcheck { expect(a).to eq([ 1, 2, 3, 4, 5 ]) }
    a.push(nil)
    pcheck { expect(a).to eq([ 1, 2, 3, 4, 5, nil ]) }
  end

end
