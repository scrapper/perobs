# encoding: UTF-8
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
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
    @store['h'] = h = PEROBS::Hash.new(@store)
    h['a'] = 'A'
    h['b'] = 'B'
    h['po'] = po = PO.new(@store)
    po.name = 'foobar'
    h['b'] = 'B'

    h['a'].should == 'A'
    h['b'].should == 'B'
    @store.sync

    @store = PEROBS::Store.new(@db_name)
    h = @store['h']
    h['a'].should == 'A'
    h['b'].should == 'B'
    h['po'].name.should == 'foobar'
  end

  it 'should have an each method to iterate' do
    @store['h'] = h = PEROBS::Hash.new(@store)
    h['a'] = 'A'
    h['b'] = 'B'
    h['c'] = 'C'
    vs = []
    h.each { |k, v| vs << k + v }
    vs.sort.join.should == 'aAbBcC'

    @store = PEROBS::Store.new(@db_name)
    @store['h'] = h = PEROBS::Hash.new(@store)
    h['a'] = PO.new(@store, 'A')
    h['b'] = PO.new(@store, 'B')
    h['c'] = PO.new(@store, 'C')
    vs = []
    h.each { |k, v| vs << k + v.name }
    vs.sort.join.should == 'aAbBcC'
  end

  # Utility method to create a PEROBS::Hash from a normal Hash.
  def cph(hash = nil)
    a = PEROBS::Hash.new(@store)
    a.replace(hash) unless hash.nil?
    @store['a'] = a
  end

  def pcheck
    yield
    @store.sync
    @store = PEROBS::Store.new(@db_name)
    yield
  end

  it 'should support reading method' do
    cph({ [1] => [2] }).flatten.should == [ [1], [2] ]

    a = cph({ 1 => "one", 2 => [ 2, "two" ], 3 => [ 3, [ "three" ] ] })
    a.flatten.should == [ 1, "one", 2, [ 2, "two" ], 3, [ 3, ["three"] ] ]
    a.flatten(0).should == [
      [ 1, "one" ],
      [ 2, [ 2, "two" ] ],
      [ 3, [ 3, [ "three" ] ] ]
    ]
    a.has_key?(2).should be_true
  end

  it 'should support rewriting methods' do
    h = cph({ 1 => 'a', 2 => 'b' })
    h.clear
    h.size.should == 0
    h[1].should be_nil

    h = cph({ 1 => 'a', 2 => 'b' })
    h.delete_if { |k, v| k == 1 }.size.should == 1
  end

  it 'should support merge!' do
    h1 = cph({ 1 => 2, 2 => 3, 3 => 4 })
    h2 = cph({ 2 => 'two', 4 => 'four' })

    ha = { 1 => 2, 2 => 'two', 3 => 4, 4 => 'four' }
    hb = { 1 => 2, 2 => 3, 3 => 4, 4 => 'four' }

    h1.update(h2).should == ha
    pcheck { h1.should == ha }

    h1 = cph({ 1 => 2, 2 => 3, 3 => 4 })
    h2 = cph({ 2 => 'two', 4 => 'four' })

    h2.update(h1).should == hb
    pcheck { h2.should == hb }
  end

end
