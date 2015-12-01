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

  after(:each) do
    FileUtils.rm_rf(@db_name)
  end

  it 'should store simple objects persistently' do
    store = PEROBS::Store.new(@db_name)
    store['a'] = a = PEROBS::Array.new(store)
    a[0] = 'A'
    a[1] = 'B'
    a[2] = po = PO.new(store)
    po.name = 'foobar'

    a[0].should == 'A'
    a[1].should == 'B'
    a[2].name.should == 'foobar'
    store.sync

    store = PEROBS::Store.new(@db_name)
    a = store['a']
    a[0].should == 'A'
    a[1].should == 'B'
    a[2].name.should == 'foobar'
  end

  it 'should have an each method to iterate' do
    store = PEROBS::Store.new(@db_name)
    store['a'] = a = PEROBS::Array.new(store)
    a[0] = 'A'
    a[1] = 'B'
    a[2] = 'C'
    vs = ''
    a.each { |v| vs << v }
    vs.should == 'ABC'
    store.sync

    store = PEROBS::Store.new(@db_name)
    a = store['a']
    vs = ''
    a[3] = PO.new(store, 'D')
    a.each { |v| vs << (v.is_a?(String) ? v : v.name) }
    vs.should == 'ABCD'
  end

  # Utility method to create a PEROBS::Array from a normal Array.
  def cpa(ary = nil)
    a = PEROBS::Array.new(@store)
    a.replace(ary) unless ary.nil?
    @store['a'] = a
  end

  it 'should support the & operator' do
    @store = PEROBS::Store.new(@db_name)

  end

  it 'should support reading methods' do
    @store = PEROBS::Store.new(@db_name)

    (cpa([ 1, 1, 3, 5 ]) & cpa([ 1, 2, 3 ])).should == [ 1, 3 ]
    (cpa & cpa([ 1, 2, 3 ])).should == []

    cpa.empty?.should be_true
    cpa([ 0 ]).empty?.should be_false

    x = cpa(["it", "came", "to", "pass", "that", "..."])
    x = x.sort.join(" ")
    x.should == "... came it pass that to"
  end

  it 'should support re-writing methods' do
    @store = PEROBS::Store.new(@db_name)

    x = cpa([2, 5, 3, 1, 7])
    x.sort!{ |a, b| a <=> b }
    x.should == [1,2,3,5,7]
    x.sort!{ |a, b| b - a }
    x.should == [7,5,3,2,1]

    x.clear.should == []
  end

  it 'should support <<()' do
    @store = PEROBS::Store.new(@db_name)

    a = cpa([ 0, 1, 2 ])
    a << 4
    a.should == [ 0, 1, 2, 4 ]
  end

  it 'should support collect()' do
    @store = PEROBS::Store.new(@db_name)

    a = cpa([ 1, 'cat', 1..1 ])
    a.collect {|e| e.class}.should == [ Fixnum, String, Range]
    a.collect { 99 }.should == [ 99, 99, 99]
    cpa([]).collect { 99 }.should == []
    cpa([1, 2, 3]).collect.should be_a_kind_of(Enumerator)
  end


end
