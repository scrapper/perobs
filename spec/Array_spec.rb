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

end
