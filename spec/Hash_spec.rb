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

$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'fileutils'
require 'time'

require 'perobs'


class PO < PEROBS::Object

  po_attr :name

  def initialize(store, name = nil)
    super(store)
    @name =  name
  end

end

describe PEROBS::Hash do

  before(:all) do
    @db_name = 'test_db'
    FileUtils.rm_rf(@db_name)
  end

  after(:each) do
    FileUtils.rm_rf(@db_name)
  end

  it 'should store simple objects persistently' do
    store = PEROBS::Store.new(@db_name)
    store['h'] = h = PEROBS::Hash.new(store)
    h['a'] = 'A'
    h['b'] = 'B'
    h['po'] = po = PO.new(store)
    po.name = 'foobar'
    h['b'] = 'B'

    h['a'].should == 'A'
    h['b'].should == 'B'
    store.sync

    store = PEROBS::Store.new(@db_name)
    h = store['h']
    h['a'].should == 'A'
    h['b'].should == 'B'
    h['po'].name.should == 'foobar'
  end

  it 'should have an each method to iterate' do
    store = PEROBS::Store.new(@db_name)
    store['h'] = h = PEROBS::Hash.new(store)
    h['a'] = 'A'
    h['b'] = 'B'
    h['c'] = 'C'
    vs = []
    h.each { |k, v| vs << k + v }
    vs.sort.join.should == 'aAbBcC'

    store = PEROBS::Store.new(@db_name)
    store['h'] = h = PEROBS::Hash.new(store)
    h['a'] = PO.new(store, 'A')
    h['b'] = PO.new(store, 'B')
    h['c'] = PO.new(store, 'C')
    vs = []
    h.each { |k, v| vs << k + v.name }
    vs.sort.join.should == 'aAbBcC'
  end

end
