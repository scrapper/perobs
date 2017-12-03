# encoding: UTF-8
#
# Copyright (c) 2016, 2017 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'fileutils'

require 'spec_helper'
require 'perobs/Store'
require 'perobs/BigTree'

describe PEROBS::BigTreeNode do

  before(:all) do
    @db_name = generate_db_name(__FILE__)
    @store = PEROBS::Store.new(@db_name)
    @t = @store.new(PEROBS::BigTree, 4)
  end

  before(:each) do
    @t.clear
  end

  after(:all) do
    @store.delete_store
  end

  it 'should insert a key/value pair' do
    n = @t.root
    n.insert(0, 0)
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(1)
    expect(s.branch_nodes).to eql(0)
    expect(s.min_depth).to eql(1)
    expect(s.max_depth).to eql(1)
    node_chain = @t.node_chain(0)
    expect(node_chain.size).to eql(1)
    expect(node_chain.first).to eql(n)
  end

  it 'should split a leaf node that becomes full' do
    4.times { |n| @t.insert(n, n) }
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(1)
    @t.insert(4, 4)
    s = @t.statistics
    expect(s.leaf_nodes).to eql(2)
    expect(s.branch_nodes).to eql(1)
    expect(s.min_depth).to eql(2)
    expect(s.max_depth).to eql(2)
  end

  it 'should split a branch node that becomes full' do
    11.times { |n| @t.insert(n, n) }
    s = @t.statistics
    expect(s.leaf_nodes).to eql(5)
    expect(s.branch_nodes).to eql(1)
    expect(s.min_depth).to eql(2)
    expect(s.max_depth).to eql(2)
    @t.insert(11, 11)
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(5)
    expect(s.branch_nodes).to eql(3)
    expect(s.min_depth).to eql(3)
    expect(s.max_depth).to eql(3)
  end

  it 'should merge leaf node with next sibling' do
    5.times { |n| @t.insert(n, n) }
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(2)

    @t.remove(0)
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(1)
    expect(s.min_depth).to eql(1)
    expect(s.max_depth).to eql(1)
  end

  it 'should merge leaf node with previous siblin' do
    5.times { |n| @t.insert(n, n) }
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(2)

    @t.remove(2)
    @t.remove(3)
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(1)
  end

  it 'should merge branch node with next sibling' do
    12.times { |n| @t.insert(n, n) }
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(5)
    expect(s.branch_nodes).to eql(3)

    @t.remove(2)
    @t.remove(3)
    @t.remove(4)
    @t.remove(5)
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(3)
    expect(s.branch_nodes).to eql(1)
  end

  it 'should merge branch node with previous sibling' do
    12.times { |n| @t.insert(n, n) }
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(5)
    expect(s.branch_nodes).to eql(3)

    @t.remove(4)
    @t.remove(5)
    @t.remove(2)
    @t.remove(3)
    expect(@t.check).to be true
    s = @t.statistics
    expect(s.leaf_nodes).to eql(3)
    expect(s.branch_nodes).to eql(1)
  end

end

