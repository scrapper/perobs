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

require 'perobs/ClassMap'
require 'perobs/BTreeDB'

describe PEROBS::ClassMap do

  before(:all) do
    @db_name = generate_db_name(__FILE__)
    @db = PEROBS::BTreeDB.new(@db_name)
    @map = PEROBS::ClassMap.new(@db)
  end

  after(:all) do
    FileUtils.rm_rf(@db_name)
  end

  it 'should return nil for an unknown ID' do
    expect(@map.id_to_class(0)).to be_nil
  end

  it 'should add a class' do
    expect(@map.class_to_id('Foo')).to eq(0)
  end

  it 'should find the class again' do
    expect(@map.id_to_class(0)).to eq('Foo')
  end

  it 'should still return nil for an unknown ID' do
    expect(@map.id_to_class(1)).to be_nil
  end

  it 'should forget classes not in keep list' do
    expect(@map.class_to_id('Bar')).to eq(1)
    expect(@map.class_to_id('Foobar')).to eq(2)
    @map.keep([ 'Bar' ])
    expect(@map.id_to_class(0)).to be_nil
    expect(@map.id_to_class(1)).to eq('Bar')
    expect(@map.id_to_class(2)).to be_nil
    expect(@map.class_to_id('Foo1')).to eq(0)
    expect(@map.class_to_id('Foo2')).to eq(2)
  end

end
