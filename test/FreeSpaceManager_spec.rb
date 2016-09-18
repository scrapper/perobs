# encoding: UTF-8
#
# Copyright (c) 2016 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/FreeSpaceManager'

describe PEROBS::FreeSpaceManager do

  before(:all) do
    @db_dir = generate_db_name('FreeSpaceManager')
    FileUtils.mkdir_p(@db_dir)
    @m = PEROBS::FreeSpaceManager.new(@db_dir)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should open the space manager' do
    @m.open
    expect(@m.inspect).to eql('[]')
  end

  it 'should support adding and removing space' do
    @m.add_space(1, 7)
    expect(@m.has_space?(1, 7)).to be true
    expect(@m.inspect).to eql('[nil, nil, [[1, 7]]]')
    expect(@m.get_space(4)).to eql([ 1, 7 ])
    expect(@m.inspect).to eql('[nil, nil, []]')
  end

  it 'should no longer provide that space' do
    expect(@m.get_space(4)).to be_nil
  end

  it 'should keep values over an close/open' do
    @m.add_space(1, 15)
    @m.close
    @m.open
    expect(@m.inspect).to eql('[nil, nil, [], [[1, 15]]]')
    expect(@m.get_space(8)).to eql([ 1, 15 ])
  end

  it 'should support clearing the data' do
    @m.clear
    expect(@m.inspect).to eql('[]')
  end

  it 'should multiple values to a pool' do
    1.upto(8) { |i| @m.add_space(i, i) }
    expect(@m.inspect).to eql('[[[1, 1]], [[2, 2], [3, 3]], [[4, 4], [5, 5], [6, 6], [7, 7]], [[8, 8]]]')
  end

  it 'should return the spaces again' do
    expect(@m.get_space(1)).to eql([ 1, 1])
    expect(@m.inspect).to eql('[[], [[2, 2], [3, 3]], [[4, 4], [5, 5], [6, 6], [7, 7]], [[8, 8]]]')
    expect(@m.get_space(2)).to eql([ 3, 3])
    expect(@m.inspect).to eql('[[], [[2, 2]], [[4, 4], [5, 5], [6, 6], [7, 7]], [[8, 8]]]')
    expect(@m.get_space(2)).to eql([ 2, 2])
    expect(@m.inspect).to eql('[[], [], [[4, 4], [5, 5], [6, 6], [7, 7]], [[8, 8]]]')
    expect(@m.get_space(4)).to eql([ 7, 7])
    expect(@m.inspect).to eql('[[], [], [[4, 4], [5, 5], [6, 6]], [[8, 8]]]')
    expect(@m.get_space(8)).to eql([ 8, 8])
    expect(@m.inspect).to eql('[[], [], [[4, 4], [5, 5], [6, 6]], []]')
  end

end
