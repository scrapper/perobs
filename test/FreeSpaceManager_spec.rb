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
  end

  it 'should support adding and removing space' do
    @m.add_space(1, 42)
    expect(@m.get_space(32)).to eql([ 1, 42 ])
  end

  it 'should no longer provide that space' do
    expect(@m.get_space(32)).to be_nil
  end

  it 'should add various spaces' do
    s = [2952, 2253, 1659, 2875, 1909, 2355, 166, 3276, 2698, 1197, 1473, 4087, 1724, 275, 1670, 64, 839, 1479, 2365, 4044, 3740, 2327, 3704, 82, 1835, 2934, 2251, 3532, 3673, 3506, 684, 1826, 2761, 1705, 3260, 1499, 3811, 1599, 909, 2527, 3694, 331, 2848, 1007, 3504, 536, 1904, 2397, 253, 2655, 766, 1568, 2631, 752, 2252, 2255, 298, 851, 2545, 4042, 3971, 2968, 555, 243, 2374]
    s.each { |n| @m.add_space(n, n) }
    s.each do |n|
      adr = @m.get_space(n / 2)
      if adr
        expect(adr[0]).to be >= n / 2
      end
    end
  end

  it 'should support a clear' do
    @m.clear
  end

  it 'should keep values over an close/open' do
    @m.add_space(1, 42)
    @m.close
    @m.open
    expect(@m.get_space(32)).to eql([ 1, 42 ])
  end

end
