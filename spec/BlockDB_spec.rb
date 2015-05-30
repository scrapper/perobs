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

require 'perobs/BlockDB'

describe PEROBS::BlockDB do

  before(:each) do
    FileUtils.rm_rf('test_dir')
    Dir.mkdir('test_dir')
  end

  after(:all) do
    FileUtils.rm_rf('test_dir')
  end

  it 'reserve blocks and find them again' do
    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.send('reserve_blocks', 0, 8)
    cfi.find(0).should == [ 8, 0 ]
    cfi.send('reserve_blocks', 1, 7)
    cfi.find(1).should == [ 7, 8 ]
    cfi.send('reserve_blocks', 2, 9)
    cfi.find(2).should == [ 9, 16 ]
    cfi.send('reserve_blocks', 3, 23)
    cfi.find(3).should == [ 23, 32 ]

    cfi.find(0).should == [ 8, 0 ]
    cfi.find(1).should == [ 7, 8 ]
    cfi.find(2).should == [ 9, 16 ]
  end

  it 'should reuse entries for known IDs' do
    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.send('reserve_blocks', 0, 8).should == 0
    cfi.send('reserve_blocks', 1, 7).should == 8
    cfi.send('reserve_blocks', 2, 9).should == 16
    cfi.send('reserve_blocks', 1, 8).should == 8
  end

  it 'should persist the entries' do
    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.send('reserve_blocks', 0, 8)
    cfi.send('reserve_blocks', 1, 7)
    cfi.send('reserve_blocks', 2, 9)
    cfi.send('reserve_blocks', 3, 23)
    cfi.send('write_index')

    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.find(0).should == [ 8, 0 ]
    cfi.find(1).should == [ 7, 8 ]
    cfi.find(2).should == [ 9, 16 ]
    cfi.find(3).should == [ 23, 32 ]
  end

  it 'should remove unmarked entries' do
    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.send('reserve_blocks', 0, 8)
    cfi.send('reserve_blocks', 1, 7)
    cfi.send('reserve_blocks', 2, 9)
    cfi.send('reserve_blocks', 3, 23)
    cfi.send('write_index')

    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.is_marked?(0).should be_false
    cfi.is_marked?(1).should be_false
    cfi.mark(1)
    cfi.is_marked?(1).should be_true
    cfi.is_marked?(2).should be_false
    cfi.mark(2)
    cfi.is_marked?(2).should be_true
    cfi.is_marked?(3).should be_false

    cfi.delete_unmarked_entries
    lambda { cfi.is_marked?(3) }.should raise_error
    cfi.clear_marks
    cfi.is_marked?(1).should be_false
    cfi.is_marked?(2).should be_false

    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.find(0).should be_nil
    cfi.find(1).should == [ 7, 8 ]
    cfi.find(2).should == [ 9, 16 ]
    cfi.find(3).should be_nil
  end

  it 'should fill gaps with best-fit strategy' do
    cfi = PEROBS::BlockDB.new('test_dir', 8)
    cfi.send('reserve_blocks', 0, 17).should == 0
    cfi.send('reserve_blocks', 1, 7).should == 24
    cfi.send('reserve_blocks', 2, 9).should == 32
    cfi.send('reserve_blocks', 3, 23).should == 48
    cfi.send('reserve_blocks', 4, 1).should == 72
    cfi.send('write_index')

    cfi.mark(1)
    cfi.mark(3)
    cfi.delete_unmarked_entries

    cfi.send('reserve_blocks', 5, 10).should == 32
    cfi.send('reserve_blocks', 6, 25).should == 72
    cfi.send('reserve_blocks', 7, 8).should == 0
    cfi.send('reserve_blocks', 8, 8).should == 8
  end

end

