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

require 'spec_helper'
require 'perobs/IDList'

module PEROBS

  class IDList

    def page_count
      @page_file.page_count
    end

    def page(index)
      @page_file.page(index)
    end

  end

  describe IDList do

    before(:all) do
      @db_dir = generate_db_name('IDList')
      FileUtils.mkdir_p(@db_dir)
      @list = PEROBS::IDList.new(@db_dir, 'idlist', 512, 4)
    end

    after(:all) do
      @list.erase
      FileUtils.rm_rf(@db_dir)
    end

    it 'should not contain any values' do
      expect(@list.include?(0)).to be false
      expect(@list.include?(1)).to be false
      expect(@list.page_count).to eql 1
      expect(@list.page(0).values).to eql []
      expect { @list.check }.to_not raise_error
    end

    it 'should fill the first page' do
      0.upto(3) do |i|
        @list.insert(i)
        expect(@list.include?(i)).to be true
        expect(@list.page_count).to eql 1
      end
      expect { @list.check }.to_not raise_error
      expect(@list.page(0).values).to eql [ 0, 1, 2, 3 ]
      expect(@list.node(0).page_idx).to eql 0
    end

    it 'should not contain 4 to 7' do
      4.upto(7) do |i|
        expect(@list.include?(i)).to be false
      end
    end

    it 'should fill the second page' do
      4.upto(7) do |i|
        @list.insert(i)
        expect(@list.include?(i)).to be true
        expect(@list.page_count).to eql 2
      end
      expect { @list.check }.to_not raise_error
      expect(@list.page(0).values).to eql [ 0, 2, 4, 6 ]
      expect(@list.node(0).page_idx).to eql 0
      expect(@list.page(1).values).to eql [ 1, 3, 5, 7 ]
      expect(@list.node(1).page_idx).to eql 1
    end

    it 'should fill the third page' do
      [ 8, 10, 12, 14 ].each do |i|
        @list.insert(i)
        expect(@list.include?(i)).to be true
        expect(@list.page_count).to eql 3
      end
      expect { @list.check }.to_not raise_error
      expect(@list.page(0).values).to eql [ 0, 4, 8, 12 ]
      expect(@list.node(0).page_idx).to eql 0
      expect(@list.page(2).values).to eql [ 2, 6, 10, 14 ]
      expect(@list.node(2).page_idx).to eql 2
    end

    it 'should still find the values 0 to 7' do
      0.upto(7) do |i|
        expect(@list.include?(i)).to be true
      end
    end

    it 'should fill the fourth page' do
      [ 9, 11, 13, 15 ].each do |i|
        @list.insert(i)
        expect(@list.include?(i)).to be true
        expect(@list.page_count).to eql 4
      end
      expect { @list.check }.to_not raise_error
      expect(@list.page(1).values).to eql [ 1, 5, 9, 13 ]
      expect(@list.page(3).values).to eql [ 3, 7, 11, 15 ]
      expect(@list.node(9).page_idx).to eql 1
      expect(@list.node(11).page_idx).to eql 3
    end

    it 'should store a large number of values' do
      vals = []
      10000.times do
        v = rand(2 ** 64)
        vals << v
        @list.insert(v)
        expect(@list.include?(v)).to be true
        v = vals[rand(vals.length)]
        unless @list.include?(v)
          $stderr.puts "Lost #{v}"
        end
        expect(@list.include?(v)).to be true
      end
      expect { @list.check }.to_not raise_error
      vals.each do |v|
        expect(@list.include?(v)).to be true
      end
    end

  end

end

