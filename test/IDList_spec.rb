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

  describe IDList do

    before(:all) do
      @db_dir = generate_db_name('IDList')
      FileUtils.mkdir_p(@db_dir)
      @list = PEROBS::IDList.new(@db_dir, 'idlist', 16, 64)
    end

    after(:all) do
      @list.erase
      FileUtils.rm_rf(@db_dir)
    end

    it 'should not contain any values' do
      expect(@list.to_a).to eql []
      expect(@list.include?(0)).to be false
      expect(@list.include?(1)).to be false
      expect { @list.check }.to_not raise_error
    end

    it 'should store a large number of values' do
      vals = []
      50000.times do
        v = rand(2 ** 64)
        vals << v

        next if @list.include?(v)
        @list.insert(v)
        #expect(@list.include?(v)).to be true
        0.upto(rand(10)) do
          v = vals[rand(vals.length)]
          expect(@list.include?(v)).to be true
        end

        #expect { @list.check }.to_not raise_error if rand(1000) == 0
      end
      expect { @list.check }.to_not raise_error

      vals.each do |v|
        expect(@list.include?(v)).to be true
      end
    end

  end

end

