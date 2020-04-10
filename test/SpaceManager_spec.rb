# encoding: UTF-8
#
# Copyright (c) 2020 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/SpaceManager'
require 'perobs/ProgressMeter'

describe PEROBS::SpaceManager do

  before(:all) do
    @db_dir = generate_db_name('SpaceManager')
    FileUtils.mkdir_p(@db_dir)
    @m = PEROBS::SpaceManager.new(@db_dir, PEROBS::ProgressMeter.new)
    PEROBS.log.level = Logger::ERROR
    PEROBS.log.open($stderr)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should open the space tree' do
    @m.open
    expect(@m.to_a).to eql([])
  end

  it 'should support adding spaces' do
    @m.add_space(80, 8)
    expect(@m.has_space?(80, 8)).to be true
    expect(@m.to_a).to eql([[80, 8]])
    expect(@m.check).to be true
    @m.add_space(40, 4)
    expect(@m.has_space?(40, 4)).to be true
    expect(@m.to_a).to eql([[40, 4], [80, 8]])
    @m.add_space(20, 2)
    expect(@m.has_space?(20, 2)).to be true
    expect(@m.to_a).to eql([[20, 2], [40, 4], [80, 8]])
    @m.add_space(160, 16)
    expect(@m.has_space?(160, 16)).to be true
    expect(@m.to_a).to eql([[20, 2], [40, 4], [80, 8], [160, 16]])
    @m.add_space(320, 32)
    expect(@m.has_space?(320, 32)).to be true
    expect(@m.to_a).to eql([[20, 2], [40, 4], [80, 8], [160, 16], [320, 32]])
    @m.add_space(100, 10)
    expect(@m.has_space?(100, 10)).to be true
    expect(@m.to_a).to eql([[20, 2], [40, 4], [80, 8], [100, 10], [160, 16], [320, 32]])
    @m.add_space(81, 8)
    expect(@m.has_space?(81, 8)).to be true
    expect(@m.to_a).to eql([[20, 2], [40, 4], [80, 8], [81, 8], [100, 10], [160, 16], [320, 32]])
    expect(@m.check).to be true
  end

  it 'should keep values over an close/open' do
    @m.add_space(1, 15)
    expect(@m.check).to be true
    @m.close
    @m.open
    expect(@m.check).to be true
    expect(@m.to_a).to eql([[1, 15], [20, 2], [40, 4], [80, 8], [81, 8], [100, 10], [160, 16], [320, 32]])
  end

  it 'should support clearing the data' do
    @m.clear
    expect(@m.to_a).to eql([])
    @m.add_space(1, 1)
    @m.add_space(2, 2)
    @m.clear
    expect(@m.to_a).to eql([])
  end

  it 'should delete an equal node' do
    @m.clear
    add_sizes([ 10, 5, 15, 10 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15], [3, 10]])
    expect(@m.get_space(10)).to eql([3, 10])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15]])

    @m.clear
    add_sizes([ 10, 5, 15, 10, 10 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15], [3, 10], [4, 10]])
    expect(@m.get_space(10)).to eql([4, 10])
    expect(@m.get_space(10)).to eql([3, 10])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15]])
  end

  it 'should delete a smaller node' do
    @m.clear
    add_sizes([ 10, 5, 3, 7 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 3], [3, 7]])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.to_a).to eql([[1, 5], [2, 3], [3, 7]])

    @m.clear
    add_sizes([ 10, 5, 3 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 3]])
    expect(@m.get_space(5)).to eql([1, 5])
    expect(@m.to_a).to eql([[0, 10], [2, 3]])

    @m.clear
    add_sizes([ 10, 5 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5]])
    expect(@m.get_space(5)).to eql([1, 5])
    expect(@m.to_a).to eql([[0, 10]])

    @m.clear
    add_sizes([ 10, 5, 3, 7, 5 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 3], [3, 7], [4, 5]])
    expect(@m.get_space(3)).to eql([2, 3])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [3, 7], [4, 5]])
  end

  it 'should delete an larger node' do
    @m.clear
    add_sizes([ 10, 15, 13, 17 ])
    expect(@m.to_a).to eql([[0, 10], [1, 15], [2, 13], [3, 17]])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.to_a).to eql([[1, 15], [2, 13], [3, 17]])

    @m.clear
    add_sizes([ 10, 15, 13 ])
    expect(@m.to_a).to eql([[0, 10], [1, 15], [2, 13]])
    expect(@m.get_space(15)).to eql([1, 15])
    expect(@m.to_a).to eql([[0, 10], [2, 13]])

    @m.clear
    add_sizes([ 10, 15 ])
    expect(@m.to_a).to eql([[0, 10], [1, 15]])
    expect(@m.get_space(15)).to eql([1, 15])
    expect(@m.to_a).to eql([[0, 10]])

    @m.clear
    add_sizes([ 10, 5, 15, 20, 17, 22 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15], [3, 20], [4, 17], [5, 22]])
    expect(@m.get_space(15)).to eql([2, 15])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [3, 20], [4, 17], [5, 22]])
  end

  it 'should move largest of small tree' do
    @m.clear
    add_sizes([ 5, 3, 7 ])
    expect(@m.get_space(5)).to eql([0, 5])
    expect(@m.check).to be true
    expect(@m.to_a).to eql([[1, 3], [2, 7]])

    @m.clear
    add_sizes([ 10, 5, 3, 7, 15, 7 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 3], [3, 7], [4, 15], [5, 7]])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.check).to be true
    expect(@m.to_a).to eql([[1, 5], [2, 3], [3, 7], [4, 15], [5, 7]])

    @m.clear
    add_sizes([ 10, 5, 3, 7, 15, 7, 6 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 3], [3, 7], [4, 15], [5, 7], [6, 6]])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.to_a).to eql([[1, 5], [2, 3], [3, 7], [4, 15], [5, 7], [6, 6]])

    @m.clear
    add_sizes([ 10, 5, 3, 15, 13, 17 ])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.to_a).to eql([[1, 5], [2, 3], [3, 15], [4, 13], [5, 17]])
  end

  it 'should support a real-world traffic pattern' do
    address = 0
    spaces = []
    @m.clear
    0.upto(1000) do
      case rand(4)
      when 0
        # Insert new values
        rand(9).times do
          size = 20 + rand(5000)
          @m.add_space(address, size)
          spaces << [ address, size ]
          address += size
        end
      when 1
        # Remove some values
        rand(7).times do
          size = 20 + rand(6000)
          if (space = @m.get_space(size))
            expect(spaces.include?(space)).to be true
            spaces.delete(space)
          end
        end
      when 2
        if rand(10) == 0
          expect(@m.check).to be true
          spaces.each do |address, size|
            expect(@m.has_space?(address, size)).to be true
          end
          @m.to_a.each do |address, size|
            expect(spaces.include?([ address, size ])).to be true
          end
        end
      when 3
        if rand(100) == 0
          expect(@m.check).to be true
          @m.close
          @m.open
          expect(@m.check).to be true
        end
      end
    end
  end

  def add_sizes(sizes)
    sizes.each_with_index do |size, i|
      @m.add_space(i, size)
    end
  end

end
