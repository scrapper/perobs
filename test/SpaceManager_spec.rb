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
    @m = PEROBS::SpaceManager.new(@db_dir, PEROBS::ProgressMeter.new, 5)
    PEROBS.log.level = Logger::ERROR
    PEROBS.log.open($stderr)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should open the space manager' do
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

  it 'should support clearing all spaces' do
    @m.clear
    expect(@m.to_a).to eql([])
    @m.add_space(1, 1)
    @m.add_space(2, 2)
    @m.clear
    expect(@m.to_a).to eql([])
  end

  it 'should return exactly matching spaces' do
    @m.clear
    add_sizes([ 10, 5, 15, 10 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15], [3, 10]])
    expect(@m.get_space(10)).to eql([3, 10])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.to_a).to eql([[1, 5], [2, 15]])
    expect(@m.added_spaces).to eql(4)
    expect(@m.recycled_spaces).to eql(2)
    expect(@m.failed_requests).to eql(0)

    @m.clear
    add_sizes([ 10, 5, 15, 10, 10 ])
    expect(@m.to_a).to eql([[0, 10], [1, 5], [2, 15], [3, 10], [4, 10]])
    expect(@m.get_space(10)).to eql([4, 10])
    expect(@m.get_space(10)).to eql([3, 10])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.to_a).to eql([[1, 5], [2, 15]])
    expect(@m.added_spaces).to eql(5)
    expect(@m.recycled_spaces).to eql(3)
    expect(@m.failed_requests).to eql(0)
  end

  it "should return nil if no space can be found" do
    expect(@m.get_space(42)).to be nil
    expect(@m.get_space(9)).to be nil
    expect(@m.get_space(11)).to be nil
    expect(@m.recycled_spaces).to eql(3)
    expect(@m.failed_requests).to eql(3)
  end

  it 'should support a real-world traffic pattern' do
    address = 0
    spaces = []
    @m.clear
    0.upto(1500) do
      case rand(4)
      when 0
        # Insert new values
        rand(9).times do
          size = 20 + rand(80)
          @m.add_space(address, size)
          spaces << [ address, size ]
          address += size
        end
      when 1
        # Remove some values
        rand(7).times do
          size = rand(110)
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
        if rand(200) == 0
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
