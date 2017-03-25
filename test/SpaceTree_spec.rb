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
require 'perobs/SpaceTree'

describe PEROBS::SpaceTree do

  before(:all) do
    @db_dir = generate_db_name('SpaceTree')
    FileUtils.mkdir_p(@db_dir)
    @m = PEROBS::SpaceTree.new(@db_dir)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should open the space tree' do
    @m.open
    expect(@m.inspect).to eql('[]')
  end

  it 'should support adding spaces' do
    @m.add_space(80, 8)
    expect(@m.has_space?(80, 8)).to be true
    expect(@m.inspect).to eql('[[80, 8]]')
    expect(@m.check).to be true
    @m.add_space(40, 4)
    expect(@m.has_space?(40, 4)).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4]]')
    @m.add_space(20, 2)
    expect(@m.has_space?(20, 2)).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4], [20, 2]]')
    @m.add_space(160, 16)
    expect(@m.has_space?(160, 16)).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4], [20, 2], [160, 16]]')
    @m.add_space(320, 32)
    expect(@m.has_space?(320, 32)).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4], [20, 2], [160, 16], [320, 32]]')
    @m.add_space(100, 10)
    expect(@m.has_space?(100, 10)).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4], [20, 2], [160, 16], [100, 10], [320, 32]]')
    @m.add_space(81, 8)
    expect(@m.has_space?(81, 8)).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4], [20, 2], [81, 8], [160, 16], [100, 10], [320, 32]]')
    expect(@m.check).to be true
  end

  it 'should keep values over an close/open' do
    @m.add_space(1, 15)
    @m.close
    @m.open
    expect(@m.check).to be true
    expect(@m.inspect).to eql('[[80, 8], [40, 4], [20, 2], [81, 8], [160, 16], [100, 10], [1, 15], [320, 32]]')
  end

  it 'should find the smallest node' do
    node = @m.instance_variable_get('@root').find_smallest_node
    expect(node.size).to eql(2)
  end

  it 'should find the largest node' do
    node, address, parent = @m.instance_variable_get('@root').find_largest_node(nil)
    expect(node.size).to eql(32)
    expect(address).to eql(4)
    expect(parent.size).to eql(16)
  end

  it 'should support clearing the data' do
    @m.clear
    expect(@m.inspect).to eql('[]')
    @m.add_space(1, 1)
    @m.add_space(2, 2)
    @m.clear
    expect(@m.inspect).to eql('[]')
  end

  it 'should delete an equal node' do
    @m.clear
    add_sizes([ 10, 5, 15, 10 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5], [3, 10], [2, 15]]')
    expect(@m.get_space(10)).to eql([0, 10])

    @m.clear
    add_sizes([ 10, 5, 15, 10, 10 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5], [4, 10], [3, 10], [2, 15]]')
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.get_space(10)).to eql([4, 10])
  end

  it 'should delete an smaller node' do
    @m.clear
    add_sizes([ 10, 5, 3, 7 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5], [2, 3], [3, 7]]')
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.inspect).to eql('[[1, 5], [2, 3], [3, 7]]')

    @m.clear
    add_sizes([ 10, 5, 3 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5], [2, 3]]')
    expect(@m.get_space(5)).to eql([1, 5])
    expect(@m.inspect).to eql('[[0, 10], [2, 3]]')

    @m.clear
    add_sizes([ 10, 5 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5]]')
    expect(@m.get_space(5)).to eql([1, 5])
    expect(@m.inspect).to eql('[[0, 10]]')
  end

  it 'should delete an larger node' do
    @m.clear
    add_sizes([ 10, 15, 13, 17 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 15], [2, 13], [3, 17]]')
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.inspect).to eql('[[1, 15], [2, 13], [3, 17]]')

    @m.clear
    add_sizes([ 10, 15, 13 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 15], [2, 13]]')
    expect(@m.get_space(15)).to eql([1, 15])
    expect(@m.inspect).to eql('[[0, 10], [2, 13]]')

    @m.clear
    add_sizes([ 10, 15 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 15]]')
    expect(@m.get_space(15)).to eql([1, 15])
    expect(@m.inspect).to eql('[[0, 10]]')
  end

  it 'should move largest of small tree' do
    @m.clear
    add_sizes([ 5, 3, 7 ])
    expect(@m.get_space(5)).to eql([0, 5])
    expect(@m.inspect).to eql('[[1, 3], [2, 7]]')

    @m.clear
    add_sizes([ 10, 5, 3, 7, 15, 7 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5], [2, 3], [3, 7], [5, 7], [4, 15]]')
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.inspect).to eql('[[3, 7], [1, 5], [2, 3], [5, 7], [4, 15]]')

    @m.clear
    add_sizes([ 10, 5, 3, 7, 15, 7, 6 ])
    expect(@m.inspect).to eql('[[0, 10], [1, 5], [2, 3], [3, 7], [6, 6], [5, 7], [4, 15]]')
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.inspect).to eql('[[3, 7], [6, 6], [1, 5], [2, 3], [5, 7], [4, 15]]')

    @m.clear
    add_sizes([ 10, 5, 3, 15, 13, 17 ])
    expect(@m.get_space(10)).to eql([0, 10])
    expect(@m.inspect).to eql('[[1, 5], [2, 3], [3, 15], [4, 13], [5, 17]]')
  end

  it 'should support a real-world traffic pattern' do
    address = 0
    spaces = []
    0.upto(100) do
      case rand(3)
      when 0
        # Insert new values
        rand(15).times do
          size = 20 + rand(50)
          @m.add_space(address, size)
          spaces << [address, size]
          address += size
        end
      when 1
        # Remove some values
        rand(13).times do
          size = 20 + rand(60)
          if (space = @m.get_space(size))
            expect(spaces.include?(space)).to be true
            spaces.delete(space)
          end
        end
      when 2
        expect(@m.check).to be true
      end
    end
  end

  def add_sizes(sizes)
    sizes.each_with_index do |size, i|
      @m.add_space(i, size)
    end
  end

end
