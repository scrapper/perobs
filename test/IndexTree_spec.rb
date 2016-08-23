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
require 'perobs/IndexTree'

describe PEROBS::StackFile do

  before(:all) do
    @db_dir = generate_db_name('IndexTree')
    FileUtils.mkdir_p(@db_dir)
    @t = PEROBS::IndexTree.new(@db_dir)
    @ref = []
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should create the file' do
    @t.open
  end

  it 'should store and retrieve a value' do
    @t.put_value(0x8, 8)
    expect(@t.inspect).to eql("{\n  8 => 8,\n}\n")
    expect(@t.get_value(0x8)).to eql(8)
  end

  it 'should store another value in the same node' do
    @t.put_value(0x4, 4)
    expect(@t.get_value(0x4)).to eql(4)
  end

  it 'should store another value in a new sub-node' do
    @t.put_value(0x88, 0x88)
    expect(@t.inspect).to eql("{\n  4 => 4,\n  {\n    8 => 8,\n    136 => 136,\n  }\n  }\n")
    expect(@t.get_value(0x88)).to eql(0x88)
  end

  it 'should store one more value in a new sub-node' do
    @t.put_value(0x888, 0x888)
    expect(@t.inspect).to eql("{\n  4 => 4,\n  {\n    8 => 8,\n    {\n      136 => 136,\n      2184 => 2184,\n    }\n    }\n  }\n")
    expect(@t.get_value(0x888)).to eql(0x888)
  end

  it 'should delete the 0x88 entry' do
    expect(@t.delete_value(0x88)).to be true
    expect(@t.inspect).to eql("{\n  4 => 4,\n  {\n    8 => 8,\n    {\n      2184 => 2184,\n    }\n    }\n  }\n")
  end

  it 'should delete all other entries' do
    expect(@t.delete_value(0x8)).to be true
    expect(@t.delete_value(0x4)).to be true
    expect(@t.delete_value(0x888)).to be true
    expect(@t.inspect).to eql("{\n}\n")
  end

  it 'should replace an existing value' do
    @t.put_value(0x8, 1)
    @t.put_value(0x8, 2)
    expect(@t.inspect).to eql("{\n  8 => 2,\n}\n")
    expect(@t.get_value(0x8)).to eql(2)
  end

  it 'should store lots more values' do
    @ref = [ 28465, 62258, 59640, 40113, 29237, 22890, 43429, 20374, 37393, 48482, 3243, 5751, 23426, 200, 16835, 38979, 31961, 58810, 40284, 53696, 44741, 53939, 16715, 2775, 16708, 49209, 48767, 6138, 36574, 23063, 13374, 33611, 43477, 63753, 22456, 4461, 52257, 62546, 13629, 52716, 54576, 64695, 7514, 22406, 60298, 43935, 50906, 48965, 56244, 12918, 630, 463, 44735, 49868, 10941, 29121, 26034, 21946, 34071, 55514, 35488, 64583, 59245, 43911, 3035, 2848, 3584, 6813, 61754, 877, 28118, 52626, 4454, 19024, 7467, 59573, 7661, 49226, 33151, 25919, 3596, 36407, 41008, 21611, 52627, 6393, 5551, 34773, 26697, 10510, 50726, 7743, 9843, 62188, 24468, 63553, 3728, 60080, 45667, 6165, 38354 ]
    @ref.each { |v| @t.put_value(v, v) }
    @ref.each do |v|
      expect(@t.get_value(v)).to eql(v)
    end
  end

  it 'should still have the values after a close/open' do
    @t.close
    @t.open
    @ref.each do |v|
      expect(@t.get_value(v)).to eql(v)
    end
  end

  it 'should delete some of the stored values' do
    del = [ 55514, 35488, 64583, 59245, 43911, 3035, 2848, 3584, 6813, 61754, 877, 28118, 52626, 4454, 19024, 7467, 23426, 200, 16835, 38979, 31961, 60080, 45667, 6165, 38354 ]
    del.each do |v|
      @ref.delete(v)
      expect(@t.delete_value(v)).to be true
    end
    del.each do |v|
      expect(@t.get_value(v)).to be_nil
    end
  end

end

