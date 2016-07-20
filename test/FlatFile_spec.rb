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
require 'perobs/FlatFile'

describe PEROBS::FlatFile do

  before(:all) do
    @db_dir = generate_db_name('FlatFile')
    FileUtils.mkdir_p(@db_dir)
    @ff = PEROBS::FlatFile.new(@db_dir)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should create the DB' do
    @ff.open
  end

  it 'should close the DB' do
    @ff.close
  end

  it 'should re-open the DB' do
    @ff.open
  end

  it 'should store the first blob' do
    @ff.write_obj_by_id(0, 'Object 0')
  end

  it 'should read the first blob again' do
    expect(@ff.read_obj_by_id(0)).to eql('Object 0')
  end

  it 'should store the 2nd blob' do
    @ff.write_obj_by_id(1, 'Object One')
    expect(@ff.find_obj_addr_by_id(1)).to eql(29)
  end

  it 'should read the 2 blobs again' do
    expect(@ff.read_obj_by_id(0)).to eql('Object 0')
    expect(@ff.read_obj_by_id(1)).to eql('Object One')
  end

  it 'should store the 3rd blob' do
    @ff.write_obj_by_id(2, 'Object Zwei')
  end

  it 'should read the 3 blobs again' do
    expect(@ff.read_obj_by_id(0)).to eql('Object 0')
    expect(@ff.read_obj_by_id(1)).to eql('Object One')
    expect(@ff.read_obj_by_id(2)).to eql('Object Zwei')
  end

  it 'should delete the 2nd blob' do
    expect(@ff.delete_obj_by_id(1)).to be true
  end

  it 'should only read the remaining blobs again' do
    expect(@ff.read_obj_by_id(0)).to eql('Object 0')
    expect(@ff.read_obj_by_id(2)).to eql('Object Zwei')
    expect(@ff.read_obj_by_id(1)).to be_nil
  end

  it 'should store a blob in the whole between 0 and 2' do
    @ff.write_obj_by_id(1, 'Object One')
    expect(@ff.read_obj_by_id(0)).to eql('Object 0')
    expect(@ff.read_obj_by_id(1)).to eql('Object One')
    expect(@ff.read_obj_by_id(2)).to eql('Object Zwei')
    expect(@ff.find_obj_addr_by_id(1)).to eql(29)
  end

  it 'should not store a blob that is slightly smaller in the whole' do
    expect(@ff.delete_obj_by_id(1)).to be true
    @ff.write_obj_by_id(1, 'Object 1')
    expect(@ff.read_obj_by_id(1)).to eql('Object 1')
    expect(@ff.find_obj_addr_by_id(1)).to eql(92)
  end

  it 'should store a small blob in a big existing whole' do
    @ff.write_obj_by_id(3, 'Object 3333333333333333333333')
    expect(@ff.find_obj_addr_by_id(3)).to eql(121)
    @ff.write_obj_by_id(4, 'Object 4')
    expect(@ff.delete_obj_by_id(3)).to be true
    @ff.write_obj_by_id(5, 'Object 5')
    expect(@ff.find_obj_addr_by_id(5)).to eql(121)
  end

  it 'should defragment the file' do
    @ff.defragmentize
    expect(@ff.read_obj_by_id(0)).to eql('Object 0')
    expect(@ff.find_obj_addr_by_id(0)).to eql(0)
    expect(@ff.read_obj_by_id(2)).to eql('Object Zwei')
    expect(@ff.find_obj_addr_by_id(2)).to eql(29)
    expect(@ff.read_obj_by_id(1)).to eql('Object 1')
    expect(@ff.find_obj_addr_by_id(1)).to eql(61)
    expect(@ff.read_obj_by_id(4)).to eql('Object 4')
    expect(@ff.read_obj_by_id(5)).to eql('Object 5')
    expect(@ff.find_obj_addr_by_id(5)).to eql(90)
  end

  it 'should close the DB' do
    @ff.close
  end

end

