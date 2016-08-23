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
require 'perobs/FixedSizeBlobFile'

describe PEROBS::FixedSizeBlobFile do

  before(:all) do
    @db_dir = generate_db_name('FixedSizeBlobFile')
    FileUtils.mkdir_p(@db_dir)
    @bf = PEROBS::FixedSizeBlobFile.new(@db_dir, 'FixedSizeBlobFile', 4)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should create the file' do
    @bf.open
  end

  it 'should return free addresses' do
    expect(@bf.free_address).to eql(0)
  end

  it 'should store and retrieve a blob' do
    @bf.store_blob(0,'0000')
    expect(@bf.retrieve_blob(0)).to eql('0000')
  end

  it 'should store and retrieve multiple blobs' do
    @bf.store_blob(0,'XXXX')
    @bf.store_blob(1,'1111')
    @bf.store_blob(2,'2222')
    @bf.store_blob(3,'3333')
    expect(@bf.retrieve_blob(0)).to eql('XXXX')
    expect(@bf.retrieve_blob(1)).to eql('1111')
    expect(@bf.retrieve_blob(2)).to eql('2222')
    expect(@bf.retrieve_blob(3)).to eql('3333')
  end

  it 'should return nil for a too large address' do
    expect(@bf.retrieve_blob(4)).to be_nil
  end

  it 'should delete an entry' do
    @bf.delete_blob(2)
    expect(@bf.retrieve_blob(4)).to be_nil
  end

  it 'should return 2 as an empty address now' do
    expect(@bf.free_address).to eql(2)
  end

  it 'should persist all values over and close/open' do
    @bf.close
    @bf.open

    expect(@bf.retrieve_blob(0)).to eql('XXXX')
    expect(@bf.retrieve_blob(1)).to eql('1111')
    expect(@bf.retrieve_blob(2)).to be_nil
    expect(@bf.retrieve_blob(3)).to eql('3333')
  end

end

