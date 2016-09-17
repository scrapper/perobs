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
require 'perobs/StackFile'

describe PEROBS::StackFile do

  before(:all) do
    @db_dir = generate_db_name('StackFile')
    FileUtils.mkdir_p(@db_dir)
    @stack = PEROBS::StackFile.new(@db_dir, 'StackFile', 4)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should create the file' do
    @stack.open
  end

  it 'should not pop an entry from an empty stack' do
    expect(@stack.pop).to be_nil
  end

  it 'should push an entry and pop it again' do
    @stack.push('1234')
    expect(@stack.pop).to eql('1234')
  end

  it 'should push and pop multiple entries' do
    @stack.push('1111')
    @stack.push('2222')
    @stack.push('3333')
    @stack.push('4444')
    expect(@stack.pop).to eql('4444')
    expect(@stack.pop).to eql('3333')
    expect(@stack.pop).to eql('2222')
    expect(@stack.pop).to eql('1111')
    expect(@stack.pop).to be_nil
  end

  it 'should handle mixed pushes and pops' do
    @stack.push('1111')
    @stack.push('2222')
    expect(@stack.pop).to eql('2222')
    @stack.push('3333')
    @stack.push('4444')
    expect(@stack.pop).to eql('4444')
    expect(@stack.pop).to eql('3333')
    expect(@stack.pop).to eql('1111')
    expect(@stack.pop).to be_nil
    @stack.push('5555')
    expect(@stack.pop).to eql('5555')
    @stack.push('6666')
    @stack.push('7777')
    expect(@stack.pop).to eql('7777')
    expect(@stack.pop).to eql('6666')
  end

  it 'should persist the stack over close/open' do
    @stack.push('1111')
    @stack.push('2222')
    @stack.close
    @stack.open
    expect(@stack.pop).to eql('2222')
    expect(@stack.pop).to eql('1111')
    expect(@stack.pop).to be_nil
  end

  it 'should iterate over all entries' do
    @stack.push('1111')
    @stack.push('2222')
    @stack.push('3333')
    s = ''
    @stack.each { |e| s << e }
    expect(s).to eql('111122223333')
    expect(@stack.to_ary).to eql([ '1111', '2222', '3333' ])
  end

  it 'should clear the stack' do
    @stack.clear
    expect(@stack.pop).to be_nil
    expect(@stack.to_ary).to eql([])
  end

end

