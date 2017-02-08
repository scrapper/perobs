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
require 'perobs/FlatFileDB'

describe PEROBS::FlatFileDB do

  before(:all) do
    @db_dir = generate_db_name('FlatFileDB')
    FileUtils.mkdir_p(@db_dir)
    @db = PEROBS::FlatFileDB.new(@db_dir)
    @db.open
  end

  after(:each) do
    @db.check_db
  end

  after(:all) do
    @db.close
    FileUtils.rm_rf(@db_dir)
  end

  it 'should have a version file' do
    version_file = File.join(@db_dir, 'version')
    expect(File.exist?(version_file)).to be true
    expect(File.read(version_file).to_i).to eq(PEROBS::FlatFileDB::VERSION)
  end

end

