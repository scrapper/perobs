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
require 'perobs/Store'

class FlatFileDB_O < PEROBS::Object

  po_attr :a, :b, :c

  def initialize(store)
    super
    attr_init(:a, 'foo')
    attr_init(:b, 42)
    attr_init(:c, false)
  end

end

describe PEROBS::FlatFileDB do

  before(:all) do
    @db_dir = generate_db_name(__FILE__)
    FileUtils.mkdir_p(@db_dir)
    @store = PEROBS::Store.new(@db_dir, :engine => PEROBS::FlatFileDB)
    @db = @store.db
  end

  after(:each) do
    @db.check_db
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should have a version file' do
    version_file = File.join(@db_dir, 'version')
    expect(File.exist?(version_file)).to be true
    expect(File.read(version_file).to_i).to eq(PEROBS::FlatFileDB::VERSION)
  end

  it 'should fail to open the same DB twice' do
    db2 = PEROBS::FlatFileDB.new(@db_dir)
    expect { db2.open }.to raise_error(PEROBS::FatalError)
  end

  it 'should do a version upgrade' do
    # Close the store
    @store['o'] = @store.new(FlatFileDB_O)
    @store.exit

    # Manually downgrade the version file to version 1
    version_file = File.join(@db_dir, 'version')
    File.write(version_file, '1')

    # Open the store again
    @store = PEROBS::Store.new(@db_dir, :engine => PEROBS::FlatFileDB)
    @db = @store.db
    expect(File.read(version_file).to_i).to eql(PEROBS::FlatFileDB::VERSION)
  end

end

