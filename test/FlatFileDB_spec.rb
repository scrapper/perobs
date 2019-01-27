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
require 'LegacyDBs/LegacyDB'

class FlatFileDB_O < PEROBS::Object

  attr_persist :a, :b, :c

  def initialize(store)
    super
    attr_init(:a, 'foo')
    attr_init(:b, 42)
    attr_init(:c, false)
  end

end

describe PEROBS::FlatFileDB do

  before(:each) do
    @db_dir = generate_db_name(__FILE__)
    FileUtils.mkdir_p(@db_dir)
    @store_options = {
      :engine => PEROBS::FlatFileDB,
      :log => $stderr,
      :log_level => Logger::ERROR
    }
    @store = PEROBS::Store.new(@db_dir, @store_options)
  end

  after(:each) do
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
    @store.exit
    src_dir = File.join(File.dirname(__FILE__), 'LegacyDBs', 'version_3')
    FileUtils.cp_r(Dir.glob(src_dir + '/*'), @db_dir)

    db = LegacyDB.new(@db_dir)
    capture_io { db.open }
    capture_io { expect(db.check).to be true }
  end

  it 'should refuse a version downgrade' do
    # Close the store
    @store.exit

    # Manually downgrade the version file to version 1
    version_file = File.join(@db_dir, 'version')
    File.write(version_file, '1000000')

    # Open the store again
    expect { PEROBS::Store.new(@db_dir) }.to raise_error(PEROBS::FatalError)
  end

  it 'should recover from a lost index file' do
    @store['o'] = @store.new(FlatFileDB_O)
    @store.exit

    File.delete(File.join(@db_dir, 'index.blobs'))
    store = nil
    capture_io do
      store = PEROBS::Store.new(@db_dir)
    end
    expect(store['o'].b).to eql(42)
  end

  it 'should repair a damaged index file' do
    @store['o'] = @store.new(FlatFileDB_O)
    @store.exit

    File.write(File.join(@db_dir, 'index.blobs'), '*' * 500)
    store = nil
    capture_io do
      store = PEROBS::Store.new(@db_dir)
    end
    capture_io { store.check(true) }
    expect(store['o'].b).to eql(42)
  end

  it 'should repair a corrupted database.blobs file' do
    @store.exit

    db = PEROBS::FlatFileDB.new(@db_dir)
    db_file = File.join(@db_dir, 'database.blobs')
    db.open
    0.upto(5) do |i|
      db.put_object("#{i + 1}:#{'X' * (i + 1) * 30}", i + 1)
    end
    db.close
    db.open
    0.upto(5) do |i|
      db.put_object("#{i + 10}:#{'Y' * (i + 1) * 25}", i + 10)
    end
    pos = db.instance_variable_get('@flat_file').find_obj_addr_by_id(10)
    db.close

    f = File.open(db_file, 'rb+')
    f.seek(pos)
    f.write('ZZZZZ')
    f.close

    db.open
    expect(db.check_db).to eql(2)
    expect(db.check_db(true)).to eql(1)
    db.close
    db = PEROBS::FlatFileDB.new(@db_dir, { :log => $stderr,
                                           :log_level => Logger::ERROR })
    db.open
    expect(db.check_db).to eql(0)

    0.upto(5) do |i|
      expect(db.get_object(i + 1)).to eql("#{i + 1}:#{'X' * (i + 1) * 30}")
    end
    expect(db.get_object(10)).to be_nil
    1.upto(5) do |i|
      expect(db.get_object(i + 10)).to eql("#{i + 10}:#{'Y' * (i + 1) * 25}")
    end
    db.close
  end

end

