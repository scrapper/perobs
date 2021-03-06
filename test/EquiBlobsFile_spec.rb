# encoding: UTF-8
#
# Copyright (c) 2016, 2017 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/EquiBlobsFile'
require 'perobs/ProgressMeter'

describe PEROBS::EquiBlobsFile do

  before(:all) do
    PEROBS.log.level = Logger::ERROR
    PEROBS.log.open($stderr)
    @db_dir = generate_db_name('EquiBlobsFile')
    FileUtils.mkdir_p(@db_dir)
    @bf = PEROBS::EquiBlobsFile.new(@db_dir, 'EquiBlobsFile',
                                    PEROBS::ProgressMeter.new, 8)
  end

  after(:all) do
    FileUtils.rm_rf(@db_dir)
  end

  it 'should create the file' do
    @bf.open
    expect(@bf.total_entries).to eql(0)
    expect(@bf.total_spaces).to eql(0)
    expect(@bf.check).to be true
  end

  it 'should return free addresses' do
    expect(@bf.free_address).to eql(1)
  end

  it 'should store and retrieve a blob' do
    @bf.store_blob(1,'00000000')
    expect(@bf.total_entries).to eql(1)
    expect(@bf.check).to be true
    expect(@bf.retrieve_blob(1)).to eql('00000000')
    expect(@bf.check).to be true
  end

  it 'should store and retrieve multiple blobs' do
    @bf.store_blob(1,'XXXXXXXX')
    expect(@bf.total_entries).to eql(1)
    expect(@bf.check).to be true
    @bf.store_blob(2,'11111111')
    expect(@bf.check).to be true
    @bf.store_blob(3,'22222222')
    expect(@bf.check).to be true
    @bf.store_blob(4,'33333333')
    expect(@bf.total_entries).to eql(4)
    expect(@bf.check).to be true
    expect(@bf.retrieve_blob(1)).to eql('XXXXXXXX')
    expect(@bf.retrieve_blob(2)).to eql('11111111')
    expect(@bf.retrieve_blob(3)).to eql('22222222')
    expect(@bf.retrieve_blob(4)).to eql('33333333')
  end

  it 'should raise error for a too large address' do
    PEROBS.log.open(StringIO.new)
    expect { @bf.retrieve_blob(5) }.to raise_error(PEROBS::FatalError)
    PEROBS.log.open($stderr)
  end

  it 'should delete entries' do
    @bf.delete_blob(3)
    expect(@bf.total_entries).to eql(3)
    expect(@bf.total_spaces).to eql(1)
    expect(@bf.check).to be true
    @bf.delete_blob(2)
    expect(@bf.total_entries).to eql(2)
    expect(@bf.total_spaces).to eql(2)
    expect(@bf.check).to be true
    @bf.delete_blob(1)
    expect(@bf.total_entries).to eql(1)
    expect(@bf.total_spaces).to eql(3)
    expect(@bf.check).to be true
  end

  it 'should raise error when inserting into a non-reserved cell' do
    PEROBS.log.open(StringIO.new)
    expect { @bf.store_blob(1,'XXXXXXXX') }.to raise_error(PEROBS::FatalError)
    PEROBS.log.open($stderr)
    expect(@bf.total_entries).to eql(1)
    expect(@bf.check).to be true
  end

  it 'shoud support inserting into deleted cells' do
    expect(@bf.free_address).to eql(1)
    @bf.store_blob(1, '44444444')
    expect(@bf.check).to be true
  end

  it 'should persist all values over and close/open' do
    @bf.close
    @bf.open
    expect(@bf.total_entries).to eql(2)
    expect(@bf.total_spaces).to eql(2)
    expect(@bf.check).to be true

    expect(@bf.retrieve_blob(1)).to eql('44444444')
    expect(@bf.retrieve_blob(4)).to eql('33333333')
  end

  it 'should support inserting into deleted cells (2)' do
    expect(@bf.free_address).to eql(2)
    @bf.store_blob(2,'55555555')
    expect(@bf.check).to be true

    expect(@bf.free_address).to eql(3)
    @bf.store_blob(3,'66666666')
    expect(@bf.total_entries).to eql(4)

    expect(@bf.check).to be true
  end

  it 'should support deleting reserved blobs' do
    adr = @bf.free_address
    @bf.delete_blob(adr)
    expect(@bf.check).to be true
  end

  it 'should support clearing the file' do
    @bf.clear
    expect(@bf.total_entries).to eql(0)
    expect(@bf.total_spaces).to eql(0)
    expect(@bf.check).to be true
    expect(@bf.free_address).to eql(1)
    @bf.store_blob(1,'00000000')
    expect(@bf.total_entries).to eql(1)
    expect(@bf.check).to be true
    expect(@bf.retrieve_blob(1)).to eql('00000000')
    expect(@bf.check).to be true
  end

  it 'should support trimming the file' do
    @bf.clear
    1.upto(8) do |i|
      adr = @bf.free_address
      @bf.store_blob(adr, (0.ord + i).chr * 8)
    end
    expect(@bf.total_entries).to eql(8)
    @bf.delete_blob(1)
    @bf.delete_blob(2)
    @bf.delete_blob(4)
    @bf.delete_blob(5)
    @bf.delete_blob(7)
    expect(@bf.total_entries).to eql(3)
    expect(@bf.total_spaces).to eql(5)
    expect(@bf.check).to be true

    @bf.delete_blob(8)
    expect(@bf.total_entries).to eql(2)
    expect(@bf.total_spaces).to eql(4)
    expect(@bf.check).to be true

    @bf.delete_blob(6)
    expect(@bf.total_entries).to eql(1)
    expect(@bf.total_spaces).to eql(2)
    expect(@bf.check).to be true

    @bf.delete_blob(3)
    expect(@bf.total_entries).to eql(0)
    expect(@bf.total_spaces).to eql(0)
    expect(@bf.check).to be true
  end

  it 'should support erasing the file' do
    @bf.close
    @bf.erase
    @bf.open
    expect(@bf.total_entries).to eql(0)
    expect(@bf.total_spaces).to eql(0)
    expect(@bf.check).to be true
    @bf.store_blob(1,'XXXXXXXX')
    expect(@bf.total_entries).to eql(1)
    expect(@bf.check).to be true
  end

  it 'should support custom offsets' do
    @bf.close
    @bf.erase
    @bf.clear_custom_data
    @bf.register_custom_data('foo', 42)
    @bf.register_custom_data('bar', 43)
    @bf.open
    expect(@bf.total_entries).to eql(0)
    expect(@bf.total_spaces).to eql(0)
    expect(@bf.check).to be true
    expect(@bf.free_address).to eql(1)
    @bf.store_blob(1,'11111111')
    expect(@bf.free_address).to eql(2)
    @bf.store_blob(2,'22222222')
    expect(@bf.free_address).to eql(3)
    @bf.store_blob(3,'33333333')
    expect(@bf.check).to be true
    expect(@bf.total_entries).to eql(3)
    expect(@bf.total_spaces).to eql(0)
    @bf.delete_blob(2)
    expect(@bf.check).to be true
    expect(@bf.total_entries).to eql(2)
    expect(@bf.total_spaces).to eql(1)
    expect(@bf.free_address).to eql(2)
    @bf.store_blob(2,'44444444')
    expect(@bf.total_entries).to eql(3)
    expect(@bf.total_spaces).to eql(0)
    expect(@bf.check).to be true
  end

  it 'should support a mix of adds and deletes' do
    @bf.close
    @bf.erase
    @bf.clear_custom_data
    @bf.register_custom_data('foo', 42)
    @bf.register_custom_data('bar', 43)
    @bf.open

    entries = {}
    1000.times do |i|
      rand(30).times do
        adr = @bf.free_address
        expect(entries[adr]).to be nil
        val = rand(2 ** 64)
        @bf.store_blob(adr, [ val ].pack('Q'))
        entries[adr] = val
        #expect(@bf.check).to be true
      end

      rand(15).times do
        unless entries.empty?
          addresses = entries.keys
          adr = addresses[rand(addresses.length)]
          expect(@bf.retrieve_blob(adr).unpack('Q').first).to eql(entries[adr])
          @bf.delete_blob(adr)
          entries.delete(adr)
          #expect(@bf.check).to be true
        end
      end

      rand(5).times do
        unless entries.empty?
          addresses = entries.keys
          adr = addresses[rand(addresses.length)]
          val = rand(2 ** 64)
          @bf.store_blob(adr, [ val ].pack('Q'))
          entries[adr] = val
          #expect(@bf.check).to be true
        end
      end

      if rand(100) == 0
        expect(@bf.check).to be true
      end

      if rand(100) == 0
        @bf.first_entry = i
        @bf.set_custom_data('foo', i)
      end

      if rand(50) == 0
        @bf.close
        @bf.open
      end

      if rand(500) == 0
        @bf.clear
        entries = {}
      end
    end

    expect(@bf.check).to be true
  end

end

