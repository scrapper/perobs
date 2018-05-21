# encoding: UTF-8
#
# Copyright (c) 2017 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'spec_helper'

require 'perobs/LockFile'

describe PEROBS::LockFile do

  before(:each) do
    @dir = Dir.mktmpdir('LockFile')
    @file = File.join(@dir, 'LockFile.lock')
  end

  after(:each) do
    FileUtils.rm_rf(@dir)
  end

  it 'should raise an error if the lock file directory does not exist' do
    capture_io do
      expect(PEROBS::LockFile.new('/foo/bar/foobar').lock).to be false
    end
    PEROBS.log.open($stderr)
  end

  it 'should support taking and releasing the lock' do
    lock = PEROBS::LockFile.new(@file)
    expect(lock.is_locked?).to be false
    expect(lock.lock).to be true
    expect(lock.is_locked?).to be true
    expect(lock.unlock).to be true
  end

  it 'should fail the unlock after a forced unlock' do
    lock = PEROBS::LockFile.new(@file)
    expect(lock.lock).to be true
    expect(lock.is_locked?).to be true
    lock.forced_unlock
    expect(lock.is_locked?).to be false
    out = capture_io{ expect(lock.unlock).to be false }.log
    expect(out).to include('There is no current lock to release')
  end

  it 'should fail if the lock is already taken' do
    lock1 = PEROBS::LockFile.new(@file)
    expect(lock1.lock).to be true
    lock2 = PEROBS::LockFile.new(@file)
    out = capture_io { expect(lock2.lock).to be false }.log
    expect(out).to include('due to timeout')
    expect(lock1.unlock).to be true
    expect(lock2.lock).to be true
    expect(lock2.unlock).to be true
  end

  it 'should wait for the old lockholder' do
    pid = Process.fork do
      lock1 = PEROBS::LockFile.new(@file)
      expect(lock1.lock).to be true
      sleep 5
      expect(lock1.unlock).to be true
    end

    while !File.exist?(@file)
      sleep 1
    end
    lock2 = PEROBS::LockFile.new(@file,
                                 { :max_retries => 100, :pause_secs => 0.5 })
    expect(lock2.lock).to be true
    expect(lock2.unlock).to be true
    Process.wait(pid)
  end

  it 'should timeout waiting for the old lockholder' do
    pid = Process.fork do
      lock1 = PEROBS::LockFile.new(@file)
      expect(lock1.lock).to be true
      sleep 3
      expect(lock1.unlock).to be true
    end

    while !File.exist?(@file)
      sleep 1
    end
    lock2 = PEROBS::LockFile.new(@file,
                                 { :max_retries => 2, :pause_secs => 0.5 })
    out = capture_io { expect(lock2.lock).to be false }.log
    expect(out).to include('due to timeout')
    Process.wait(pid)
  end

  it 'should terminate the old lockholder after timeout' do
    pid = Process.fork do
      lock1 = PEROBS::LockFile.new(@file)
      expect(lock1.lock).to be true
      # This sleep will be killed
      sleep 1000
    end

    while !File.exist?(@file)
      sleep 1
    end

    lock2 = PEROBS::LockFile.new(@file, { :timeout_secs => 1 })
    out = capture_io { expect(lock2.lock).to be true }.log
    expect(out).to include('Old lock file found for PID')
    expect(lock2.unlock).to be true
    Process.wait(pid)
  end

end

