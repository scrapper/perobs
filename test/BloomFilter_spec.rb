# encoding: UTF-8
#
# Copyright (c) 2020 by Chris Schlaeger <chris@taskjuggler.org>
#
# This file contains tests for Array that are similar to the tests for the
# Array implementation in MRI. The ideas of these tests were replicated in
# this code.
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

require 'perobs/BloomFilter'

describe PEROBS::BloomFilter do

  before(:all) do
    @id = [
      15191050008455428680, 12299007744984813316,
      5166768350264410923, 13292348670247722217,
      14430487124357777801, 15487549567917479701,
      4219749632997374336, 18325170387060340324,
      16220333585121207007, 11589657688989748247,
      12862051842956427746, 17668317881882501174,
      11277833426558760461, 3646670960627580271,
      1285779814179452747, 11596411545808801205,
      2535055195441993333, 5724862331395403665,
      10105029540253865427, 14970343112406871467,
      16485638787670942362, 16034040017385350636,
      2312331645889760827, 14149061299157779195,
      16432393689941514970, 15935843650302579023,
      11273766652852261146, 1494749247683857535,
      4692394982787425616, 5455121910323487202,
      7428456740978387035, 11443672151789240871
    ]
    @seed = 11129024599157579195
  end

  it 'should not include any numbers when empty' do
    bf = PEROBS::BloomFilter.new(@id.length)
    @id.each do |id|
      expect(bf.include?(id)).to be false
    end
  end

  it 'should store a set of 64 bit numbers' do
    bf = PEROBS::BloomFilter.new(3 * @id.length, @seed)
    @id.each do |id|
      expect(bf.include?(id)).to be false
      bf.insert(id)
    end
    @id.each do |id|
      expect(bf.include?(id)).to be true
    end
  end

  it 'should have a conflict if the bitmap is too small' do
    bf = PEROBS::BloomFilter.new(@id.length / 4, @seed)
    conflicts = 0
    @id.each do |id|
      if bf.include?(id)
        conflicts += 1
      end
      bf.insert(id)
    end
    expect(conflicts).to be > 0
  end

  it 'should have good hash functions' do
    bf = PEROBS::BloomFilter.new(32, @seed)
    occurance_counter1 = Hash.new { |a, b| 0 }
    occurance_counter2 = Hash.new { |a, b| 0 }

    n = 100000
    n.times do |i|
      number = rand(2 ** 64)
      hash = number & 0x1F #bf.hash1(number)
      occurance_counter1[hash] += 1
      hash = bf.hash2(number)
      occurance_counter2[hash] += 1
    end
    puts standard_deviation(occurance_counter1)
    expect(standard_deviation(occurance_counter1)).to be < 18.6
    expect(standard_deviation(occurance_counter2)).to be < 18.6
  end

  def mean(a)
    a.reduce(:+) / a.length
  end

  def standard_deviation(hash)
    mean_value = mean(hash.keys)
    total = hash.values.reduce(:+)
    variance = 0
    hash.each do |x, p|
      variance += (x - mean_value) ** 2 * (p.to_f / total)
    end
    Math.sqrt(variance)
  end

end

