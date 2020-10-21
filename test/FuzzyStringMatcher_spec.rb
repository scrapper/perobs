# encoding: UTF-8
#
# Copyright (c) 2020 by Chris Schlaeger <chris@taskjuggler.org>
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
require 'perobs/Store'
require 'perobs/FuzzyStringMatcher'

module PEROBS

  class WordRef < PEROBS::Object

    attr_persist :word, :line

    def initialize(store, word, line)
      super(store)
      self.word = word
      self.line = line
    end

  end

  describe FuzzyStringMatcher do

    before(:all) do
      @db_name = generate_db_name(__FILE__)
      @store = PEROBS::Store.new(@db_name)
      @store['fsm'] = @fsm = @store.new(FuzzyStringMatcher)
      @store['fsm2'] = @fsm2 = @store.new(FuzzyStringMatcher, true, 2)
    end

    after(:all) do
      @store.delete_store
    end

    it 'should have no matches for empty dict' do
      expect(@fsm.best_matches('foobar')).to eql([])
      expect(stats = @fsm.stats).not_to be_nil
      expect(stats['dictionary_size']).to eql(0)
      expect(stats['max_list_size']).to eql(0)
      expect(stats['avg_list_size']).to eql(0)
    end

    it 'should learn a word' do
      @fsm.learn('kindergarten')
      expect(stats = @fsm.stats).not_to be_nil
      expect(stats['dictionary_size']).to eql(11)
      expect(stats['max_list_size']).to eql(1)
      expect(stats['avg_list_size']).to eql(1.0)
    end

    it 'should clear the dictionary' do
      @fsm.clear
      expect(stats = @fsm.stats).not_to be_nil
      expect(stats['dictionary_size']).to eql(0)
      expect(stats['max_list_size']).to eql(0)
      expect(stats['avg_list_size']).to eql(0)
    end

    it 'should learn some words' do
      %w( one two three four five six seven eight nine ten
          eleven twelve thirteen fourteen fifteen sixteen
          seventeen eighteen nineteen twenty ).each do |w|
        @fsm.learn(w, w)
      end
      expect(stats = @fsm.stats).not_to be_nil
      expect(stats['dictionary_size']).to eql(65)
      expect(stats['max_list_size']).to eql(7)
      expect(stats['avg_list_size']).to be_within(0.001).of(1.415)
    end

    it 'should find a match' do
      dut = {
        [ 'one' ] => [ [ 'one', 1.0 ] ],
        [ 'three' ] => [ [ 'three', 1.0 ] ],
        [ 'four' ]=> [ [ 'four', 1.0 ], [ 'fourteen', 0.666 ] ],
        [ 'four', 1.0 ]=> [ [ 'four', 1.0 ] ],
        [ 'even' ] => [ [ 'seven', 0.666 ], [ 'eleven', 0.666 ] ],
        [ 'teen' ] => [ ['thirteen', 0.6666666666666666],
                      ['fourteen', 0.6666666666666666],
                      ['fifteen', 0.6666666666666666],
                      ['sixteen', 0.6666666666666666],
                      ['seventeen', 0.6666666666666666],
                      ['eighteen', 0.6666666666666666],
                      ['nineteen', 0.6666666666666666] ],
        [ 'aight' ] => [ [ 'eight', 0.5 ] ],
        [ 'thirdteen' ] => [ [ 'thirteen', 0.5 ] ],
        [ 'shirt teen', 0.3 ] => [ [ 'thirteen', 0.333 ] ]
      }
      check_data_under_test(@fsm, dut)
    end

    it 'should not find an unknown match' do
      expect(@fsm.best_matches('foobar')).to eql([])
    end

    it 'should find a match' do
      dut = {
        [ 'one' ] => [ [ 'one', 1.0 ] ],
        [ 'three' ] => [ [ 'three', 1.0 ] ],
        [ 'four' ]=> [ [ 'four', 1.0 ], [ 'fourteen', 0.666 ] ],
        [ 'four', 1.0 ]=> [ [ 'four', 1.0 ] ],
        [ 'even' ] => [ [ 'seven', 0.666 ], [ 'eleven', 0.666 ] ],
        [ 'teen' ] => [ ['thirteen', 0.6666666666666666],
                      ['fourteen', 0.6666666666666666],
                      ['fifteen', 0.6666666666666666],
                      ['sixteen', 0.6666666666666666],
                      ['seventeen', 0.6666666666666666],
                      ['eighteen', 0.6666666666666666],
                      ['nineteen', 0.6666666666666666] ],
        [ 'aight' ] => [ [ 'eight', 0.5 ] ],
        [ 'thirdteen' ] => [ [ 'thirteen', 0.5 ] ],
        [ 'shirt teen', 0.3 ] => [ [ 'thirteen', 0.333 ] ]
      }
      check_data_under_test(@fsm, dut)
    end

    it 'should sort best to worst matches' do
      @fsm.clear
      %w( xbar xfoox foor bar foobar barfoo foo rab baar fool xbarx
          foobarx xfoobarx foo_bar ).each do |w|
        @fsm.learn(w, w)
      end
      dut = {
        [ 'foo' ] => [["foo", 1.0], ["foor", 0.5], ["foobar", 0.5],
                      ["fool", 0.5], ["foobarx", 0.5], ["foo_bar", 0.5],
                      ["barfoo", 0.5]],
        [ 'bar' ] => [["bar", 1.0], ["barfoo", 0.5], ["xbar", 0.5],
                      ["foobar", 0.5], ["foo_bar", 0.5]],
        [ 'foobar' ] => [["foobar", 1.0], ["foobarx", 0.8], ["xfoobarx", 0.6]]
      }
      check_data_under_test(@fsm, dut)
    end

    it 'should handle a larger text' do
      text =<<-EOT
MIT License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
EOT

      text.split.each do |word|
        @fsm2.learn(word, word)
      end
      stats = @fsm2.stats
      expect(stats['dictionary_size']).to eql(352)
      expect(stats['max_list_size']).to eql(22)
      expect(stats['avg_list_size']).to be_within(0.001).of(2.409)
    end

    it 'should find case sensitive matches' do
      dut = {
        [ 'SOFTWARE', 0.5, 20 ] => [ [ 'SOFTWARE', 1.0 ], [ 'SOFTWARE.', 0.888 ] ],
        [ 'three', 0.5, 20 ] => [ [ 'the', 0.5 ], [ 'free', 0.5 ] ]
      }

      check_data_under_test(@fsm2, dut)
    end

    it 'should support references to PEROBS objects' do
      text =<<-EOT
MIT License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:
EOT

      line_no = 1
      @store['fsm'] = fsm = @store.new(FuzzyStringMatcher)
      @store['refs'] = refs = @store.new(Array)
      text.each_line do |line|
        line.split.each do |word|
          ref = @store.new(WordRef, word, line_no)
          refs << ref
          fsm.learn(word, ref)
        end
        line_no += 1
      end

      found_lines = []
      fsm.best_matches('SOFTWARE').each do |match|
        found_lines << match[0].line
      end
      expect(found_lines.sort).to eql([ 4, 5, 5, 7, 8 ])
    end

    it 'should with small search words' do
      @fsm.clear
      mats = 'Yukihiro Matsumoto'
      @fsm.learn(mats)
      expect(@fsm.best_matches('Yukihiro').first.first).to eql(mats)
      expect(@fsm.best_matches('Mats', 0.3).first.first).to eql(mats)
    end

    def check_data_under_test(fsm, dut)
      dut.each do |inputs, reference|
        key = inputs[0]
        results = fsm.best_matches(*inputs)

        expect(results.length).to eql(reference.length),
          "Wrong number of results for '#{key}': \n#{results}\n#{reference}"

        reference.each do |key, rating|
          match = results.find { |v| v[0] == key}
          expect(match).not_to be_nil,
            "result is missing key #{key}: #{results}"
          expect(match[0]).to eql(key),
            "Wrong match returned for key #{key}: #{match}"
          expect(match[1]).to be_within(0.001).of(rating),
            "Wrong rating returend for key #{key}: #{match}"
        end
      end
    end

  end

end

