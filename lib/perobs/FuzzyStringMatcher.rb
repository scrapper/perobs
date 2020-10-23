# encoding: UTF-8
#
# = FuzzyStringMatcher.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'
require 'perobs/Object'

module PEROBS

  # The fuzzy string matcher can be used to perform a fuzzy string search
  # against a known set of strings. The dictionary of known strings does not
  # store the actual strings but references to arbitrary objects. These could
  # be the string, but can be something else related to the learned strings.
  # To use this class a list of strings with their references must be learned.
  # Once the dictionary has been established, fuzzy matches can be done.
  class FuzzyStringMatcher < PEROBS::Object

    attr_persist :case_sensitive, :n, :dict

    # Create a new FuzzyStringMatcher.
    # @param p [PEROBS::Store] place to store the dictionary
    # @param case_sensitive [Boolean] True if case matters for matching
    # @param n [Integer] Determines what kind of n-gramm is used to store the
    #        references in the dictionary. It also determines the minimum word
    #        length that can be used for fuzzy matches.
    def initialize(p, case_sensitive = false, n = 4)
      super(p)
      if n < 2 || n > 10
        raise ArgumentError, 'n must be between 2 and 10'
      end
      self.case_sensitive = case_sensitive
      self.n = n

      clear unless @dict
    end

    # Wipe the dictionary.
    def clear
      self.dict = @store.new(BigHash)
    end

    # Add a string with its reference to the dictionary.
    # @param string [String] The string to store
    # @param reference [Object] Any object that is associated with the string
    def learn(string, reference = string)
      reference = string if reference.nil?

      unless @case_sensitive
        string = string.downcase
      end
      # Enclose string in 'start of text' and 'end of text' ASCII values.
      string = "\002" + string + "\003"

      each_n_gramm(string) do |n_gramm|
        unless (ng_list = @dict[n_gramm])
          @dict[n_gramm] = ng_list = @store.new(Hash)
        end

        # We use the Hash as a Set. The value doesn't matter.
        ng_list[reference] = true unless ng_list.include?(reference)
      end

      nil
    end

    # Find the references who's string best matches the given string.
    # @param string [String] string to search for
    # @param min_score [Float] Value 0.01 and 1.0 that specifies how strict
    #        the matching should be done. The larger the value the more closer
    #        the given string needs to be.
    # @param max_count [Integer] The maximum number of matches that should be
    #        returned.
    # @return [Array] The result is an Array of Arrays. The nested Arrays only
    #         have 2 entries. The reference and a Float value between 0 and
    #         1.0 that describes how good the match is. The matches are sorted
    #         in descending order by the match score.
    def best_matches(string, min_score = 0.5, max_count = 100)
      unless @case_sensitive
        string = string.downcase
      end
      # Enclose string in 'start of text' and 'end of text' ASCII values.
      string = "\002" + string + "\003"

      matches = {}

      each_n_gramm(string) do |n_gramm|
        if (ng_list = @dict[n_gramm])
          ng_list.each do |reference, dummy|
            if matches.include?(reference)
              matches[reference] += 1
            else
              matches[reference] = 1
            end
          end
        end
      end

      return [] if matches.empty?

      match_list = matches.to_a

      # Set occurance counters to scores relative to the best possible score.
      # This will be the best possible score for a perfect match.
      best_possible_score = string.length - @n + 1
      match_list.map! { |a, b| [ a, b.to_f / best_possible_score ] }

      # Delete all matches that don't have the required minimum match score.
      match_list.delete_if { |a| a[1] < min_score }

      # Sort the list best to worst match
      match_list.sort! do |a, b|
        b[1] <=> a[1]
      end

      # Return the top max_count matches.
      match_list[0..max_count - 1]
    end

    # Returns some internal stats about the dictionary.
    def stats
      s = {}
      s['dictionary_size'] = @dict.size
      max = total = 0
      @dict.each do |n_gramm, ng_list|
        size = ng_list.length
        max = size if size > max
        total += size
      end
      s['max_list_size'] = max
      s['avg_list_size'] = total > 0 ? total.to_f / s['dictionary_size'] : 0

      s
    end

    private

    def each_n_gramm(string, &block)
      return if string.length < @n

      0.upto(string.length - @n) do |i|
        n_gramm = string[i, @n]

        yield(n_gramm)
      end
    end

  end

end

