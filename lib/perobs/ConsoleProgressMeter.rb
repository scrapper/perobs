# encoding: UTF-8
#
# = ConsoleProgressMeter.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2018 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/ProgressMeter'

module PEROBS

  class ConsoleProgressMeter < ProgressMeter

    LINE_LENGTH = 79

    private

    def print_bar
      percent = @max_value == 0 ? 100.0 :
        (@current_value.to_f / @max_value) * 100.0
      percent = 0.0 if percent < 0
      percent = 100.0 if percent > 100.0

      meter = "<#{percent.to_i}%>"

      bar_length = LINE_LENGTH - @name.chars.length - 3 - meter.chars.length
      left_bar = '*' * (bar_length * percent / 100.0)
      right_bar = ' ' * (bar_length - left_bar.chars.length)

      print "\r#{@name} [#{left_bar}#{meter}#{right_bar}]"
    end

    def print_time
      s = "\r#{@name} [#{secsToHMS(@end_time - @start_time)}]"
      puts s + (' ' * (LINE_LENGTH - s.chars.length + 1))
    end

  end

end

