# encoding: UTF-8
#
# = ProgressMeter.rb -- Persistent Ruby Object Store
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

require 'time'

module PEROBS

  # This is the base class for all ProgressMeter classes. It only logs into
  # the PEROBS log. You need to create a derived class that overloads
  # print_bar() and print_time() to provide more fancy outputs.
  class ProgressMeter

    def initialize
      @name = nil
      @max_value = nil
      @current_value = nil
      @start_time = nil
      @end_time = nil
    end

    def start(name, max_value)
      @name = name
      unless max_value >= 0
        raise ArgumentError, "Maximum value (#{max_value}) must be larger " +
          "or equal to 0"
      end
      @max_value = max_value
      @current_value = 0
      @start_time = Time.now
      @end_time = nil
      print_bar

      if block_given?
        yield(self)
        done
      end
    end

    def update(value)
      return unless (value_i = value.to_i) > @current_value

      @current_value = value_i
      print_bar
    end

    def done
      @end_time = Time.now
      print_time
      PEROBS.log.info "#{@name} completed in " +
        secsToHMS(@end_time - @start_time)
    end

    private

    def print_bar
    end

    def print_time
    end

    def secsToHMS(secs)
      secs = secs.to_i
      s = secs % 60
      mins = secs / 60
      m = mins % 60
      h = mins / 60
      "#{h}:#{'%02d' % m}:#{'%02d' % s}"
    end

  end

end

