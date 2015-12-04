# encoding: UTF-8
#
# = Delegator.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015 by Chris Schlaeger <chris@taskjuggler.org>
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

require 'perobs/ClassMap'

module PEROBS

  module Delegator

    # Proxy all calls to unknown methods to the data object.
    def method_missing(method_sym, *args, &block)
      if self.class::READERS.include?(method_sym)
        # If any element of this class is read, we register this object as
        # being read with the cache.
        @store.cache.cache_read(self)
        @data.send(method_sym, *args, &block)
      elsif self.class::REWRITERS.include?(method_sym)
        # Re-writers don't introduce any new elements. We just mark the object
        # as written in the cache and call the class' method.
        @store.cache.cache_write(self)
        @data.send(method_sym, *args, &block)
      else
        # Any method we don't know about must cause an error. A new class
        # method needs to be added to the right bucket first.
        raise NoMethodError.new("undefined method '#{method_sym}' for " +
                                "#{self.class}")
      end
    end

    def respond_to?(method_sym, include_private = false)
      (self.class::READERS + self.class::REWRITERS).include?(method_sym) ||
        super
    end

  end

end

