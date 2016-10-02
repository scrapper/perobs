# encoding: UTF-8
#
# = Log.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
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
#!/usr/bin/env ruby -w
# encoding: UTF-8
#

require 'monitor'
require 'logger'
require 'singleton'

module PEROBS

  # This is the Exception type that will be thrown for all unrecoverable
  # library internal (program logic) errors.
  class FatalError < StandardError ; end

  # This is the Exception type that will be thrown for all program errors that
  # are caused by user error rather than program logic errors.
  class UsageError < StandardError ; end

  # The ILogger class is a singleton that provides a common logging mechanism
  # to all objects. It exposes essentially the same interface as the Logger
  # class, just as a singleton and extends fatal to raise an FatalError
  # exception.
  class ILogger < Monitor

    include Singleton

    # Default options to create a logger. Keep 4 log files, each 1MB max.
    @@options = [ 4, 2**20 ]
    @@level = Logger::INFO
    @@formatter = proc do |severity, time, progname, msg|
      "#{time} #{severity} #{msg}\n"
    end
    @@logger = nil

    # Set log level.
    # @param l [Logger::WARN, Logger:INFO, etc]
    def level=(l)
      @@level = l
    end

    # Set Logger formatter.
    # @param f [Proc]
    def formatter=(f)
      @@formatter = f
    end

    # Set Logger options
    # @param o [Array] Optional parameters for Logger.new().
    def options=(o)
      @@options = o
    end

    # Redirect all log messages to the given IO.
    # @param io [IO] Output file descriptor
    def open(io)
      begin
        @@logger = Logger.new(io, *@@options)
      rescue IOError => e
        @@logger = Logger.new($stderr)
        $stderr.puts "Cannot open log file: #{e.message}"
      end
      @@logger.level = @@level
      @@logger.formatter = @@formatter
    end

    # Pass all calls to unknown methods to the @@logger object.
    def method_missing(method, *args, &block)
      @@logger.send(method, *args, &block)
    end

    # Make it properly introspectable.
    def respond_to?(method, include_private = false)
      @@logger.respond_to?(method)
    end

    # Print an error message via the Logger and raise a Fit4Ruby::Error.
    # This method should be used to abort the program in case of program logic
    # errors.
    def fatal(msg, &block)
      @@logger.fatal(msg, &block)
      raise FatalError, msg
    end

  end

  class << self

    ILogger.instance.open($stderr)

    # Convenience method to we can use PEROBS::log instead of
    # PEROBS::ILogger.instance.
    def log
      ILogger.instance
    end

  end

end

