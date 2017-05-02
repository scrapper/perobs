# encoding: UTF-8
#
# = LockFile.rb -- Persistent Ruby Object Store
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

require 'perobs/Log'

module PEROBS

  # This class implements a file based lock. It can only be taken by one
  # process at a time. It support configurable lock lifetime, maximum retries
  # and pause between retries.
  class LockFile

    # Create a new lock for the given file.
    # @param file_name [String] file name of the lock file
    # @param options [Hash] See case statement
    def initialize(file_name, options = {})
      @file_name = file_name
      # The handle of the lock file
      @file = nil
      # The maximum duration after which a lock file is considered a left-over
      # from a dead or malefunctioning process.
      @timeout_secs = 60 * 60
      # The maximum number of times we try to get the lock.
      @max_retries = 5
      # The time we wait between retries
      @pause_secs = 1

      options.each do |name, value|
        case name
        when :timeout_secs
          @timeout_secs = value
        when :max_retries
          @max_retries = value
        when :pause_secs
          @pause_secs = value
        else
          PEROBS.log.fatal "Unknown option #{name}"
        end
      end
    end

    # Attempt to take the lock.
    # @return [Boolean] true if lock was taken, false otherwise
    def lock
      retries = @max_retries
      while retries > 0
        begin
          @file = File.open(@file_name, File::RDWR | File::CREAT, 0644)

          if @file.flock(File::LOCK_EX | File::LOCK_NB)
            # We have taken the lock. Write the PID into the file and leave it
            # open.
            @file.write($$)
            @file.flush
            @file.truncate(@file.pos)
            PEROBS.log.debug "Lock file #{@file_name} has been taken for " +
              "process #{$$}"

            return true
          else
            # We did not manage to take the lock file.
            if @file.mtime <= Time.now - @timeout_secs
              pid = @file.read.to_i
              PEROBS.log.info "Old lock file found for PID #{pid}. " +
                "Removing lock."
              if is_running?(pid)
                send_signal('TERM', pid)
                # Give the process 3 seconds to terminate gracefully.
                sleep 3
                # Then send a SIGKILL to ensure it's gone.
                send_signal('KILL', pid) if is_running?(pid)
              end
              @file.close
              File.delete(@file_name) if File.exist?(@file_name)
            else
              PEROBS.log.debug "Lock file #{@file_name} is taken. Trying " +
                "to get it #{retries} more times."
            end
          end
        rescue => e
          PEROBS.log.error "Cannot take lock file #{@file_name}: #{e.message}"
          return false
        end

        retries -= 1
        sleep(@pause_secs)
      end

      PEROBS.log.info "Failed to get lock file #{@file_name} due to timeout"
      false
    end

    # Check if the lock has been taken.
    # @return [Boolean] true if taken, false otherweise.
    def is_locked?
      File.exist?(@file_name)
    end

    # Release the lock again.
    def unlock
      unless @file
        PEROBS.log.error "There is no current lock to release"
        return false
      end

      begin
        @file.flock(File::LOCK_UN)
        @file.close
        forced_unlock
        PEROBS.log.debug "Lock file #{@file_name} for PID #{$$} has been " +
          "released"
      rescue => e
        PEROBS.log.error "Releasing of lock file #{@file_name} failed: " +
          e.message
        return false
      end

      true
    end

    # Erase the lock file. It's essentially a forced unlock method.
    def forced_unlock
      @file = nil
      if File.exist?(@file_name)
        begin
          File.delete(@file_name)
          PEROBS.log.debug "Lock file #{@file_name} has been deleted."
        rescue IOError => e
          PEROBS.log.error "Cannot delete lock file #{@file_name}: " +
            e.message
        end
      end
    end

    private

    def send_signal(name, pid)
      begin
        Process.kill(name, pid)
      rescue => e
        PEROBS.log.info "Process kill error: #{e.message}"
      end
    end

    def is_running?(pid)
      begin
        Process.getpgid(pid)
        true
      rescue Errno::ESRCH
        false
      end
    end

  end

end

