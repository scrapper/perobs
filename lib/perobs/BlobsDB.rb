# encoding: UTF-8
#
# = BlobsDB.rb -- Persistent Ruby Object Store
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

require 'json'
require 'json/add/core'
require 'json/add/struct'

module PEROBS

  # This class manages the usage of the data blobs in the corresponding
  # HashedBlobsDB object.
  class BlobsDB

    # Create a new BlobsDB object.
    def initialize(dir)
      @dir = dir

      @index_file_name = File.join(dir, 'index.json')
      @blobs_file_name = File.join(dir, 'data')
      read_index
    end

    # Write the given bytes with the given ID into the DB.
    # @param id [Fixnum or Bignum] ID
    # @param raw [String] sequence of bytes
    def write_object(id, raw)
      bytes = raw.bytesize
      start_address = reserve_bytes(id, bytes)
      if write_to_blobs_file(raw, start_address) != bytes
        raise RuntimeError, 'Object length does not match written bytes'
      end
      write_index
    end

    # Read the entry for the given ID and return it as bytes.
    # @param id [Fixnum or Bignum] ID
    # @return [String] sequence of bytes
    def read_object(id)
      read_from_blobs_file(*find(id))
    end


    # Find the data for the object with given id.
    # @param id [Fixnum or Bignum] Object ID
    # @return [Array] Returns an Array with two Fixnum entries. The first is
    #         the number of bytes and the second is the starting offset in the
    #         blob storage file.
    def find(id)
      @entries.each do |entry|
        if entry['id'] == id
          return [ entry['bytes'], entry['start'] ]
        end
      end

      nil
    end

    # Write a string of bytes into the file at the given address.
    # @param raw [String] bytes to write
    # @param address [Fixnum] offset in the file
    # @return [Fixnum] number of bytes written
    def write_to_blobs_file(raw, address)
      begin
        File.write(@blobs_file_name, raw, address)
      rescue => e
        raise IOError,
              "Cannot write blobs file #{@blobs_file_name}: #{e.message}"
      end
    end

    # Read _bytes_ bytes from the file starting at offset _address_.
    # @param bytes [Fixnum] number of bytes to read
    # @param address [Fixnum] offset in the file
    def read_from_blobs_file(bytes, address)
      begin
        File.read(@blobs_file_name, bytes, address)
      rescue => e
        raise IOError,
              "Cannot read blobs file #{@blobs_file_name}: #{e.message}"
      end
    end

    # Clear the mark on all entries in the index.
    def clear_marks
      @entries.each { |e| e['marked'] = false}
      write_index
    end

    # Set a mark on the entry with the given ID.
    # @param id [Fixnum or Bignum] ID of the entry
    def mark(id)
      found = false
      @entries.each do |entry|
        if entry['id'] == id
          entry['marked'] = true
          found = true
          break
        end
      end

      unless found
        raise ArgumentError, "Cannot find an entry for ID #{id} to mark"
      end

      write_index
    end

    # Check if the entry for a given ID is marked.
    # @param id [Fixnum or Bignum] ID of the entry
    # @return [TrueClass or FalseClass] true if marked, false otherwise
    def is_marked?(id)
      @entries.each do |entry|
        return entry['marked'] if entry['id'] == id
      end

      raise ArgumentError, "Cannot find an entry for ID #{id} to check"
    end

    # Remove all entries from the index that have not been marked.
    def delete_unmarked_entries
      @entries.delete_if { |e| e['marked'] == false }
      write_index
    end

    private

    # Reserve the bytes needed for the specified number of bytes with the
    # given ID.
    # @param id [Fixnum or Bignum] ID of the entry
    # @param bytes [Fixnum] number of bytes for this entry
    # @return [Fixnum] the start address of the reserved blob
    def reserve_bytes(id, bytes)
      # index of first blob after the last seen entry
      end_of_last_entry = 0
      # blob index of best fit segment
      best_fit_start = nil
      # best fir segment size in bytes
      best_fit_bytes = nil
      # If there is already an entry for an object with the _id_, we mark it
      # for deletion.
      entry_to_delete = nil

      @entries.each do |entry|
        if entry['id'] == id
          # We've found an old entry for this ID. Mark it for deletion.
          entry_to_delete = entry
          next
        end

        gap = entry['start'] - end_of_last_entry
        if gap >= bytes &&
          (best_fit_bytes.nil? || gap < best_fit_bytes)
          # We've found a segment that fits the requested bytes and fits
          # better than any previous find.
          best_fit_start = end_of_last_entry
          best_fit_bytes = gap
        end
        end_of_last_entry = entry['start'] + entry['bytes']
      end

      # Delete the old entry if requested.
      @entries.delete(entry_to_delete) if entry_to_delete

      # Create a new entry and insert it.
      entry = {
        'id' => id,
        'bytes' => bytes,
        'start' => best_fit_start || end_of_last_entry,
        'marked' => false
      }
      @entries << entry
      @entries.sort! { |e1, e2| e1['start'] <=> e2['start'] }

      entry['start']
    end

    def read_index
      @entries = []
      if File.exists?(@index_file_name)
        begin
          #File.open(@index_file_name, 'rb') do |f|
          #  while !f.eof?
          #    ea = f.read(8 + 8 + 8 + 1).unpack('QQQC')
          #    @entries << {
          #      'id' => ea[0],
          #      'bytes' => ea[1],
          #      'start' => ea[2],
          #      'marked' => ea[3] == 1
          #    }
          #  end
          #end
          @entries = JSON.parse(File.read(@index_file_name))
        rescue => e
          raise RuntimeError,
                "BlobsDB file #{@index_file_name} corrupted: #{e.message}"
        end
      else
        @entries = []
      end
    end

    def write_index
      begin
        #File.open(@index_file_name, 'wb') do |f|
        #  @entries.each do |e|
        #    ea = [ e['id'], e['bytes'], e['start'], e['marked'] ? 1 : 0 ]
        #    f.write(ea.pack('QQQC'))
        #  end
        #end
        File.write(@index_file_name, @entries.to_json)
      rescue => e
        raise RuntimeError,
              "Cannot write BlobsDB index file #{@index_file_name}: " +
              e.message
      end
    end

  end

end

