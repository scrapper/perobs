# encoding: UTF-8
#
# = BTreeBlob.rb -- Persistent Ruby Object Store
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


module PEROBS

  # This class manages the usage of the data blobs in the corresponding
  # HashedBlobsDB object.
  class BTreeBlob

    # For performance reasons we use an Array for the entries instead of a
    # Hash. These constants specify the Array index for the corresponding
    # value.
    ID = 0
    # Number of bytes
    BYTES = 1
    # Start Address
    START = 2
    # Mark/Unmarked flag
    MARKED = 3

    # Create a new BTreeBlob object.
    # @param dir [String] Fully qualified directory name
    # @param btreedb [BTreeDB] Reference to the DB that owns this blob
    def initialize(dir, btreedb)
      @dir = dir
      @btreedb = btreedb

      @index_file_name = File.join(dir, 'index')
      @blobs_file_name = File.join(dir, 'data')
      read_index
    end

    # Write the given bytes with the given ID into the DB.
    # @param id [Fixnum or Bignum] ID
    # @param raw [String] sequence of bytes
    def write_object(id, raw)
      if @entries.length > @btreedb.max_blob_size
        # The blob has reached the maximum size. Replace the blob with a BTree
        # node directory and distribute the blob entires into the sub-blobs of
        # the new BTree node.
        split_blob
        # Insert the passed object into the newly created BTree node.
        @btreedb.put_raw_object(raw, id)
      else
        bytes = raw.bytesize
        start_address = reserve_bytes(id, bytes)
        if write_to_blobs_file(raw, start_address) != bytes
          raise RuntimeError, 'Object length does not match written bytes'
        end
        write_index
      end
    end

    # Read the entry for the given ID and return it as bytes.
    # @param id [Fixnum or Bignum] ID
    # @return [String] sequence of bytes or nil if ID is unknown
    def read_object(id)
      return nil unless (bytes_and_start = find(id))
      read_from_blobs_file(*bytes_and_start)
    end


    # Find the data for the object with given id.
    # @param id [Fixnum or Bignum] Object ID
    # @return [Array] Returns an Array with two Fixnum entries. The first is
    #         the number of bytes and the second is the starting offset in the
    #         blob storage file.
    def find(id)
      if (entry = @entries_by_id[id])
        return [ entry[BYTES], entry[START] ]
      end

      nil
    end

    # Clear the mark on all entries in the index.
    def clear_marks
      @entries.each { |e| e[MARKED] = 0 }
      write_index
    end

    # Set a mark on the entry with the given ID.
    # @param id [Fixnum or Bignum] ID of the entry
    def mark(id)
      found = false
      @entries.each do |entry|
        if entry[ID] == id
          entry[MARKED] = 1
          found = true
          break
        end
      end

      unless found
        raise ArgumentError,
              "Cannot find an entry for ID #{'%016X' % id} #{id} to mark"
      end

      write_index
    end

    # Check if the entry for a given ID is marked.
    # @param id [Fixnum or Bignum] ID of the entry
    # @param ignore_errors [Boolean] If set to true no errors will be raised
    #        for non-existing objects.
    # @return [TrueClass or FalseClass] true if marked, false otherwise
    def is_marked?(id, ignore_errors = false)
      @entries.each do |entry|
        return entry[MARKED] != 0 if entry[ID] == id
      end

      return false if ignore_errors
      raise ArgumentError,
            "Cannot find an entry for ID #{'%016X' % id} to check"
    end

    # Remove all entries from the index that have not been marked.
    # @return [Array] List of deleted object IDs.
    def delete_unmarked_entries
      deleted_ids = []
      # First remove the entry from the hash table.
      @entries_by_id.delete_if do |id, e|
        if e[MARKED] == 0
          deleted_ids << id
          true
        else
          false
        end
      end
      # Then delete the entry itself.
      @entries.delete_if { |e| e[MARKED] == 0 }
      write_index

      deleted_ids
    end

    # Run a basic consistency check.
    # @param repair [TrueClass/FalseClass] Not used right now
    # @return [TrueClass/FalseClass] Always true right now
    def check(repair = false)
      # Determine size of the data blobs file.
      data_file_size = File.exists?(@blobs_file_name) ?
        File.size(@blobs_file_name) : 0

      next_start = 0
      prev_entry = nil
      @entries.each do |entry|
        # Entries should never overlap
        if prev_entry && next_start > entry[START]
          raise RuntimeError,
                "#{@dir}: Index entries are overlapping\n" +
                "ID: #{'%016X' % prev_entry[ID]}  " +
                "Start: #{prev_entry[START]}  " +
                "Bytes: #{prev_entry[BYTES]}\n" +
                "ID: #{'%016X' % entry[ID]}  Start: #{entry[START]}  " +
                "Bytes: #{entry[BYTES]}"
        end
        next_start = entry[START] + entry[BYTES]

        # Entries must fit within the data file
        if next_start > data_file_size
          raise RuntimeError,
                "#{@dir}: Entry for ID #{'%016X' % entry[ID]} " +
                "goes beyond 'data' file " +
                "size (#{data_file_size})\n" +
                "ID: #{'%016X' % entry[ID]}  Start: #{entry[START]}  " +
                "Bytes: #{entry[BYTES]}"
        end

        prev_entry = entry
      end

      true
    end

    private

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
      # Index where to insert the new entry. Append by default.
      best_fit_index = -1
      # If there is already an entry for an object with the _id_, we mark it
      # for deletion.
      entry_to_delete = nil

      @entries.each.with_index do |entry, i|
        if entry[ID] == id
          # We've found an old entry for this ID. Mark it for deletion.
          entry_to_delete = entry
          next
        end

        gap = entry[START] - end_of_last_entry
        if gap >= bytes &&
          (best_fit_bytes.nil? || gap < best_fit_bytes)
          # We've found a segment that fits the requested bytes and fits
          # better than any previous find.
          best_fit_start = end_of_last_entry
          best_fit_bytes = gap
          # The old entry gets deleted before the new one gets inserted. We
          # need to correct the index appropriately.
          best_fit_index = i - (entry_to_delete ? 1 : 0)
        end
        end_of_last_entry = entry[START] + entry[BYTES]
      end

      # Delete the old entry if requested.
      @entries.delete(entry_to_delete) if entry_to_delete

      # Create a new entry and insert it. The order must match the above
      # defined constants!
      # Object reads can trigger creation of new objects. As the marking
      # process triggers reads as well, all newly created objects are always
      # marked to prevent them from being collected right after creation.
      entry = [ id, bytes, best_fit_start || end_of_last_entry, 1 ]
      @entries.insert(best_fit_index, entry)
      @entries_by_id[id] = entry

      entry[START]
    end

    def read_index
      # The entries are stored in two data structures to provide the fastest
      # access mechanism for each situation. The Array @entries stores them in
      # a plan Array. @entries_by_id stores them hashed by their ID.
      @entries = []
      @entries_by_id = {}
      if File.exists?(@index_file_name)
        begin
          File.open(@index_file_name, 'rb') do |f|
            # The index is a binary format. Each entry has exactly 25 bytes.
            # Bytes
            #  0 -  7 : 64 bits, little endian : ID
            #  8 - 15 : 64 bits, little endian : Entry length in bytes
            # 16 - 23 : 64 bits, little endian : Start address in data file
            # 24      : 8 bits : 0 if unmarked, 1 if marked
            while (bytes = f.read(25))
              @entries << (e = bytes.unpack('QQQC'))
              @entries_by_id[e[ID]] = e
            end
          end
        rescue => e
          raise RuntimeError,
                "BTreeBlob file #{@index_file_name} corrupted: #{e.message}"
        end
      end
    end

    def write_index
      begin
        File.open(@index_file_name, 'wb') do |f|
          # See read_index for data format documentation.
          @entries.each do |entry|
            f.write(entry.pack('QQQC'))
          end
        end
      rescue => e
        raise RuntimeError,
              "Cannot write BTreeBlob index file #{@index_file_name}: " +
              e.message
      end
    end

    def split_blob
      # Rename the index file to hide the blob file from the DB.
      File.rename(@index_file_name, @index_file_name + '.bak')

      # Read all entries from the blob and re-store them into the DB. We've
      # already created the new BTree node, so these entries will be
      # distributed into new leaf blobs of this new node.
      @entries.each do |entry|
        raw = read_from_blobs_file(entry[BYTES], entry[START])
        @btreedb.put_raw_object(raw, entry[ID])
      end

      # Once the entries are re-stored, we can delete the old blob files.
      File.delete(@index_file_name + '.bak')
      File.delete(@blobs_file_name)
    end

  end

end

