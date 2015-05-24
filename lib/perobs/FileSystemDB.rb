# encoding: UTF-8
#
# = FileSystemDB.rb -- Persistent Ruby Object Store
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

require 'time'
require 'json'
require 'json/add/core'
require 'json/add/struct'
require 'yaml'
require 'fileutils'

require 'perobs/ObjectBase'

module PEROBS

  # This class provides a filesytem based database store for objects.
  class FileSystemDB

    @@Extensions = {
      :marshal => '.mshl',
      :json => '.json',
      :yaml => '.yml'
    }

    # Create a new FileSystemDB object. This will create a DB with the given
    # name. A database will live in a directory of that name.
    # @param db_name [String] name of the DB directory
    def initialize(db_name, serializer = :json)
      @db_dir = db_name
      @serializer = serializer

      # Create the database directory if it doesn't exist yet.
      ensure_dir_exists(@db_dir)
    end

    # Return true if the object with given ID exists
    # @param id [Fixnum or Bignum]
    def include?(id)
      File.exists?(object_file_name(id))
    end

    # Store the given object into the filesystem.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      raw = case @serializer
            when :marshal
              Marshal.dump(obj)
            when :json
              obj.to_json
            when :yaml
              YAML.dump(obj)
            end
      File.write(object_file_name(id), raw)
    end

    # Load the given object from the filesystem.
    # @param id [Fixnum or Bignum] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase
    def get_object(id)
      begin
        raw = File.read(file_name = object_file_name(id))
        case @serializer
        when :marshal
          Marshal.load(raw)
        when :json
          JSON.parse(raw, :create_additions => true)
        when :yaml
          YAML.load(raw)
        end
      rescue => e
        raise RuntimeError, "Error in #{file_name}: #{e.message}"
      end
    end

    # Generate a new unique ID.
    # @return [Fixnum or Bignum]
    def new_id
      begin
        # Generate a random number. It's recommended to not store more than
        # 2**62 objects in the same store.
        id = rand(2**64)
        # Ensure that we don't have already another object with this ID.
      end while include?(id)

      id
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      @mark_start = Time.now
      # The filesystem stores access times with second granularity. We need to
      # wait 1 sec. to ensure that all marks are noticeable.
      sleep(1)
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    def delete_unmarked_objects
      Dir.glob(File.join(@db_dir, '*')) do |dir|
        next unless Dir.exists?(dir)

        Dir.glob(File.join(dir, '*')) do |file|
          if File.atime(file) <= @mark_start
            File.delete(file)
          end
        end
      end
    end

    # Mark an object.
    # @param id [Fixnum or Bignum] ID of the object to mark
    def mark(id)
      FileUtils.touch(object_file_name(id))
    end

    # Check if the object is marked.
    # @param id [Fixnum or Bignum] ID of the object to check
    def is_marked?(id)
      File.atime(object_file_name(id)) > @mark_start
    end

    # Check if the stored object is syntactically correct.
    # @param id [Fixnum/Bignum] Object ID
    # @param repair [TrueClass/FalseClass] True if an repair attempt should be
    #        made.
    # @return [TrueClass/FalseClass] True if the object is OK, otherwise
    #         false.
    def check(id, repair)
      file_name = object_file_name(id)
      unless File.exists?(file_name)
        $stderr.puts "Object file for ID #{id} does not exist"
        return false
      end

      begin
        get_object(id)
      rescue => e
        $stderr.puts "Cannot read object file #{file_name}: #{e.message}"
        return false
      end

      true
    end

    private

    # Ensure that we have a directory to store the DB items.
    def ensure_dir_exists(dir)
      unless Dir.exists?(dir)
        begin
          Dir.mkdir(dir)
        rescue IOError => e
          raise IOError, "Cannote create DB directory '#{dir}': #{e.message}"
        end
      end
    end

    # Determine the file name to store the object. The object ID determines
    # the directory and file name inside the store.
    # @param id [Fixnum or Bignum] ID of the object
    def object_file_name(id)
      hex_id = "%08X" % id
      dir = hex_id[0..1]
      ensure_dir_exists(File.join(@db_dir, dir))

      File.join(@db_dir, dir, hex_id + @@Extensions[@serializer])
    end

  end

end

