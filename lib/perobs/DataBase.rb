# encoding: UTF-8
#
# = DataBase.rb -- Persistent Ruby Object Store
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

  # Base class for all storage back-ends.
  class DataBase

    def initialize(serializer = :json)
      @serializer = serializer
      @config = {}
    end

    # Serialize the given object using the object serializer.
    # @param obj [ObjectBase] Object to serialize
    # @return [String] Serialized version
    def serialize(obj)
      begin
        case @serializer
        when :marshal
          Marshal.dump(obj)
        when :json
          obj.to_json
        when :yaml
          YAML.dump(obj)
        end
      rescue => e
        raise RuntimeError,
              "Cannot serialize object as #{@serializer}: #{e.message}"
      end
    end

    # De-serialize the given String into a Ruby object.
    # @param raw [String]
    # @return [Hash] Deserialized version
    def deserialize(raw)
      begin
        case @serializer
        when :marshal
          Marshal.load(raw)
        when :json
          JSON.parse(raw, :create_additions => true)
        when :yaml
          YAML.load(raw)
        end
      rescue => e
        raise RuntimeError,
              "Cannot de-serialize object with #{@serializer} parser: " +
              e.message
      end
    end

    # Check a config option and adjust it if needed.
    # @param name [String] Name of the config option.
    def check_option(name)
      value = instance_variable_get('@' + name)

      if @config.include?(name)
        # The database already existed and has a setting for this config
        # option. If it does not match the instance variable, adjust the
        # instance variable accordingly.
        unless @config[name] == value
          instance_variable_set('@' + name, @config[name])
        end
      else
        # There is no such config option yet. Create it with the value of the
        # corresponding instance variable.
        @config[name] = value
      end
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

  end

end
