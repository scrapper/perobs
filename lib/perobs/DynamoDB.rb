# encoding: UTF-8
#
# = DynamoDB.rb -- Persistent Ruby Object Store
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

require 'aws-sdk-dynamodb'

require 'perobs/DataBase'
require 'perobs/BTreeBlob'

module PEROBS

  # This class implements an Amazon DynamoDB storage engine for PEROBS.
  class DynamoDB < DataBase

    # Create a new DynamoDB object.
    # @param db_name [String] name of the DB directory
    # @param options [Hash] options to customize the behavior. Currently only
    #        the following options are supported:
    #        :serializer    : Can be :json and :yaml
    #        :aws_id        : AWS credentials ID
    #        :aws_key       : AWS credentials key
    #        :aws_region    : AWS region to host the data
    def initialize(db_name, options = {})
      # :marshal serialization results in a binary format that cannot easily
      # be stored in DynamoDB. We fall back to :yaml.
      if options[:serializer] == :marshal
        options[:serializer] = :yaml
      end

      super(options[:serializer] || :json)

      if options.include?(:aws_id) && options.include?(:aws_key)
        Aws.config[:credentials] = Aws::Credentials.new(options[:aws_id],
                                                        options[:aws_key])
      end
      if options.include?(:aws_region)
        Aws.config[:region] = options[:aws_region]
      end

      @dynamodb = Aws::DynamoDB::Client.new
      @table_name = db_name
      ensure_table_exists(@table_name)

      # Read the existing DB config.
      @config = get_hash('config')
      check_option('serializer')
      put_hash('config', @config)
    end

    # Delete the entire database. The database is no longer usable after this
    # method was called.
    def delete_database
      dynamodb = Aws::DynamoDB::Client.new
      dynamodb.delete_table(:table_name => @table_name)
      dynamodb.wait_until(:table_not_exists, table_name: @table_name)
    end

    def DynamoDB::delete_db(table_name)
      dynamodb = Aws::DynamoDB::Client.new
      dynamodb.delete_table(:table_name => table_name)
      dynamodb.wait_until(:table_not_exists, table_name: table_name)
    end

    # Return true if the object with given ID exists
    # @param id [Integer]
    def include?(id)
      !dynamo_get_item(id.to_s).nil?
    end

    # Store a simple Hash as a JSON encoded file into the DB directory.
    # @param name [String] Name of the hash. Will be used as file name.
    # @param hash [Hash] A Hash that maps String objects to strings or
    # numbers.
    def put_hash(name, hash)
      dynamo_put_item(name, hash.to_json)
    end

    # Load the Hash with the given name.
    # @param name [String] Name of the hash.
    # @return [Hash] A Hash that maps String objects to strings or numbers.
    def get_hash(name)
      if (item = dynamo_get_item(name))
        JSON.parse(item)
      else
        ::Hash.new
      end
    end

    # Store the given object into the cluster files.
    # @param obj [Hash] Object as defined by PEROBS::ObjectBase
    def put_object(obj, id)
      dynamo_put_item(id.to_s, serialize(obj))
    end

    # Load the given object from the filesystem.
    # @param id [Integer] object ID
    # @return [Hash] Object as defined by PEROBS::ObjectBase or nil if ID does
    #         not exist
    def get_object(id)
      (item = dynamo_get_item(id.to_s)) ? deserialize(item) : nil
    end

    # This method must be called to initiate the marking process.
    def clear_marks
      each_item do |id|
        dynamo_mark_item(id, false)
      end
      # Mark the 'config' item so it will not get deleted.
      dynamo_mark_item('config')
    end

    # Permanently delete all objects that have not been marked. Those are
    # orphaned and are no longer referenced by any actively used object.
    # @return [Array] List of object IDs of the deleted objects.
    def delete_unmarked_objects
      deleted_ids = []
      each_item do |id|
        unless dynamo_is_marked?(id)
          dynamo_delete_item(id)
          deleted_ids << id
        end
      end

      deleted_ids
    end

    # Mark an object.
    # @param id [Integer] ID of the object to mark
    def mark(id)
      dynamo_mark_item(id.to_s, true)
    end

    # Check if the object is marked.
    # @param id [Integer] ID of the object to check
    def is_marked?(id)
      dynamo_is_marked?(id.to_s)
    end

    # Basic consistency check.
    # @param repair [TrueClass/FalseClass] True if found errors should be
    #        repaired.
    def check_db(repair = false)
      # TODO: See if we can add checks here
    end

    # Check if the stored object is syntactically correct.
    # @param id [Integer] Object ID
    # @param repair [TrueClass/FalseClass] True if an repair attempt should be
    #        made.
    # @return [TrueClass/FalseClass] True if the object is OK, otherwise
    #         false.
    def check(id, repair)
      begin
        get_object(id)
      rescue => e
        PEROBS.log.error "Cannot read object with ID #{id}: #{e.message}"
        return false
      end

      true
    end

    private

    def ensure_table_exists(table_name)
      begin
        @dynamodb.describe_table(:table_name => table_name)
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        @dynamodb.create_table(
          :table_name => table_name,
          :attribute_definitions => [
            {
              :attribute_name => :Id,
              :attribute_type => :S
            }
          ],
          :key_schema => [
            {
              :attribute_name => :Id,
              :key_type => :HASH
            }
          ],
          :provisioned_throughput => {
            :read_capacity_units => 1,
            :write_capacity_units => 1,
          }
        )

        @dynamodb.wait_until(:table_exists, table_name: table_name)
      end
    end

    def dynamo_get_item(id)
      resp = @dynamodb.get_item(:table_name => @table_name,
                                :key => { :Id => id })
      resp[:item] ? resp[:item]['Value'] : nil
    end

    def dynamo_put_item(id, value)
      @dynamodb.put_item(:table_name => @table_name,
                         :item => { :Id => id, :Value => value })
    end

    def dynamo_delete_item(id)
      @dynamodb.delete_item(:table_name => @table_name,
                            :key => { :Id => id })
    end

    def dynamo_mark_item(id, set_mark = true)
      @dynamodb.update_item(:table_name => @table_name,
                            :key => { :Id => id },
                            :attribute_updates => {
                              :Mark => { :value => set_mark,
                                         :action => "PUT" }})
    end

    def dynamo_is_marked?(id)
      resp = @dynamodb.get_item(:table_name => @table_name,
                               :key => { :Id => id })
      resp[:item] && resp[:item]['Mark']
    end

    def each_item
      start_key = nil
      loop do
        resp = @dynamodb.scan(:table_name => @table_name,
                              :exclusive_start_key => start_key)
        break if resp.count <= 0

        resp.items.each do |item|
          yield(item['Id'])
        end

        break unless resp.last_evaluated_key
        start_key = resp.last_evaluated_key['AttributeName']
      end
    end

  end

end

