# encoding: UTF-8
#
# = IDListPage.rb -- Persistent Ruby Object Store
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

module PEROBS

  class IDListPage

    attr_reader :uid, :values
    attr_accessor :record

    def initialize(page_file, record, uid, values = [])
      @page_file = page_file
      @record = record
      @uid = uid
      @values = values
      @record.page_entries = @values.length
    end

    def IDListPage::load(page_file, uid, ref)
      page_file.load(uid, ref)
    end

    def is_full?
      @values.length >= @page_file.page_size
    end

    def length
      @values.length
    end

    def save
      @page_file.save_page(self)
    end

    def insert(id)
      if is_full?
        raise ArgumentError, "IDListPage is already full"
      end
      index = @values.bsearch_index { |v| v >= id } || @values.length

      # If the value isn't stored already, insert it.
      if @values[index] != id
        @values.insert(index, id)
        @record.page_entries = @values.length
        @page_file.mark_page_as_modified(self)
      end
    end

    def include?(id)
      !(v = @values.bsearch { |v| v >= id }).nil? && v == id
    end

    def delete(max_id)
      a = []
      @values.delete_if { |v| v > max_id ? a << v : false }

      unless a.empty?
        @record.page_entries = @values.length
        @page_file.mark_page_as_modified(self)
      end

      a
    end

    def check
      last_value = nil
      @values.each_with_index do |v, i|
        if last_value && last_value >= v
          raise RuntimeError, "The values #{last_value} and #{v} must be " +
            "strictly ascending: #{@values.inspect}"
        end
        last_value = v
      end
    end

    def to_s
      "[ #{@values.join(', ')} ]"
    end

  end

end

