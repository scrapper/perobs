# encoding: UTF-8
#
# Copyright (c) 2019 by Chris Schlaeger <chris@taskjuggler.org>
#
# This file contains tests for Array that are similar to the tests for the
# Array implementation in MRI. The ideas of these tests were replicated in
# this code.
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

require 'spec_helper'

require 'perobs'

describe PEROBS::FNV_Hash_1a_64 do

  it 'should generate stable hashes for Strings' do
    refs = [
      [ 'foo', 15902901984413996407 ],
      [ 'foo', 15902901984413996407 ],
      [ 'bar', 16101355973854746 ],
      [ 'foobar', 9625390261332436968 ],
      [ 'PEROBS rocks your application!', 4089220442501866848 ],
      [ 'Permission is hereby granted, free of charge, to any person ' +
        'obtaining a copy of this software and associated documentation ' +
        'files (the "Software"), to deal in the Software without ' +
        'restriction, including without limitation the rights to use, ' +
        'copy, modify, merge, publish, distribute, sublicense, and/or ' +
        'sell copies of the Software, and to permit persons to whom the ' +
        'Software is furnished to do so, subject to the following conditions:',
        17637146001033534275 ]
    ]

    refs.each do |v|
      expect(PEROBS::FNV_Hash_1a_64::digest(v[0])).to eql(v[1])
    end
  end

end

