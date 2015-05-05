$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'fileutils'
require 'time'

require 'perobs'

module PEROBS

  @@DBName = 'test_db'

  class PO < PersistentObject

    po_attr :name

    def initialize(store)
      super
    end

  end

  describe PersistentHash do

    before(:all) do
      FileUtils.rm_rf(@@DBName)
    end

    after(:each) do
      FileUtils.rm_rf(@@DBName)
    end

    it 'should store simple objects' do
      store = Store.new(@@DBName)
      store[:h] = h = PersistentHash.new(store)
      h['a'] = 'A'
      h['b'] = 'B'
      h['po'] = po = PO.new(store)
      po.name = 'foobar'
      h['b'] = 'B'

      h['a'].should == 'A'
      h['b'].should == 'B'
      store.sync

      store = Store.new(@@DBName)
      h = store[:h]
      h['a'].should == 'A'
      h['b'].should == 'B'
      h['po'].name.should == 'foobar'
    end

  end

end
