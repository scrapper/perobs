$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'fileutils'
require 'time'

require 'perobs'

DBName = 'test_db'

class PO < PEROBS::Object

  po_attr :name

  def initialize(store, name = nil)
    super(store)
    set(:name, name)
  end

end

describe PEROBS::Hash do

  before(:all) do
    FileUtils.rm_rf(DBName)
  end

  after(:each) do
    FileUtils.rm_rf(DBName)
  end

  it 'should store simple objects persistently' do
    store = PEROBS::Store.new(DBName)
    store[:h] = h = PEROBS::Hash.new(store)
    h['a'] = 'A'
    h['b'] = 'B'
    h['po'] = po = PO.new(store)
    po.name = 'foobar'
    h['b'] = 'B'

    h['a'].should == 'A'
    h['b'].should == 'B'
    store.sync

    store = PEROBS::Store.new(DBName)
    h = store[:h]
    h['a'].should == 'A'
    h['b'].should == 'B'
    h['po'].name.should == 'foobar'
  end

  it 'should have an each method to iterate' do
    store = PEROBS::Store.new(DBName)
    store[:h] = h = PEROBS::Hash.new(store)
    h['a'] = 'A'
    h['b'] = 'B'
    h['c'] = 'C'
    vs = []
    h.each { |k, v| vs << k + v }
    vs.sort.join.should == 'aAbBcC'

    store = PEROBS::Store.new(DBName)
    store[:h] = h = PEROBS::Hash.new(store)
    h['a'] = PO.new(store, 'A')
    h['b'] = PO.new(store, 'B')
    h['c'] = PO.new(store, 'C')
    vs = []
    h.each { |k, v| vs << k + v.name }
    vs.sort.join.should == 'aAbBcC'
  end

end
