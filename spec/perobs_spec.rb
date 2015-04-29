$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'fileutils'
require 'time'
require 'perobs'

module PEROBS

  class Person < PersistentObject

    attribute 'String', :name, ''
    attribute 'Integer', :zip
    attribute 'Float', :bmi, 22.2
    attribute 'Boolean', :married, false
    attribute 'Time', :birthdate
    attribute 'Reference', :related

    def initialize
      super
    end

  end

  describe Store do

    before(:each) do
      FileUtils.rm_rf('test_db')
    end

    after(:each) do
      #FileUtils.rm_rf('test_db')
    end

    it 'should store and retrieve simple objects' do
      store = Store.new('test_db')
      store[:john] = john = Person.new
      john.name = 'John'
      john.birthdate = Time.parse('2015-04-20')
      john.zip = 4060
      john.bmi = 25.5
      store[:jane] = jane = Person.new
      jane.name = 'Jane'
      jane.birthdate = Time.parse('2015-04-21')
      jane.related = john
      jane.married = true

      store.sync

      store = Store.new('test_db')
      john = store[:john]
      john.name.should == 'John'
      john.birthdate.should == Time.parse('2015-04-20')
      john.zip.should == 4060
      john.bmi.should == 25.5
      john.married.should be_false
      john.related.should be_nil
      jane = store[:jane]
      jane.name.should == 'Jane'
      jane.birthdate.should == Time.parse('2015-04-21')
      jane.related.should == john
      jane.married.should be_true
    end

    it 'should flush objects when reaching the threshold' do
      store = Store.new('test_db')
      store.max_objects = 10
      store.flush_count = 5
      last_obj = nil
      0.upto(20) do |i|
        if i <= 10
          store.length.should == i
        elsif i <= 15
          store.length.should == i - 5
        else
          store.length.should == i - 10
        end
        store[":person#{i}".to_sym] = obj = Person.new
        obj.name = "Person #{i}"
        obj.birthdate = Time.parse("2015-04-#{i+1}")
        obj.related = last_obj if last_obj
        last_obj = obj
      end
    end

    it 'should detect modification to non-working objects' do
      store = Store.new('test_db')
      store.max_objects = 5
      store.flush_count = 5
      store[:person] = obj = Person.new
      obj.name = "John"
      0.upto(5) do |i|
        store[":person#{i}".to_sym] = tobj = Person.new
        tobj.name = "Person #{i}"
      end
      obj.name = "Jim"
      store.sync
      store = Store.new('test_db')
      obj = store[:person]
      obj.name.should == 'Jim'
    end

    it 'should garbage collect unlinked objects' do
      store = Store.new('test_db')
      store[:person1] = obj = Person.new
      id1 = obj.id
      store[:person2] = obj = Person.new
      id2 = obj.id
      obj.related = obj = Person.new
      id3 = obj.id
      store.sync
      store[:person1] = nil
      store.gc
      store = Store.new('test_db')
      store.object_by_id(id1).should be_nil
      store[:person2].id.should == id2
      store[:person2].related.id.should == id3
    end

  end

end

