# PEROBS - PErsistent Ruby OBject Store

PEROBS is a library that provides a persistent object store for Ruby
objects. Objects of your classes can be made persistent by deriving
them from PEROBS::Object. They will be in memory when needed and
transparently stored into a persistent storage. Currently only
filesystem based storage is supported, but back-ends for key/value
databases can be easily added.

This library is ideal for Ruby applications that work on huge, mostly
constant data sets and usually handle a small subset of the data at a
time. To ensure data consistency of a larger data set, you can use
transactions to make modifications of multiple objects atomic.
Transactions can be nested and are aborted when an exception is
raised.

## Usage

It features a garbage collector that removes all objects that are no
longer in use. A build-in cache keeps access latencies to recently
used objects low and lazily flushes modified objects into the
persistend back-end.

Persistent objects must be created by deriving your class from
PEROBS::Object. Only instance variables that are declared via
po_attr will be persistent. All objects that are stored in persitant
instance variables must provide a to_json method that generates JSON
syntax that can be parsed into their original object again. It is
recommended that references to other objects are all going to persistent
objects again.

There are currently 3 kinds of persistent objects available:

* PEROBS::Object is the base class for all your classes that should be
  persistent.

* PEROBS::Array provides an interface similar to the built-in Array class
  but its objects are automatically stored.

* PEROBS::Hash provides an interface similar to the built-in Hash
  class but its objects are automatically stored.

In addition to these classes, you also need to create a PEROBS::Store
object that owns your persistent objects. The store provides the
persistent database. If you are using the default serializer (JSON),
you can only use the subset of Ruby types that JSON supports.
Alternatively, you can use Marshal or YAML which support almost every
Ruby data type.

Here is an example how to use PEROBS. Let's define a class that models
a person with their family relations.

```
require 'perobs'

class Person < PEROBS::Object

  po_attr :name, :mother, :father, :kids

  def initialize(store, name)
    super
    attr_init(:name, name)
    attr_init(:kids, PEROBS::Array.new(store))
  end

  def to_s
    "#{@name} is the child of #{self.mother ? self.mother.name : 'unknown'} " +
    "and #{self.father ? self.father.name : 'unknown'}.
  end

end

store = PEROBS::Store.new('family')
store['grandpa'] = joe = Person.new(store, 'Joe')
store['grandma'] = jane = Person.new(store, 'Jane')
jim = Person.new(store, 'Jim')
jim.father = joe
joe.kids << jim
jim.mother = jane
jane.kids << jim
store.sync
```

When you run this script, a folder named 'family' will be created. It
contains the 3 Person objects.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'perobs'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install perobs

## Contributing

1. Fork it ( https://github.com/scrapper/perobs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
