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
persistend back-end when not using transactions.

Persistent objects must be created by deriving your class from
PEROBS::Object. Only instance variables that are declared via
po_attr will be persistent. All objects that are stored in persistent
instance variables must provide a to_json() method that generates JSON
syntax that can be also parsed into their original object again. It is
required that references to other objects are all going to persistent
objects again.

There are currently 3 kinds of persistent objects available:

* PEROBS::Object is the base class for all your classes that should be
  persistent.

* PEROBS::Array provides an interface similar to the built-in Array class
  but its objects are automatically stored.

* PEROBS::Hash provides an interface similar to the built-in Hash
  class but its objects are automatically stored.

You must create at least one PEROBS::Store object that owns your
persistent objects. The store provides the persistent database. If you
are using the default serializer (JSON), you can only use the subset
of Ruby types that JSON supports.  Alternatively, you can use Marshal
or YAML which support almost every Ruby data type.

Here is an example how to use PEROBS. Let's define a class that models
a person with their family relations.

```
require 'perobs'

class Person < PEROBS::Object

  po_attr :name, :mother, :father, :kids, :spouse, :status

  def initialize(store, name)
    super
    attr_init(:name, name)
    attr_init(:kids, store.new(PEROBS::Array))
    attr_init(:status, :single)
  end

  def merry(spouse)
    self.spouse = spouse
    self.status = :married
  end

  def to_s
    "#{@name} is the child of #{@mother ? @mother.name : 'unknown'} " +
    "and #{@father ? @father.name : 'unknown'}.
  end

end

store = PEROBS::Store.new('family')
store['grandpa'] = joe = store.new(Person, 'Joe')
store['grandma'] = jane = store.new(Person, 'Jane')
jim = store.new(Person, 'Jim')
jim.father = joe
joe.kids << jim
jim.mother = jane
jane.kids << jim
store.sync
```

When you run this script, a folder named 'family' will be created. It
contains the 3 Person objects.

### Accessing persistent instance variables

All instance variables that should be persisted must be declared with
'po_attr'. This will create the instance variable, a getter and setter
method for it. These getter and setter methods are the recommended way
to access instance variables both from ouside of the instances as well
as from within. To access the setter or getter method from within an
instance method use the self.<variable> notation.

The @<variable> notation is also supported, but special care needs to
be taken when modifying an instance variable. The setter methods will
automatically take care of persisting the modified instance when
required. If you use the @ notation for mutating instance variable
accesses, you must manually mark the instance as modified by calling
Object::mark_as_modified(). If that is forgotten, the change will
reside in memory but might not be persisted into the database.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'perobs'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install perobs

## Copyright and License

Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>

PEROBS and all accompanying files are licensed under this MIT License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Contributing

1. Fork it ( https://github.com/scrapper/perobs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
