# PEROBS - PErsistent Ruby OBject Store

PEROBS is a library that provides a persistent object store for Ruby
objects. Objects of your classes can be made persistent by deriving
them from PEROBS::Object. They will be in memory when needed and
transparently stored into a persistent storage.

This library is ideal for Ruby applications that work on huge, mostly
constant data sets and usually handle a small subset of the data at a
time. To ensure data consistency of a larger data set, you can use
transactions to make modifications of multiple objects atomicaly.
Transactions can be nested and are aborted when an exception is
raised.

## Usage

The objects that you want to persist must be of a class that has been
derived from PEROBS::BaseObject. PEROBS already provides 3 such
classes:

* PEROBS::Object is the base class for all your classes that should be
  persistent. You can determine which instance variables should be
  persisted and what default values should be used.

* PEROBS::Array provides an interface similar to the built-in Array class
  but its objects are automatically persisted.

* PEROBS::Hash provides an interface similar to the built-in Hash
  class but its objects are automatically persisted.

When you derive your own class from PEROBS::Object you need to
specify which instance variables should be persistent. By using
po_attr you can provide a list of symbols that describe the instance
variables to persist. This will also create getter and setter methods
for these instance varables.  You can set default values in the
constructor . The constructor of PEROBS::ObjectBase derived objects
must have at least one argument. The first argument is a PEROBS
internal object that must be passed to super() as first thing in
initialize(). You can have other arguments if needed. Be aware that
initialize() is not called when objects are restored from the
database! You can define a restore() method to deal with object
initialization or modification after restore from database. restore()
is also the proper place to initialize non-persistent instance
variables.  New objects are created via Store.new() so you cannot call
the constructor directly in your code.

To start off you must create at least one PEROBS::Store object that
owns your persistent objects. The store provides the persistent
database. A persistent object is tied to the creating store for its
whole lifetime. By default, PEROBS::Store uses an on-disk database in the
directory you specify. But you can use key/value databases as well.
Currently only Amazon DynamoDB is supported. You can create your own
key/value database wrapper with little effort.

When creating the store you can also specify the serializer to use.
The serializer controls how your data is converted to be stored in the
database.  The default serializer (JSON), you can only use the subset
of Ruby types that JSON supports. See http://www.json.org/ for
details. Alternatively, you can use Marshal or YAML which support
almost every Ruby data type. YAML is much slower than JSON and Marshal
is not guaranteed to be compatible between Ruby versions.

Once you have created a store you can assign objects to it. All
persistent objects must be created with Store.new(). This is
necessary as you will only deal with proxy objects in your code.
Except for the member methods, you will never deal with the objects
directly. Instead Store.new() returns a POXReference object that acts
as a transparent proxy. This proxy is needed as your code never knows
if the actual object is really loaded into the memory or not. PEROBS
will handle this transparently for you.

A build-in cache keeps access latencies to recently used objects low
and lazily flushes modified objects into the persistend back-end when
not using transactions.  It also features a garbage collector that
removes all objects that are no longer in use. 

So what does 'in use' mean? You can assign a few objects to the store
directly. The store acts like a hash. These root objects can then
reference other persistent objects and so on. The garbage collector
will find all objects that are reachable from the root objects and
discards all other from the database. You have to invoke the garbage
collector manually with Store.gc(). Depending on the size of your
database it can take some time. It is recommended that you don't use
persistend objects for temporary objects in your code. Every created
object will end up in the database end needs to be garbage collected.

Here is an example how to use PEROBS. Let's define a class that models
a person with their family relations.

```
require 'perobs'

class Person < PEROBS::Object

  po_attr :name, :mother, :father, :kids, :spouse, :status

  def initialize(p, name)
    super(p)
    attr_init(:name, name)
    attr_init(:kids, store.new(PEROBS::Array))
    attr_init(:status, :single)
  end

  def restore
    # Use block version of attr_init() to avoid creating unneded
    # objects. The block is only called when @father doesn't exist yet.
    attr_init(:father) do { store.new(Person, 'Dad') }
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
store.exit
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

### Use of proxy objects

Your code should never deal with the persistent objects directly. The
PEROBS API takes care that you will always get a proxy object. The
only exception to this rule is the code in the instance methods. By
design, this code operates on the real object. The only caveat here is
the use of self(). If you pass the result of self() to another object
you will leak a PEROBS::ObjectBase derived object into your data
structures.  PEROBS will watch for this and will throw an exception
when it detects such objects. Just remember to use myself() instead of
self() if you want to pass a reference to the current persistent
object to another object.

### Caveats and known issues

PEROBS is currently not thread-safe. You cannot simultaneously access
the database from multiple application. You must provide your own
locking mechanism to prevent this from happening.

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
