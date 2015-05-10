# PEROBS - PErsistent Ruby OBject Store

PEROBS is a library that provides a persistent object store for Ruby
objects. Objects of your classes can be made persistent by deriving
them from PEROBS::Object. They will be in memory when needed and
transparently stored into a persistent storage. Currently only
filesystem based storage is supported, but back-ends for key/value
databases can be easily added.

This library is ideal for Ruby applications that work on huge, mostly
constant data sets and usually handle a small subset of the data at a
time.

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
persistent database.

Here is an example how to use PEROBS. Let's define a class that models
a person with their family relations.

```
require 'perobs'

class Person < PEROBS::Object

  po_attr :name
  po_attr :mother
  po_attr :father
  po_attr :kids

  def initialize(store, name)
    super
    @name = name
  end

end
```

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'perobs'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install perobs

## Usage

TODO: Write usage instructions here

## Contributing

1. Fork it ( https://github.com/[my-github-username]/perobs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
