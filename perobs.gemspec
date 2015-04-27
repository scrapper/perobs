# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'perobs/version'

Gem::Specification.new do |spec|
  spec.name          = "perobs"
  spec.version       = PEROBS::VERSION
  spec.authors       = ["Chris Schlaeger"]
  spec.email         = ["chris@linux.com"]
  spec.summary       = %q{Persistent Ruby Object Store}
  spec.description   = %q{Library to provide a persistent object store}
  spec.homepage      = "https://github.com/scrapper/perobs"
  spec.license       = "MIT"
  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake", "~> 10.0"
end
