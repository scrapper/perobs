require 'rake'
require 'rspec/core/rake_task'

desc 'Run all RSpec tests in the spec directory'
RSpec::Core::RakeTask.new(:test) do |t|
  t.pattern = Dir.glob('test/*_spec.rb')
  t.rspec_opts = "-I #{File.join(File.dirname(__FILE__), '..', 'test')}"
end
