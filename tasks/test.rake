require 'rake'
require 'rspec/core/rake_task'

desc 'Run all RSpec tests in the spec directory'
RSpec::Core::RakeTask.new(:test) do |t|
  t.pattern = 'test/*_spec.rb'
end
