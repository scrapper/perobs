# Add the lib directory to the search path if it isn't included already
# lib = File.expand_path('../lib', __FILE__)
# $:.unshift lib unless $:.include?(lib)

require "bundler/gem_tasks"
require "rspec/core/rake_task"
require 'rake/clean'
require 'yard'
YARD::Rake::YardocTask.new

Dir.glob( 'tasks/*.rake').each do |fn|
  begin 
    load fn;
  rescue LoadError
    puts "#{fn.split('/')[1]} tasks unavailable: #{$!}"
  end
end

RSpec::Core::RakeTask.new(:spec) do |task|
  task.pattern = Dir.glob('test/*_spec.rb')
  task.rspec_opts = "-I #{File.join(File.dirname(__FILE__), "test")}"
end

task :default  => :spec
task :test => :spec
desc 'Run all unit and spec tests'
