source 'https://rubygems.org'

platform :jruby do
  gem 'redstorm', '~> 0.6.4'
end

platform :mri do
  gem 'hiredis', '~> 0.4.5'
  gem 'redis', '~> 3.0.2'
  gem 'twitter-stream', '~> 0.1.16'
end

group :test do
  gem 'rake'
  gem 'rspec', '~> 2.11.0'
end

group :topology do
  gem 'json', platforms: :jruby
  gem 'redis', '~> 3.0.2', platforms: :jruby
  gem 'twitter-text', '~> 1.5.0', platform: :jruby
end
