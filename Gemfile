# frozen_string_literal: true

source "https://rubygems.org"

gemspec

group :development, :test do
  # Here and not in the gemspec on purpose. The gem declares no runtime
  # dependencies so that reading an AceMQ envelope does not drag a broker
  # client into a process that will never open a socket; the transport requires
  # bunny lazily and says which gem to install when it is missing. The tests do
  # open sockets, so they need it.
  gem "bunny", "~> 2.23"
  # Ruby 4.0 removed logger from the default gems and bunny 2.24 still requires
  # it without declaring it, so bunny will not even load here without this line.
  # It is bunny's omission rather than ours, and it belongs beside bunny for
  # whoever next wonders why a standard-library name is in a Gemfile.
  gem "logger", "~> 1.6"
  gem "rspec", "~> 3.13"
  gem "rubocop", "~> 1.66"
  gem "rubocop-rspec", "~> 3.0"
end
