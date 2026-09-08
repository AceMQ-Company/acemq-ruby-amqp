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
  # The database-backed stores are written against a connection seam rather
  # than against a driver, and neither of these is a runtime dependency: the
  # gem still declares none, and a process that never opens a database never
  # installs one. They are here because a store whose whole claim is
  # transactional cannot be tested against a stub of a transaction.
  gem "pg", "~> 1.5"
  gem "rspec", "~> 3.13"
  gem "rubocop", "~> 1.66"
  gem "rubocop-rspec", "~> 3.0"
  gem "sqlite3", "~> 2.0"
  # The documentation site's API reference is generated from the comments in
  # lib/. Here rather than in the gemspec because nobody installing this gem
  # needs a documentation tool to use it.
  gem "yard", "~> 0.9"
end
