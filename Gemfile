# frozen_string_literal: true

source "https://rubygems.org"

gemspec

group :development, :test do
  # The Avro codec requires this lazily and names it when it is missing, the
  # way the transport does with bunny. Here so the specs can read the bytes the
  # Java and Go libraries wrote; not in the gemspec, because a service
  # publishing JSON should not be made to compile an Avro parser to do it.
  gem "avro", "~> 1.12"
  # Here and not in the gemspec on purpose. The gem declares no runtime
  # dependencies so that reading an AceMQ envelope does not drag a broker
  # client into a process that will never open a socket; the transport requires
  # bunny lazily and says which gem to install when it is missing. The tests do
  # open sockets, so they need it.
  gem "bunny", "~> 2.23"
  # The same bargain as avro: the Protobuf codec requires it lazily and says
  # which gem to install when it is absent.
  #
  # Held back on Ruby 3.1, along with multi_json below, because both have since
  # raised their floor to Ruby 3.2 while this gem still promises 3.1 and CI
  # still builds on it. The codecs themselves are pure Ruby and run on 3.1
  # perfectly well; it is only these two runtimes that do not, and a library
  # that cannot be tested on the version it claims to support is not supporting
  # it. Conditional rather than pinned outright, because multi_json 1.17 does
  # not work with the json gem Ruby 4 ships. Both lines go when 3.1 does.
  gem "google-protobuf", RUBY_VERSION < "3.2" ? "~> 4.29.0" : "~> 4.29"
  # Ruby 4.0 removed logger from the default gems and bunny 2.24 still requires
  # it without declaring it, so bunny will not even load here without this line.
  # It is bunny's omission rather than ours, and it belongs beside bunny for
  # whoever next wonders why a standard-library name is in a Gemfile.
  gem "logger", "~> 1.6"
  # avro's own dependency, named here for the reason given above
  # google-protobuf: 1.20 raised its floor to Ruby 3.2, and this gem still
  # promises 3.1. Only on 3.1, because 1.17 does not work with Ruby 4's json.
  gem "multi_json", "~> 1.17.0" if RUBY_VERSION < "3.2"
  # The database-backed stores are written against a connection seam rather
  # than against a driver, and neither of these is a runtime dependency: the
  # gem still declares none, and a process that never opens a database never
  # installs one. They are here because a store whose whole claim is
  # transactional cannot be tested against a stub of a transaction.
  gem "pg", "~> 1.5"
  # REXML ships with Ruby but has been a *bundled* gem rather than a default
  # one since Ruby 3.4, which means it is on the load path of a plain `ruby`
  # and not on the load path under Bundler unless a Gemfile names it. The XML
  # codec requires it lazily and says exactly that when it is missing; this
  # line is why the specs can reach it.
  gem "rexml", "~> 3.3"
  gem "rspec", "~> 3.13"
  gem "rubocop", "~> 1.66"
  gem "rubocop-rspec", "~> 3.0"
  gem "sqlite3", "~> 2.0"
  # The documentation site's API reference is generated from the comments in
  # lib/. Here rather than in the gemspec because nobody installing this gem
  # needs a documentation tool to use it.
  gem "yard", "~> 0.9"
end
