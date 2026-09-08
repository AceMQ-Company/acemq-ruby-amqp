# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

Dir[File.expand_path("support/*.rb", __dir__)].each { |file| require file }

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  # A broker is not needed by anything under spec/ except the integration
  # specs, which say so, so a laptop with no Docker runs everything else.
  config.filter_run_excluding(:integration) unless ENV["ACEMQ_TEST_BROKER"]
  # The database-backed stores are tested against SQLite by the ordinary specs,
  # which need nothing installed. The same examples run against PostgreSQL when
  # there is one to point at, because "written for PostgreSQL" and "run against
  # PostgreSQL" are different claims and only the second one is worth making.
  config.filter_run_excluding(:postgres) unless ENV["ACEMQ_TEST_POSTGRES"]
  config.disable_monkey_patching!
end
