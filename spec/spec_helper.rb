# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  # A broker is not needed by anything under spec/ except the integration
  # specs, which say so, so a laptop with no Docker runs everything else.
  config.filter_run_excluding(:integration) unless ENV["ACEMQ_TEST_BROKER"]
  config.disable_monkey_patching!
end
