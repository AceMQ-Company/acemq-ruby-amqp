# frozen_string_literal: true

# The AceMQ TCK driver for this library.
#
# Started by the TCK runner (github.com/AceMQ-Company/acemq-amqp-tck), which
# writes one JSON request per line to stdin and reads one JSON answer per line
# from stdout. Every operation goes through the public API; nothing here
# asserts, because the runner checks the outcome on the broker. The protocol is
# docs/driver-contract.md in the TCK repository.
#
#     bundle exec ruby -Ilib spec/tck/driver.rb
#
# Not a spec: its name does not end in _spec.rb, so rspec never loads it.

require "json"
require "socket"
require "acemq/amqp"
require "acemq/amqp/patterns"

module AceMQ
  module AMQP
    module TCK
      OPS = %w[hello begin end publish consume handled schedule request].freeze

      class UnsupportedOp < StandardError; end

      # Maps each TCK operation onto the public API.
      class Driver
        def initialize(url)
          @url = url
          @connection = nil
          @consumers = {}
          @counts = {}
          @redelivered = {}
          @scheduler = nil
          @requesters = {}
          @lock = Mutex.new
        end

        def hello(_args)
          { library: "acemq-ruby-amqp", version: VERSION, ops: OPS }
        end

        def begin(args)
          self.end({})
          origin = "#{args.fetch("clientName", "tck")}@#{Socket.gethostname}"
          @connection = Connection.open(@url, origin: origin)
          {}
        end

        def end(_args)
          @requesters.each_value(&:close)
          @requesters.clear
          @scheduler&.close
          @scheduler = nil
          @consumers.each_value(&:cancel)
          @consumers.clear
          @counts.clear
          @redelivered.clear
          @connection&.close
          @connection = nil
          {}
        end

        def publish(args)
          envelope = mq.publish(args["body"], to: args["routingKey"],
                                              exchange: args.fetch("exchange", ""),
                                              **envelope_fields(args["envelope"] || {}))
          { messageId: envelope.id }
        end

        def consume(args)
          queue = args["queue"]
          retry_args = args["retry"] || {}
          policy = RetryPolicy.fixed(retry_args.fetch("maxAttempts", 3),
                                     retry_args.fetch("delayMs", 1000) / 1000.0)
          handle = args["handle"]
          @counts[handle] = Hash.new(0)
          @redelivered[handle] = Hash.new(0)
          handler = handler_for(handle, args["behaviour"] || {})

          mq.apply(Topology.new.queue(queue, dead_letter: true, retry_policy: policy))
          @consumers[handle] = mq.consume(queue, retry_policy: policy,
                                                 prefetch: args.fetch("prefetch", 10), &handler)
          {}
        end

        def handled(args)
          handle = args["handle"]
          @lock.synchronize do
            { counts: @counts.fetch(handle).dup, redelivered: @redelivered.fetch(handle).dup }
          end
        end

        def schedule(args)
          @scheduler ||= Patterns::Scheduler.on(mq)
          @scheduler.in(args["delayMs"] / 1000.0, args["body"],
                        to: args["routingKey"], exchange: args.fetch("exchange", ""))
          {}
        end

        def request(args)
          key = [args.fetch("exchange", ""), args["routingKey"]]
          requester = @requesters[key] ||= Patterns::Requester.new(
            mq, to: key[1], exchange: key[0], timeout: args["timeoutMs"] / 1000.0
          )
          { reply: requester.call(args["body"]) }
        end

        private

        def mq
          @connection or raise "begin was not called"
        end

        # Counts the delivery, sleeps, and fails the first n deliveries of an id
        # named in failFirst with an ordinary exception, which the consumer
        # turns into a retry.
        def handler_for(handle, behaviour)
          counts = @counts[handle]
          redelivered = @redelivered[handle]
          fail_first = behaviour["failFirst"] || {}
          sleep_for = behaviour.fetch("sleepMs", 0) / 1000.0
          lambda do |message|
            ident = message.payload["id"].to_s
            seen = @lock.synchronize do
              redelivered[ident] += 1 if message.redelivered?
              counts[ident] += 1
            end
            sleep(sleep_for) if sleep_for.positive?
            if seen <= fail_first.fetch(ident, 0)
              raise "tck: failing #{ident} on purpose, delivery #{seen}"
            end

            Ack.accept
          end
        end

        def envelope_fields(given)
          {
            id: given["id"], type: given["type"], version: given["version"],
            correlation_id: given["correlationId"], causation_id: given["causationId"],
            headers: given["headers"]
          }.compact
        end
      end

      def self.kind_of(failure)
        case failure
        when UnsupportedOp then "unsupported"
        when Patterns::RequestTimedOut then "timeout"
        when Patterns::ResponderFailed then "responder"
        when PublishError then failure.unroutable? ? "unroutable" : "refused"
        when FatalError then "decode"
        else "other"
        end
      end

      def self.answer(driver, request)
        op = request["op"].to_s
        raise UnsupportedOp, op unless OPS.include?(op)

        { id: request["id"], ok: true, result: driver.public_send(op, request) }
      rescue StandardError => e
        { id: request["id"], ok: false,
          error: { kind: kind_of(e), type: e.class.name, message: e.message } }
      end

      def self.main
        url = ENV.fetch("ACEMQ_TCK_AMQP_URL", "amqp://guest:guest@localhost:5672/")
        driver = Driver.new(url)
        # The answers get the real stdout to themselves; anything else written
        # to fd 1, bunny's default logger included, goes to stderr.
        out = $stdout.dup
        $stdout.reopen($stderr)
        out.sync = true
        $stdin.each_line do |line|
          out.puts(JSON.generate(answer(driver, JSON.parse(line))))
        end
      ensure
        driver&.end({})
      end
    end
  end
end

AceMQ::AMQP::TCK.main if $PROGRAM_NAME == __FILE__
