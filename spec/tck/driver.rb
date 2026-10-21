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
      OPS = %w[hello begin end publish consume handled schedule request
               declareStream readStream metrics replay pipeline].freeze

      class UnsupportedOp < StandardError; end

      # The telemetry seam, recording every metric name with its labels.
      class Recorder
        def initialize
          @samples = []
          @lock = Mutex.new
        end

        def count(metric, _delta = 1, **labels) = record(metric, labels)
        def observe(metric, _value, **labels) = record(metric, labels)
        def gauge(metric, _value, **labels) = record(metric, labels)

        def samples = @lock.synchronize { @samples.dup }

        private

        def record(metric, labels)
          sample = { name: metric, tags: labels.to_h { |k, v| [k.to_s, v.to_s] } }
          @lock.synchronize { @samples << sample }
        end
      end

      # Maps each TCK operation onto the public API.
      class Driver
        def initialize(url)
          @url = url
          @connection = nil
          @consumers = {}
          @counts = {}
          @redelivered = {}
          @order = {}
          @recorder = Recorder.new
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
          # Publisher confirms cannot be turned off in this library, so a
          # case's confirms: false changes nothing here.
          @recorder = Recorder.new
          @connection = Connection.open(@url, origin: origin, telemetry: @recorder)
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
          @order.clear
          @connection&.close
          @connection = nil
          {}
        end

        # mandatory absent is the library's default (mandatory); true and false
        # are passed as given.
        def publish(args)
          options = args.key?("mandatory") ? { mandatory: args["mandatory"] } : {}
          fields = envelope_fields(args["envelope"] || {})
          envelope = mq.publish(args["body"], to: args["routingKey"],
                                              exchange: args.fetch("exchange", ""),
                                              **options, **fields)
          { messageId: envelope.id }
        end

        # retry absent is no retry policy at all: the consumer as the library
        # builds it when the caller configures none.
        def consume(args)
          queue = args["queue"]
          retry_args = args["retry"]
          policy = retry_args && RetryPolicy.fixed(retry_args.fetch("maxAttempts", 3),
                                                   retry_args.fetch("delayMs", 1000) / 1000.0)
          handle = args["handle"]
          handler = handler_for(handle, args["behaviour"] || {})
          if args["idempotent"]
            handler = Patterns.idempotent(Patterns::InMemoryIdempotencyStore.new, &handler)
          end

          mq.apply(Topology.new.queue(queue, dead_letter: true, retry_policy: policy))
          @consumers[handle] = mq.consume(queue, retry_policy: policy,
                                                 prefetch: args.fetch("prefetch", 10), &handler)
          {}
        end

        # The pipeline's own topology, and on each step's queue a consumer that
        # follows the slip with the identity: count, and pass the body on. A
        # step named in behaviour raises inside the follow block, and no retry
        # policy is set anywhere.
        def pipeline(args)
          pipeline = Patterns::Pipeline.new(args["name"], args["steps"])
          mq.apply(pipeline.topology)
          behaviour = args["behaviour"] || {}
          pipeline.steps.each do |step|
            counted = handler_for(args["handle"], behaviour.fetch(step, {}))
            follow = pipeline.follow(mq) do |message|
              counted.call(message)
              message.payload
            end
            @consumers["#{args["handle"]}/#{step}"] =
              mq.consume(pipeline.queue_for(step), &follow)
          end
          {}
        end

        def handled(args)
          handle = args["handle"]
          @lock.synchronize do
            { counts: @counts.fetch(handle).dup, redelivered: @redelivered.fetch(handle).dup,
              order: @order.fetch(handle).dup }
          end
        end

        def declare_stream(args)
          Patterns.declare_stream(mq, args["queue"], max_age: args["maxAgeMs"] / 1000.0)
          {}
        end

        def read_stream(args)
          handle = args["handle"]
          offset = args["offset"] == "first" ? Patterns::StreamOffset.first : Patterns::StreamOffset.next
          handler = handler_for(handle, args["behaviour"] || {})
          @consumers[handle] = Patterns.read_stream(mq, args["queue"], offset: offset, &handler)
          {}
        end

        def metrics(_args)
          samples = @recorder.samples
          { names: samples.map { |s| s[:name] }.uniq.sort, samples: samples }
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

        # One filtered replay and nothing after it: the case reads the source
        # queue the moment this answers.
        def replay(args)
          ids = args["ids"]
          result = Patterns.replay(mq, from: args["from"], exchange: "",
                                       routing_key: args["queue"]) do |_envelope, body|
            ids.include?(JSON.parse(body)["id"])
          end
          { replayed: result.moved }
        end

        private

        def mq
          @connection or raise "begin was not called"
        end

        # Counts the delivery, sleeps, and fails the first n deliveries of an id
        # named in failFirst with an ordinary exception, which the consumer
        # turns into a retry.
        #
        # The counts are per handle, shared by every handler under it (a
        # pipeline's steps); failFirst counts this handler's own deliveries.
        def handler_for(handle, behaviour)
          unless @counts.key?(handle)
            @counts[handle] = Hash.new(0)
            @redelivered[handle] = Hash.new(0)
            @order[handle] = []
          end
          counts = @counts[handle]
          redelivered = @redelivered[handle]
          order = @order[handle]
          mine = Hash.new(0)
          fail_first = behaviour["failFirst"] || {}
          sleep_for = behaviour.fetch("sleepMs", 0) / 1000.0
          lambda do |message|
            ident = message.payload["id"].to_s
            seen = @lock.synchronize do
              redelivered[ident] += 1 if message.redelivered?
              order << ident
              counts[ident] += 1
              mine[ident] += 1
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

      # Most specific first: PublishingPausedError is a PublishError.
      KINDS = {
        UnsupportedOp => "unsupported", Patterns::RequestTimedOut => "timeout",
        Patterns::ResponderFailed => "responder", PublishingPausedError => "paused",
        PublishError => "nacked", FatalError => "decode"
      }.freeze

      def self.kind_of(failure)
        return "unroutable" if failure.is_a?(PublishError) && failure.unroutable?

        KINDS.find { |type, _| failure.is_a?(type) }&.last || "other"
      end

      def self.answer(driver, request)
        op = request["op"].to_s
        raise UnsupportedOp, op unless OPS.include?(op)

        method = op.gsub(/[A-Z]/) { "_#{_1.downcase}" }
        { id: request["id"], ok: true, result: driver.public_send(method, request) }
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
