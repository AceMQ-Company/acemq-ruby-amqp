# frozen_string_literal: true

# Copyright 2026 AceMQ.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "securerandom"
require "time"

require_relative "queue_type"

module AceMQ
  module AMQP
    # Whether this process can actually do what it is running to do.
    #
    # The question a readiness probe is really asking is not "is the socket
    # open" but "would a message published right now reach the broker, and is
    # anything still reading the queues this service is responsible for". An
    # open TCP connection answers neither: a broker that has been paused, a
    # network that is black-holing, and a healthy one all look the same from
    # this end until something is asked of them.
    #
    #   report = mq.health
    #   report.up?        # => true
    #   report.to_h       # => what to render as JSON on /acemq-health
    #
    # Every AceMQ library serves this at +/acemq-health+ and returns 503 when
    # the status is +:down+, so a probe written for one service works against
    # another. This library stops at the report: it has no web framework and
    # should not choose one.
    module Health
      # Working.
      UP = :up

      # Not working. A readiness probe should fail on this.
      DOWN = :down

      # Working, but not as well as it should. Worth an alert; not worth taking
      # the instance out of rotation on its own.
      DEGRADED = :degraded

      # What a check found.
      #
      # +parts+ is whatever the check can say about itself — how many consumers
      # there are, how long the broker took to answer, what a nested check
      # reported — and it is the half somebody reads when the status alone does
      # not tell them what to do.
      Report = Struct.new(:status, :detail, :checked_at, :parts, keyword_init: true) do
        def up? = status == UP
        def down? = status == DOWN
        def degraded? = status == DEGRADED

        # The shape every AceMQ library renders at +/acemq-health+.
        def to_h
          rendered = { "status" => status.to_s, "checked" => checked_at.utc.iso8601 }
          rendered["detail"] = detail unless detail.nil? || detail.empty?
          rendered["parts"] = parts.transform_values { |v| v.is_a?(Report) ? v.to_h : v }
          rendered
        end

        def to_s = detail.to_s.empty? ? status.to_s : "#{status}: #{detail}"
      end

      # Checks a connection, and the consumers running on it.
      #
      # The broker is checked by declaring a queue and deleting it again rather
      # than by asking whether the socket is open, because a declaration is the
      # cheapest thing AMQP offers that actually proves the round trip. The
      # queue is named for this moment, so two instances running the check
      # cannot collide, and it is removed straight afterwards, so a probe on a
      # five-second interval leaves nothing behind it.
      #
      # It costs a round trip, so this is not something to call per request.
      # Wire it to a readiness probe and let the probe's interval decide.
      #
      # A consumer that has stopped while the connection is still up is
      # +:degraded+ rather than +:down+. The process can still publish and its
      # other consumers still work, so failing the probe would take out
      # something that is doing most of its job; but a queue with nothing
      # reading it is a real fault and has to be visible, which is what
      # +:degraded+ is for.
      #
      # @param connection [Connection]
      # @return [Report]
      def self.of(connection)
        checked_at = Time.now
        consumers = consumer_parts(connection)
        return closed(checked_at, consumers) unless open?(connection)

        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        probe(connection)
        consumers["round_trip_ms"] = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) -
                                       started) * 1000).round
        consumer_verdict(checked_at, consumers)
      rescue StandardError => e
        Report.new(status: DOWN, detail: "the broker did not answer: #{e.message}",
                   checked_at: checked_at, parts: consumers)
      end

      # Runs several checks and combines them.
      #
      # The combined status is the worst of them: a service that cannot reach
      # its broker is not ready however healthy the rest of it is.
      #
      # A check is anything answering +name+ and +check+. They are run on
      # threads rather than in turn, so a slow one does not add its latency to
      # the others, and one that raises becomes a +:down+ part rather than an
      # exception out of a readiness probe — a probe that raises tells the
      # orchestrator nothing at all.
      #
      # @param checks [Array<#name, #check>]
      # @return [Report]
      def self.aggregate(*checks)
        checked_at = Time.now
        parts = checks.flatten.map { |check| Thread.new { [check.name.to_s, run(check)] } }
                      .to_h(&:value)
        worst = parts.values.map(&:status)
        status = if worst.include?(DOWN) then DOWN
                 elsif worst.include?(DEGRADED) then DEGRADED
                 else UP
                 end
        troubled = parts.reject { |_, part| part.up? }.keys
        Report.new(status: status, detail: troubled.join(", "), checked_at: checked_at,
                   parts: parts)
      end

      # Adapts a connection to the +name+/+check+ pair {aggregate} wants.
      Check = Struct.new(:name, :connection) do
        def check = Health.of(connection)
      end

      # @api private
      def self.run(check)
        check.check
      rescue StandardError => e
        Report.new(status: DOWN, detail: "the check itself failed: #{e.message}",
                   checked_at: Time.now, parts: {})
      end

      # @api private
      def self.probe(connection)
        name = "acemq-health-#{SecureRandom.uuid}"
        # Classic, which is the only thing a queue with these flags can be:
        # RabbitMQ will not replicate a queue that disappears with the
        # connection that declared it. Said out loud so that a probe is never
        # quietly caught by the quorum default the rest of the library has.
        connection.declare_queue(name, queue_type: QueueType::CLASSIC, durable: false,
                                       auto_delete: true, exclusive: true)
        # Deleted rather than left to clean itself up, which it will not.
        # Auto-delete fires when the last consumer goes and a probe queue never
        # has one; exclusive fires when the connection goes and a connection
        # lives as long as the process. Both flags are set anyway, because they
        # are what limits the damage if this process dies between the two calls
        # — but the delete is what actually keeps a probe running every five
        # seconds from leaving a queue every five seconds.
        connection.delete_queue(name)
      end

      # @api private
      def self.open?(connection)
        transport = connection.respond_to?(:transport) ? connection.transport : connection
        return transport.open? if transport.respond_to?(:open?)

        # A transport that will not say is taken at its word rather than
        # guessed at; the probe below is what actually decides.
        true
      end

      # @api private
      def self.consumer_parts(connection)
        consumers = connection.respond_to?(:consumers) ? connection.consumers : []
        running = consumers.count(&:running?)
        { "consumers" => consumers.size, "consumers_running" => running,
          "queues" => consumers.map(&:queue).uniq }
      end

      def self.closed(checked_at, parts)
        Report.new(status: DOWN, detail: "the connection has been closed",
                   checked_at: checked_at, parts: parts)
      end

      def self.consumer_verdict(checked_at, parts)
        stopped = parts["consumers"] - parts["consumers_running"]
        return Report.new(status: UP, checked_at: checked_at, parts: parts) if stopped.zero?

        Report.new(status: DEGRADED, checked_at: checked_at, parts: parts,
                   detail: "#{stopped} of #{parts["consumers"]} consumers " \
                           "#{stopped == 1 ? "has" : "have"} stopped")
      end

      private_class_method :closed, :consumer_verdict
    end
  end
end
