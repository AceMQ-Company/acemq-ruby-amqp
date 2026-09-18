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

      # What a report says when the broker has blocked the connection.
      #
      # Fixed wording, because it is what an alert rule will match on. The
      # broker's own reason follows it after a colon.
      BLOCKED = "the broker has blocked this connection; publishing is paused"

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
      # **A blocked connection is reported up, with the reason.** RabbitMQ
      # blocks a connection when it is low on memory or disk, and every publish
      # on it stops — so the temptation is to fail the probe, and failing it is
      # exactly wrong. A blocked connection is the broker protecting itself from
      # a producer that is doing nothing wrong. An orchestrator told this
      # instance is unready restarts it into the same blocked broker, having
      # thrown away whatever it was holding, and doing that to every replica at
      # once turns a broker under memory pressure into an outage with a crash
      # loop on top. The state still has to be *visible*, so it is a detail on
      # an +:up+ report: a dashboard shows it, an alert can match it, and
      # nothing is taken out of rotation for it. Java's +AceMqHealthIndicator+
      # and Go's actuator make the same call, in the same words.
      #
      # Blocking never changes the status downwards: a report that was
      # +:degraded+ because a consumer stopped stays +:degraded+ and says both
      # things, and a connection that is shut is +:down+ for a better reason.
      #
      # **The round trip is skipped while the connection is blocked**, and that
      # is not an optimisation. A blocked connection is one the broker has
      # stopped reading, so the declare this check is built on does not fail —
      # it hangs, until bunny's continuation timeout gives up seconds later and
      # reports +:down+ for a broker that is up and talking. Every careful word
      # above would then be overruled by the probe: an operator would see
      # +:down+, an orchestrator would restart into the pressured broker, and
      # the reason would never be read. The broker told this process it was
      # blocked over this same socket, which is a livelier proof than a declare
      # — so when it is blocked, that is the answer, and +round_trip_ms+ is
      # absent from the parts because nothing was timed. The block arriving
      # *during* a probe is caught the same way on the failure path.
      #
      # @param connection [Connection]
      # @return [Report]
      def self.of(connection)
        checked_at = Time.now
        consumers = consumer_parts(connection)
        return closed(checked_at, consumers) unless open?(connection)

        blocked = blocked_reason(connection)
        return also_blocked(consumer_verdict(checked_at, consumers), blocked) if blocked

        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        probe(connection)
        consumers["round_trip_ms"] = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) -
                                       started) * 1000).round
        consumer_verdict(checked_at, consumers)
      rescue StandardError => e
        # Asked again rather than trusted from before the probe: a connection
        # blocked while the declare was in flight is the ordinary way a probe
        # meets this, and the block is the explanation for the silence rather
        # than a second fault beside it.
        late = blocked_reason(connection)
        return also_blocked(consumer_verdict(checked_at, consumers), late) if late

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
      # The detail names every part with something to say, which is not the same
      # as every part that is not up. A blocked connection is reported +:up+
      # with the reason on it, and an aggregate that summarised by status alone
      # would answer +up+ with an empty detail — throwing away, at exactly the
      # level an operator reads first, the one fact the check went to the
      # trouble of finding. The part is still there in +parts+ either way; this
      # is about what the top line says.
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
        noteworthy = parts.reject { |_, part| silent?(part) }.keys
        Report.new(status: status, detail: noteworthy.join(", "), checked_at: checked_at,
                   parts: parts)
      end

      # Whether a part has nothing the line above it needs to mention: up, and
      # with no reason written on it.
      #
      # @api private
      def self.silent?(part) = part.up? && part.detail.to_s.empty?

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

      # Why the broker has blocked this connection, or nil.
      #
      # Asked of the transport seam rather than of a driver, the same way
      # {open?} is. The seam is what a test double satisfies — a fake that
      # answers +blocked_reason+ with a string is a blocked broker as far as
      # this is concerned, and one that has never heard of the method is simply
      # not asked. A downstream package reaching through the connection to
      # +transport.session.blocked?+ to get this was reaching past the seam into
      # bunny, which is why it is here.
      #
      # Deliberately tolerant: a health check that raises inside a readiness
      # probe tells an orchestrator nothing at all.
      #
      # @api private
      def self.blocked_reason(connection)
        transport = connection.respond_to?(:transport) ? connection.transport : connection
        reason = transport.respond_to?(:blocked_reason) ? transport.blocked_reason.to_s : ""
        reason.empty? ? nil : reason
      rescue StandardError
        nil
      end

      # The report with the block written onto it, or the report unchanged.
      #
      # The status is not touched; see {of} for why a blocked broker is up.
      #
      # @api private
      def self.also_blocked(report, reason)
        return report unless reason

        said = [report.detail, "#{BLOCKED}: #{reason}"].compact.reject(&:empty?).join("; ")
        Report.new(status: report.status, detail: said, checked_at: report.checked_at,
                   parts: report.parts.merge("blocked" => true, "blocked_reason" => reason))
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
