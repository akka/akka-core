/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.internal

import java.time.{ Duration => JDuration }
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually

import akka.NotUsed
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TestClock
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.internal.EventsBySliceFirehose.FirehoseKey
import akka.persistence.query.typed.internal.EventsBySliceFirehose.SlowConsumerException
import akka.persistence.query.typed.scaladsl.EventsBySliceFirehoseQuery
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.OnNext
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.AkkaSpec
import akka.testkit.WithLogCapturing

object EventsBySliceFirehoseSpec {
  // real PersistenceId is in akka-persistence-typed, and no dependency to that from here
  final case class PersistenceId(entityTypeHint: String, entityId: String) {
    def id: String = s"$entityTypeHint|$entityId"
  }

  private val config = ConfigFactory.parseString(s"""
  akka.loglevel = DEBUG
  akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]

  akka.persistence.query.events-by-slice-firehose {
    # test-query-plugin is not used by this test, but must be defined
    delegate-query-plugin-id = test-query-plugin

    broadcast-buffer-size = 64

    firehose-linger-timeout = 2s

    # don't deplicate in this test
    deduplication-capacity = 0

    # disable reaper in this test, will be triggered by code
    slow-consumer-reaper-interval = 1 day

    verbose-debug-logging = on
  }

  events-by-slice-firehose-with-deduplication = $${akka.persistence.query.events-by-slice-firehose}
  events-by-slice-firehose-with-deduplication {
    deduplication-capacity = 10000
  }
  """)
}

class EventsBySliceFirehoseSpec
    extends AkkaSpec(EventsBySliceFirehoseSpec.config)
    with WithLogCapturing
    with Eventually {
  import EventsBySliceFirehoseSpec._

  private val entityType = "EntityA"

  private val persistence = Persistence(system)

  private val clock = new TestClock

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      tags: Set[String] = Set.empty): EventEnvelope[Any] = {
    clock.tick(JDuration.ofMillis(1))
    val now = clock.instant()
    EventEnvelope(
      TimestampOffset(now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      persistence.sliceForPersistenceId(pid.id),
      filtered = false,
      source = "",
      tags = tags)
  }

  private val envelopes = Vector(
    createEnvelope(PersistenceId(entityType, "a"), 1, "a1"),
    createEnvelope(PersistenceId(entityType, "b"), 1, "b1"),
    createEnvelope(PersistenceId(entityType, "c"), 1, "c1"),
    createEnvelope(PersistenceId(entityType, "d"), 1, "d1"))

  private class Setup {
    def allEnvelopes: Vector[EventEnvelope[Any]] = envelopes

    def sliceRange = 0 to 1023

    def pluginId: String = EventsBySliceFirehoseQuery.Identifier

    class ConsumerSetup {
      private val catchupPublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
      val catchupSource: Source[EventEnvelope[Any], NotUsed] =
        TestSource[EventEnvelope[Any]]().mapMaterializedValue { probe =>
          catchupPublisherPromise.success(probe)
          NotUsed
        }

      lazy val outProbe =
        eventsBySliceFirehose
          .eventsBySlices[Any](pluginId, entityType, sliceRange.min, sliceRange.max, NoOffset)
          .runWith(TestSink())

      lazy val catchupPublisher = {
        outProbe // materialize
        catchupPublisherPromise.future.futureValue
      }
    }

    private val consumerCount = new AtomicInteger
    private val consumers = Vector.fill(100)(new ConsumerSetup)

    def catchupPublisher(consumerIndex: Int): TestPublisher.Probe[EventEnvelope[Any]] =
      consumers(consumerIndex).catchupPublisher
    def catchupPublisher: TestPublisher.Probe[EventEnvelope[Any]] = catchupPublisher(0)

    def outProbe(consumerIndex: Int): TestSubscriber.Probe[EventEnvelope[Any]] = consumers(consumerIndex).outProbe
    def outProbe: TestSubscriber.Probe[EventEnvelope[Any]] = outProbe(0)

    val firehoseRunning = new AtomicBoolean
    private val firehosePublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
    private val firehoseSource: Source[EventEnvelope[Any], NotUsed] =
      TestSource[EventEnvelope[Any]]().watchTermination()(Keep.both).mapMaterializedValue {
        case (probe, termination) =>
          firehoseRunning.set(true)
          termination.onComplete(_ => firehoseRunning.set(false))(ExecutionContext.parasitic)
          firehosePublisherPromise.success(probe)
          NotUsed
      }

    val eventsBySliceFirehose = new EventsBySliceFirehose(system.classicSystem) {
      override protected def underlyingEventsBySlices[Event](
          pluginId: String,
          entityType: String,
          minSlice: Int,
          maxSlice: Int,
          offset: Offset,
          firehose: Boolean): Source[EventEnvelope[Event], NotUsed] = {
        if (firehose)
          firehoseSource.map(_.asInstanceOf[EventEnvelope[Event]])
        else {
          val i = consumerCount.getAndIncrement()
          consumers(i).catchupSource.map(_.asInstanceOf[EventEnvelope[Event]])
        }
      }
    }

    lazy val firehosePublisher = {
      outProbe // materialize at least one
      firehosePublisherPromise.future.futureValue
    }
  }

  "EventsBySliceFirehose" must {
    "emit from catchup" in new Setup {
      allEnvelopes.foreach(catchupPublisher.sendNext)
      outProbe.request(10)
      outProbe.expectNextN(envelopes.size) shouldBe envelopes
    }

    "emit from catchup and then switch over to firehose" in new Setup {
      catchupPublisher.sendNext(allEnvelopes(0))
      catchupPublisher.sendNext(allEnvelopes(1))
      outProbe.request(10)
      outProbe.expectNext(allEnvelopes(0))
      outProbe.expectNext(allEnvelopes(1))

      firehosePublisher.sendNext(allEnvelopes(2))
      outProbe.expectNoMessage()

      catchupPublisher.sendNext(allEnvelopes(2))
      outProbe.expectNext(allEnvelopes(2)) // still from catchup

      firehosePublisher.sendNext(allEnvelopes(3))
      outProbe.expectNext(allEnvelopes(3)) // from firehose

      catchupPublisher.sendNext(allEnvelopes(3))
      outProbe.expectNext(allEnvelopes(3)) // from catchup, emitting from both

      clock.tick(JDuration.ofSeconds(60))
      val env5 = createEnvelope(PersistenceId(entityType, "a"), 2, "a2")
      catchupPublisher.sendNext(env5)
      outProbe.expectNext(env5)

      val env6 = createEnvelope(PersistenceId(entityType, "a"), 3, "a3")
      catchupPublisher.sendNext(env6)
      outProbe.expectNoMessage() // catchup closed
      firehosePublisher.sendNext(env6)
      outProbe.expectNext(env6)
    }

    "track consumer progress" in new Setup {
      // using two consumers
      outProbe(0).request(2)
      outProbe(1).request(2)

      // FIXME is there a way to know that it has been added to the hub?
      Thread.sleep(1000)

      catchupPublisher(0).sendNext(allEnvelopes(0))
      catchupPublisher(1).sendNext(allEnvelopes(0))
      outProbe(0).expectNext(allEnvelopes(0))
      outProbe(1).expectNext(allEnvelopes(0))

      firehosePublisher.sendNext(allEnvelopes(0))
      // this "sleep" is needed so that the firehose envelope is received first
      outProbe.expectNoMessage()

      catchupPublisher(0).sendNext(allEnvelopes(1))
      catchupPublisher(1).sendNext(allEnvelopes(1))
      outProbe(0).expectNext(allEnvelopes(1))
      outProbe(1).expectNext(allEnvelopes(1))

      val firehose =
        eventsBySliceFirehose.getFirehose(FirehoseKey(EventsBySliceFirehoseQuery.Identifier, entityType, sliceRange))
      firehose.consumerTracking.size shouldBe 2
      import scala.jdk.CollectionConverters._
      firehose.consumerTracking.values.asScala.foreach { tracking =>
        tracking.offsetTimestamp shouldBe allEnvelopes(1).offset.asInstanceOf[TimestampOffset].timestamp
      }

      // only requesting for outProbe(0)
      outProbe(0).request(100)
      // less than BroadcastHub buffer size
      val moreEnvelopes = (1 to 20).map(n => createEnvelope(PersistenceId(entityType, "x"), n, s"x$n"))
      moreEnvelopes.foreach(firehosePublisher.sendNext)
      outProbe(0).expectNextN(moreEnvelopes.size) shouldBe moreEnvelopes
      firehose.consumerTracking.size shouldBe 2
      firehose.consumerTracking.values.asScala
        .count(_.offsetTimestamp == moreEnvelopes.last.offset.asInstanceOf[TimestampOffset].timestamp) shouldBe 1
    }

    "abort slow consumers" in new Setup {
      // using two consumers
      outProbe(0).request(3)
      outProbe(1).request(3)

      // FIXME is there a way to know that it has been added to the hub?
      Thread.sleep(1000)

      catchupPublisher(0).sendNext(allEnvelopes(0))
      catchupPublisher(1).sendNext(allEnvelopes(0))
      outProbe(0).expectNext(allEnvelopes(0))
      outProbe(1).expectNext(allEnvelopes(0))

      firehosePublisher.sendNext(allEnvelopes(0))
      // this "sleep" is needed so that the firehose envelope is received first
      outProbe.expectNoMessage()

      catchupPublisher(0).sendNext(allEnvelopes(1))
      catchupPublisher(1).sendNext(allEnvelopes(1))
      outProbe(0).expectNext(allEnvelopes(1))
      outProbe(1).expectNext(allEnvelopes(1))

      // switch to firehose only
      clock.tick(JDuration.ofSeconds(60))
      // more than half BroadcastHub buffer size
      val moreEnvelopes = (1 to 40).map { n =>
        clock.tick(JDuration.ofSeconds(1))
        createEnvelope(PersistenceId(entityType, "x"), n, s"x$n")
      }
      // both consumers will now switch to firehose only
      catchupPublisher(0).sendNext(moreEnvelopes.head)
      catchupPublisher(1).sendNext(moreEnvelopes.head)
      outProbe(0).expectNext(moreEnvelopes.head)
      outProbe(1).expectNext(moreEnvelopes.head)
      // only requesting for outProbe(0)
      outProbe(0).request(100)
      moreEnvelopes.foreach(firehosePublisher.sendNext)
      outProbe(0).expectNextN(moreEnvelopes.size) shouldBe moreEnvelopes
      val firehose =
        eventsBySliceFirehose.getFirehose(FirehoseKey(EventsBySliceFirehoseQuery.Identifier, entityType, sliceRange))
      clock.tick(JDuration.ofSeconds(6)) // simulate consumer lag
      firehose.detectSlowConsumers(clock.instant())
      clock.tick(JDuration.ofSeconds(2))
      firehose.detectSlowConsumers(clock.instant())
      clock.tick(JDuration.ofSeconds(2))
      firehose.detectSlowConsumers(clock.instant())
      outProbe(1).expectError().getClass shouldBe classOf[SlowConsumerException]
    }

    "not abort consumers when fast" in new Setup {
      val numberOfConsumers = 10
      (0 until numberOfConsumers).foreach { i =>
        outProbe(i).request(10000)
      }

      // FIXME is there a way to know that it has been added to the hub?
      Thread.sleep(1000)

      val moreEnvelopes = (1 to 100).map { n =>
        createEnvelope(PersistenceId(entityType, "x"), n, s"x$n")
      }

      moreEnvelopes.foreach { env =>
        firehosePublisher.sendNext(env)
        (0 until numberOfConsumers).foreach { i =>
          catchupPublisher(i).sendNext(env)
        }
      }

      (0 until numberOfConsumers).foreach { i =>
        // there may be duplicates
        val received = outProbe(i).receiveWhile(max = 10.seconds, idle = 100.millis) { case OnNext(env) => env }
        received.toSet shouldBe moreEnvelopes.toSet
      }
    }

    "not close when catchup is closed" in new Setup {
      allEnvelopes.foreach(catchupPublisher.sendNext)
      outProbe.request(10)
      catchupPublisher.sendComplete()
      outProbe.expectNextN(envelopes.size) shouldBe envelopes
      outProbe.expectNoMessage() // not OnComplete
    }

    "close when firehose is closed" in new Setup {
      allEnvelopes.foreach(catchupPublisher.sendNext)
      outProbe.request(10)
      outProbe.expectNextN(envelopes.size) shouldBe envelopes
      firehosePublisher.sendComplete()
      outProbe.expectComplete()
    }

    "close when last consumer is removed, but more consumers can be added later" in new Setup {
      // using two consumers first
      outProbe(0).request(10)
      outProbe(1).request(10)

      // FIXME is there a way to know that it has been added to the hub?
      Thread.sleep(1000)

      catchupPublisher(0).sendNext(allEnvelopes(0))
      catchupPublisher(1).sendNext(allEnvelopes(0))
      outProbe(0).expectNext(allEnvelopes(0))
      outProbe(1).expectNext(allEnvelopes(0))
      firehoseRunning.get shouldBe true

      outProbe(0).cancel()
      catchupPublisher(1).sendNext(allEnvelopes(1))
      outProbe(1).expectNext(allEnvelopes(1))
      firehoseRunning.get shouldBe true

      outProbe(1).cancel()

      // another consumer
      outProbe(2).request(10)
      // FIXME is there a way to know that it has been added to the hub?
      Thread.sleep(1000)
      catchupPublisher(2).sendNext(allEnvelopes(2))
      outProbe(2).expectNext(allEnvelopes(2))
      firehoseRunning.get shouldBe true

      outProbe(2).cancel()
      // after a while the firehose will be shutdown
      eventually {
        firehoseRunning.get shouldBe false
      }

      // FIXME add another (more integration test) that verifies similar that consumers can be added to the
      // EventsBySliceFirehose extension after the firehose has been shutdown
    }

    "deduplicate when emitting from both" in new Setup {
      override def pluginId = "events-by-slice-firehose-with-deduplication"

      catchupPublisher.sendNext(allEnvelopes(0))
      catchupPublisher.sendNext(allEnvelopes(1))
      outProbe.request(10)
      outProbe.expectNext(allEnvelopes(0))
      outProbe.expectNext(allEnvelopes(1))

      firehosePublisher.sendNext(allEnvelopes(2))
      outProbe.expectNoMessage()

      catchupPublisher.sendNext(allEnvelopes(2))
      outProbe.expectNext(allEnvelopes(2)) // still from catchup

      firehosePublisher.sendNext(allEnvelopes(3))
      outProbe.expectNext(allEnvelopes(3)) // from firehose

      catchupPublisher.sendNext(allEnvelopes(3))
      outProbe.expectNoMessage() // duplicate from catchup

      clock.setInstant(allEnvelopes.last.offset.asInstanceOf[TimestampOffset].timestamp)
      val env4 = createEnvelope(PersistenceId(entityType, "a"), 2, "a2")
      catchupPublisher.sendNext(env4)
      firehosePublisher.sendNext(env4)
      outProbe.expectNext(env4)
      outProbe.expectNoMessage() // deduplicate

      clock.tick(JDuration.ofSeconds(60))
      val env5 = createEnvelope(PersistenceId(entityType, "b"), 2, "b2")
      catchupPublisher.sendNext(env5)
      outProbe.expectNext(env5)

      val env6 = createEnvelope(PersistenceId(entityType, "b"), 3, "b3")
      catchupPublisher.sendNext(env6)
      outProbe.expectNoMessage() // catchup closed
      firehosePublisher.sendNext(env6)
      outProbe.expectNext(env6)
    }
  }

}
