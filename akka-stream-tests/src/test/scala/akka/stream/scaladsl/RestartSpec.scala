/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.NotUsed
import akka.event.Logging
import akka.stream.Attributes
import akka.stream.Attributes.Name
import akka.stream.RestartSettings
import akka.stream.scaladsl.AttributesSpec.AttributesFlow
import akka.stream.scaladsl.AttributesSpec.AttributesSink
import akka.stream.scaladsl.AttributesSpec.AttributesSource
import akka.stream.scaladsl.AttributesSpec.WhateverAttribute
import akka.stream.scaladsl.AttributesSpec.whateverAttribute
import akka.stream.scaladsl.RestartWithBackoffFlow.Delay
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.DefaultTimeout
import akka.testkit.EventFilter
import akka.testkit.TestDuration
import akka.testkit.TestProbe
import akka.testkit.TimingTest

class RestartSpec
    extends StreamSpec(Map("akka.test.single-expect-default" -> "10s", "akka.loglevel" -> "INFO"))
    with DefaultTimeout {

  import system.dispatcher

  private val shortMinBackoff = 200.millis
  private val shortMaxBackoff = 300.millis
  private val minBackoff = 1.second.dilated
  private val maxBackoff = 3.seconds.dilated

  private val logSettings = RestartSettings.LogSettings(Logging.InfoLevel).withCriticalLogLevel(Logging.WarningLevel, 2)
  private val shortRestartSettings =
    RestartSettings(shortMinBackoff, shortMaxBackoff, 0).withLogSettings(logSettings)
  private val restartSettings =
    RestartSettings(minBackoff, maxBackoff, 0).withLogSettings(logSettings)

  "A restart with backoff source" should {
    "run normally" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Source.repeat("a")
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("a")
      probe.requestNext("a")
      probe.requestNext("a")
      probe.requestNext("a")

      created.get() should ===(1)

      probe.cancel()
    }

    "restart on completion" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Source(List("a", "b"))
        }
        .runWith(TestSink())

      EventFilter.info(start = "Restarting stream due to completion", occurrences = 2).intercept {
        probe.requestNext("a")
        probe.requestNext("b")
        probe.requestNext("a")
        probe.requestNext("b")
        probe.requestNext("a")
      }

      created.get() should ===(3)

      probe.cancel()
    }

    "restart on failure" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Source(List("a", "b", "c")).map {
            case "c"   => throw TE("failed")
            case other => other
          }
        }
        .runWith(TestSink())

      EventFilter.info(start = "Restarting stream due to failure", occurrences = 2).intercept {
        probe.requestNext("a")
        probe.requestNext("b")
        probe.requestNext("a")
        probe.requestNext("b")
        probe.requestNext("a")
      }

      // after 2, use critical level
      EventFilter.warning(start = "Restarting stream due to failure [3]", occurrences = 1).intercept {
        probe.requestNext("b")
        probe.requestNext("a")
      }

      created.get() should ===(4)

      probe.cancel()
    }

    "backoff before restart" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(restartSettings) { () =>
          created.incrementAndGet()
          Source(List("a", "b"))
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("b")

      // There should be a delay of at least minBackoff before we receive the element after restart
      val deadline = (minBackoff - 1.millis).fromNow
      probe.request(1)

      probe.expectNext("a")
      deadline.isOverdue() should be(true)

      created.get() should ===(2)

      probe.cancel()
    }

    "reset exponential backoff back to minimum when source runs for at least minimum backoff without completing" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(restartSettings) { () =>
          created.incrementAndGet()
          Source(List("a", "b"))
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("b")
      // There should be minBackoff delay
      probe.requestNext("a")
      probe.requestNext("b")
      probe.request(1)
      // The probe should now be backing off again with with increased backoff

      // Now wait for the delay to pass, then it will start the new source, we also want to wait for the
      // subsequent backoff to pass, so it resets the restart count
      Thread.sleep((minBackoff + (minBackoff * 2) + minBackoff + 500.millis).toMillis)

      probe.expectNext("a")
      probe.requestNext("b")

      // We should have reset, so the restart delay should be back, ie we should receive the
      // next element within < 2 * minBackoff
      probe.requestNext(2 * minBackoff - 10.milliseconds) should ===("a")

      created.get() should ===(4)

      probe.cancel()
    }

    "cancel the currently running source when cancelled" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val promise = Promise[Done]()
      val probe = RestartSource
        .withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Source.repeat("a").watchTermination() { (_, term) =>
            promise.completeWith(term)
          }
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.cancel()

      promise.future.futureValue should ===(Done)

      // Wait to ensure it isn't restarted
      Thread.sleep(200)
      created.get() should ===(1)
    }

    "not restart the source when cancelled while backing off" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(restartSettings) { () =>
          created.incrementAndGet()
          Source.single("a")
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.request(1)
      // Should be backing off now
      probe.cancel()

      // Wait to ensure it isn't restarted
      Thread.sleep((minBackoff + 100.millis).toMillis)
      created.get() should ===(1)
    }

    "stop on completion if it should only be restarted in failures" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .onFailuresWithBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Source(List("a", "b", "c")).map {
            case "c"   => if (created.get() == 1) throw TE("failed") else "c"
            case other => other
          }
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("b")
      // will fail, and will restart
      probe.requestNext("a")
      probe.requestNext("b")
      probe.requestNext("c")
      probe.expectComplete()

      created.get() should ===(2)

      probe.cancel()
    }

    "restart on failure when only due to failures should be restarted" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .onFailuresWithBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Source(List("a", "b", "c")).map {
            case "c"   => throw TE("failed")
            case other => other
          }
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("b")
      probe.requestNext("a")
      probe.requestNext("b")
      probe.requestNext("a")

      created.get() should ===(3)

      probe.cancel()

    }

    "not restart the source when maxRestarts is reached" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(shortRestartSettings.withMaxRestarts(1, shortMinBackoff)) { () =>
          created.incrementAndGet()
          Source.single("a")
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("a")
      probe.expectComplete()

      created.get() should ===(2)

      probe.cancel()
    }

    "reset maxRestarts when source runs for at least minimum backoff without completing" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(restartSettings.withMaxRestarts(2, minBackoff)) { () =>
          created.incrementAndGet()
          Source(List("a"))
        }
        .runWith(TestSink())

      probe.requestNext("a")
      // There should be minBackoff delay
      probe.requestNext("a")
      // The probe should now be backing off again with with increased backoff

      // Now wait for the delay to pass, then it will start the new source, we also want to wait for the
      // subsequent backoff to pass
      Thread.sleep((minBackoff + (minBackoff * 2) + minBackoff + 500.millis).toMillis)

      probe.requestNext("a")
      // We now are able to trigger the third restart, since enough time has elapsed to reset the counter
      probe.requestNext("a")

      created.get() should ===(4)

      probe.cancel()
    }

    "allow using withMaxRestarts instead of minBackoff to determine the maxRestarts reset time" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSource
        .withBackoff(shortRestartSettings.withMaxRestarts(2, 1.second)) { () =>
          created.incrementAndGet()
          Source(List("a", "b")).takeWhile(_ != "b")
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("a")

      Thread.sleep((shortMinBackoff + (shortMinBackoff * 2) + shortMinBackoff).toMillis) // if using shortMinBackoff as deadline cause reset

      probe.requestNext("a")

      probe.request(1)
      probe.expectComplete()

      created.get() should ===(3)

      probe.cancel()
    }

    "provide attributes to inner source" taggedAs TimingTest in {
      val promisedAttributes = Promise[Attributes]()
      RestartSource
        .withBackoff(restartSettings) { () =>
          Source.fromGraph(new AttributesSource().named("inner-name")).mapMaterializedValue(promisedAttributes.success)
        }
        .withAttributes(whateverAttribute("other-thing"))
        .named("outer-name")
        .runWith(Sink.cancelled)

      val attributes = Await.result(promisedAttributes.future, 1.second)
      val name = attributes.get[Name]
      name should contain(Name("inner-name"))
      val whatever = attributes.get[WhateverAttribute]
      whatever should contain(WhateverAttribute("other-thing"))
    }

    "fail the stream when restartOn returns false" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val settings = shortRestartSettings.withRestartOn {
        case TE("failed") => false
        case _            => true
      }
      val probe = RestartSource
        .withBackoff(settings) { () =>
          created.incrementAndGet()
          Source(List("a", "b", "c")).map {
            case "c"   => throw TE("failed")
            case other => other
          }
        }
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("b")
      probe.request(1).expectError(TE("failed"))

      created.get() shouldEqual 1
    }
  }

  "A restart with backoff sink" should {
    "run normally" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val result = Promise[Seq[String]]()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Sink.seq.mapMaterializedValue(result.completeWith)
        })(Keep.left)
        .run()

      probe.sendNext("a")
      probe.sendNext("b")
      probe.sendNext("c")
      probe.sendComplete()

      (result.future.futureValue should contain).inOrderOnly("a", "b", "c")
      created.get() should ===(1)
    }

    "restart on cancellation" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("a")
      sinkProbe.requestNext("a")
      probe.sendNext("b")
      sinkProbe.requestNext("b")
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      probe.sendNext("c")
      sinkProbe.requestNext("c")

      created.get() should ===(2)

      sinkProbe.cancel()
      probe.sendComplete()
    }

    "backoff before restart" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(restartSettings) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("a")
      sinkProbe.requestNext("a")
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      probe.sendNext("b")
      val deadline = (minBackoff - 1.millis).fromNow
      sinkProbe.request(1)
      sinkProbe.expectNext("b")
      deadline.isOverdue() should be(true)

      created.get() should ===(2)

      sinkProbe.cancel()
      probe.sendComplete()
    }

    "reset exponential backoff back to minimum when sink runs for at least minimum backoff without completing" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(restartSettings) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("a")
      sinkProbe.requestNext("a")
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      // There should be a minBackoff delay
      probe.sendNext("b")
      sinkProbe.requestNext("b")
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      sinkProbe.request(1)
      // The probe should now be backing off for 2 * minBackoff

      // Now wait for the 2 * minBackoff delay to pass, then it will start the new source, we also want to wait for the
      // subsequent minBackoff min backoff to pass, so it resets the restart count
      Thread.sleep((minBackoff + (minBackoff * 2) + minBackoff + 500.millis).toMillis)

      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")

      // We should have reset, so the restart delay should be back to minBackoff, ie we should definitely receive the
      // next element within < 2 * minBackoff
      probe.sendNext("c")
      sinkProbe.request(1)
      sinkProbe.expectNext((2 * minBackoff) - 10.millis, "c")

      created.get() should ===(4)

      sinkProbe.cancel()
      probe.sendComplete()
    }

    "not restart the sink when completed while backing off" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(restartSettings) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("a")
      sinkProbe.requestNext("a")
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      // Should be backing off now
      probe.sendComplete()

      // Wait to ensure it isn't restarted
      Thread.sleep((minBackoff + 100.millis).toMillis)
      created.get() should ===(1)

      sinkProbe.cancel()
    }

    "not restart the sink when maxRestarts is reached" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(shortRestartSettings.withMaxRestarts(1, shortMinBackoff)) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")

      probe.expectCancellation()

      created.get() should ===(2)

      sinkProbe.cancel()
      probe.sendComplete()
    }

    "reset maxRestarts when sink runs for at least minimum backoff without completing" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(restartSettings.withMaxRestarts(2, minBackoff)) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      // There should be a minBackoff delay
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      // The probe should now be backing off for 2 * minBackoff

      // Now wait for the 2 * minBackoff delay to pass, then it will start the new source, we also want to wait for the
      // subsequent minBackoff to pass, so it resets the restart count
      Thread.sleep((minBackoff + (minBackoff * 2) + minBackoff + 500.millis).toMillis)

      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")

      // We now are able to trigger the third restart, since enough time has elapsed to reset the counter
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")

      created.get() should ===(4)

      sinkProbe.cancel()
      probe.sendComplete()
    }

    "allow using withMaxRestarts instead of minBackoff to determine the maxRestarts reset time" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (queue, sinkProbe) = TestSource[String]().toMat(TestSink())(Keep.both).run()
      val probe = TestSource[String]()
        .toMat(RestartSink.withBackoff(shortRestartSettings.withMaxRestarts(2, 1.second)) { () =>
          created.incrementAndGet()
          Flow[String].takeWhile(_ != "cancel", inclusive = true).to(Sink.foreach(queue.sendNext))
        })(Keep.left)
        .run()

      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      // There should be a shortMinBackoff delay
      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")
      // The probe should now be backing off for 2 * shortMinBackoff

      Thread.sleep((shortMinBackoff + (shortMinBackoff * 2) + minBackoff).toMillis) // if using shortMinBackoff as deadline cause reset

      probe.sendNext("cancel")
      sinkProbe.requestNext("cancel")

      // We cannot get a final element
      probe.sendNext("cancel")
      sinkProbe.request(1)
      sinkProbe.expectNoMessage()

      created.get() should ===(3)

      sinkProbe.cancel()
      probe.sendComplete()
    }

    "provide attributes to inner sink" taggedAs TimingTest in {
      val promisedAttributes = Promise[Attributes]()
      RestartSink
        .withBackoff(restartSettings) { () =>
          Sink.fromGraph(new AttributesSink().named("inner-name")).mapMaterializedValue(promisedAttributes.success)
        }
        .withAttributes(whateverAttribute("other-thing"))
        .named("outer-name")
        .runWith(Source.empty)

      val attributes = Await.result(promisedAttributes.future, 1.second)
      val name = attributes.get[Name]
      name shouldBe Some(Name("inner-name"))
      val whatever = attributes.get[WhateverAttribute]
      whatever shouldBe Some(WhateverAttribute("other-thing"))
    }

    "fail the stream when restartOn returns false" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val settings = shortRestartSettings.withRestartOn {
        case TE("failed") => false
        case _            => true
      }
      val probe = RestartSink
        .withBackoff(settings) { () =>
          created.incrementAndGet()
          Sink.foreach[String] {
            case "c" => throw TE("failed")
            case _   =>
          }
        }
        .runWith(TestSource[String]())

      probe.sendNext("a")
      probe.sendNext("b")
      probe.sendNext("c")
      probe.expectCancellationWithCause(TE("failed"))

      created.get() shouldEqual 1
    }
  }

  "A restart with backoff flow" should {

    // helps reuse all the setupFlow code for both methods: withBackoff, and onlyOnFailuresWithBackoff
    def RestartFlowFactory[In, Out](
        onlyOnFailures: Boolean,
        settings: RestartSettings): (() => Flow[In, Out, _]) => Flow[In, Out, NotUsed] =
      if (onlyOnFailures) RestartFlow.onFailuresWithBackoff(settings)
      else RestartFlow.withBackoff(settings)

    def setupFlow(
        minBackoff: FiniteDuration,
        maxBackoff: FiniteDuration,
        maxRestarts: Int = -1,
        onlyOnFailures: Boolean = false) = {
      val created = new AtomicInteger()

      val flowInProbe = TestProbe("in-probe")

      val (flowOutProbe: TestPublisher.Probe[String], flowOutSource: Source[String, NotUsed]) =
        TestSource[String]().toMat(BroadcastHub.sink)(Keep.both).run()

      // We can't just use ordinary probes here because we're expecting them to get started/restarted. Instead, we
      // simply use the probes as a message bus for feeding and capturing events.
      val (source, sink) = TestSource[String]()
        .viaMat(
          RestartFlowFactory(
            onlyOnFailures,
            RestartSettings(minBackoff, maxBackoff, 0)
              .withMaxRestarts(maxRestarts, minBackoff)
              .withLogSettings(logSettings)) { () =>
            created.incrementAndGet()
            Flow.fromSinkAndSource(
              Flow[String]
                .takeWhile(_ != "cancel")
                .map {
                  case "in error" => throw TE("in error")
                  case other      => other
                }
                .to(Sink
                  .foreach[String] { msg =>
                    flowInProbe.ref ! msg
                  }
                  .mapMaterializedValue(_.onComplete {
                    case Success(_) => flowInProbe.ref ! "in complete"
                    case Failure(_) => flowInProbe.ref ! "in error"
                  })),
              flowOutSource
                .takeWhile(_ != "complete")
                .map {
                  case "error" => throw TE("error")
                  case other   => other
                }
                .watchTermination()((_, term) =>
                  term.foreach(_ => {
                    flowInProbe.ref ! "out complete"
                  })))
          })(Keep.left)
        .toMat(TestSink[String]())(Keep.both)
        .run()

      (created, source, flowInProbe, flowOutProbe, sink)
    }

    "run normally" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val (source, sink) = TestSource[String]()
        .viaMat(RestartFlow.withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          Flow[String]
        })(Keep.left)
        .toMat(TestSink[String]())(Keep.both)
        .run()

      source.sendNext("a")
      sink.requestNext("a")
      source.sendNext("b")
      sink.requestNext("b")

      created.get() should ===(1)

      source.sendComplete()
    }

    "restart on cancellation" taggedAs TimingTest in {
      val (created, source, flowInProbe, flowOutProbe, sink) = setupFlow(shortMinBackoff, shortMaxBackoff)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      source.sendNext("cancel")
      flowInProbe.expectMsgAllOf("in complete", "out complete")

      // and it should restart
      source.sendNext("c")
      flowInProbe.expectMsg("c")
      flowOutProbe.sendNext("d")
      sink.requestNext("d")

      created.get() should ===(2)
    }

    "restart on completion" taggedAs TimingTest in {
      val (created, source, flowInProbe, flowOutProbe, sink) = setupFlow(shortMinBackoff, shortMaxBackoff)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      EventFilter.info(start = "Restarting stream due to completion", occurrences = 1).intercept {
        sink.request(1)
        flowOutProbe.sendNext("complete")

        flowInProbe.expectMsgAllOf("in complete", "out complete")
      }

      // and it should restart
      source.sendNext("c")
      flowInProbe.expectMsg("c")
      flowOutProbe.sendNext("d")
      sink.requestNext("d")

      created.get() should ===(2)
    }

    "restart on failure" taggedAs TimingTest in {
      val (created, source, flowInProbe, flowOutProbe, sink) = setupFlow(shortMinBackoff, shortMaxBackoff)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      EventFilter.info(start = "Restarting stream due to failure", occurrences = 1).intercept {
        sink.request(1)
        flowOutProbe.sendNext("error")

        // This should complete the in probe
        flowInProbe.expectMsg("in complete")
      }

      // and it should restart
      source.sendNext("c")
      flowInProbe.expectMsg("c")
      flowOutProbe.sendNext("d")
      sink.requestNext("d")

      created.get() should ===(2)
    }

    "backoff before restart" taggedAs TimingTest in {
      val (created, source, flowInProbe, flowOutProbe, sink) = setupFlow(minBackoff, maxBackoff)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      // we need to start counting time before we issue the cancel signal,
      // as starting the counter anywhere after the cancel signal, might not
      // capture all of the time, that has been spent for the backoff.
      val deadline = minBackoff.fromNow

      source.sendNext("cancel")
      flowInProbe.expectMsgAllOf("in complete", "out complete")

      source.sendNext("c")
      flowInProbe.expectMsg("c")
      deadline.isOverdue() should be(true)

      created.get() should ===(2)
    }

    "continue running flow out port after in has been sent completion" taggedAs TimingTest in {
      val (created, source, flowInProbe, flowOutProbe, sink) = setupFlow(shortMinBackoff, maxBackoff)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      source.sendComplete()
      flowInProbe.expectMsg("in complete")

      flowOutProbe.sendNext("c")
      sink.requestNext("c")
      flowOutProbe.sendNext("d")
      sink.requestNext("d")

      sink.request(1)
      flowOutProbe.sendComplete()
      flowInProbe.expectMsg("out complete")
      sink.expectComplete()

      created.get() should ===(1)
    }

    "continue running flow in port after out has been cancelled" taggedAs TimingTest in {
      val (created, source, flowflowInProbe, flowOutProbe, sink) = setupFlow(shortMinBackoff, maxBackoff)

      source.sendNext("a")
      flowflowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      sink.cancel()
      flowflowInProbe.expectMsg("out complete")

      source.sendNext("c")
      flowflowInProbe.expectMsg("c")
      source.sendNext("d")
      flowflowInProbe.expectMsg("d")

      source.sendNext("cancel")
      flowflowInProbe.expectMsg("in complete")
      source.expectCancellation()

      created.get() should ===(1)
    }

    "not restart on completion when maxRestarts is reached" taggedAs TimingTest in {
      val (created, _, flowInProbe, flowOutProbe, sink) =
        setupFlow(shortMinBackoff, shortMaxBackoff, maxRestarts = 1)

      sink.request(1)
      flowOutProbe.sendNext("complete")
      flowInProbe.expectMsgAllOf("in complete", "out complete")

      // and it should restart
      sink.request(1)
      flowOutProbe.sendNext("complete")

      // This will complete the flow in probe and cancel the flow out probe
      flowInProbe.expectMsg("out complete")
      flowInProbe.expectNoMessage(shortMinBackoff * 3)
      sink.expectComplete()

      created.get() should ===(2)
    }

    // onlyOnFailures -->
    "stop on cancellation when using onlyOnFailuresWithBackoff" taggedAs TimingTest in {
      val onlyOnFailures = true
      val (created, source, flowInProbe, flowOutProbe, sink) =
        setupFlow(shortMinBackoff, shortMaxBackoff, -1, onlyOnFailures)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      source.sendNext("cancel")
      // This will complete the flow in probe and cancel the flow out probe
      flowInProbe.expectMsg("in complete")

      source.expectCancellation()

      created.get() should ===(1)
    }

    "stop on completion when using onlyOnFailuresWithBackoff" taggedAs TimingTest in {
      val onlyOnFailures = true
      val (created, source, flowInProbe, flowOutProbe, sink) =
        setupFlow(shortMinBackoff, shortMaxBackoff, -1, onlyOnFailures)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      flowOutProbe.sendNext("complete")
      sink.request(1)
      sink.expectComplete()

      created.get() should ===(1)
    }

    "restart on failure when using onlyOnFailuresWithBackoff" taggedAs TimingTest in {
      val (created, source, flowInProbe, flowOutProbe, sink) =
        setupFlow(shortMinBackoff, shortMaxBackoff, -1, true)

      source.sendNext("a")
      flowInProbe.expectMsg("a")
      flowOutProbe.sendNext("b")
      sink.requestNext("b")

      sink.request(1)
      flowOutProbe.sendNext("error")

      // This should complete the in probe
      flowInProbe.expectMsg("in complete")

      // and it should restart
      source.sendNext("c")
      flowInProbe.expectMsg("c")
      flowOutProbe.sendNext("d")
      sink.requestNext("d")

      created.get() should ===(2)
    }

    "onFailuresWithBackoff, wrapped flow exception should restart configured times" taggedAs TimingTest in {
      val flowCreations = new AtomicInteger(0)
      val failsSomeTimes = Flow[Int].map { i =>
        if (i % 3 == 0) throw TE("fail") else i
      }

      val restartOnFailures =
        RestartFlow
          .onFailuresWithBackoff(
            RestartSettings(1.second, 2.seconds, 0.2).withMaxRestarts(2, 1.second).withLogSettings(logSettings))(() => {
            flowCreations.incrementAndGet()
            failsSomeTimes
          })
          .addAttributes(Attributes(Delay(100.millis)))

      val elements = Source(1 to 7).via(restartOnFailures).runWith(Sink.seq).futureValue
      elements shouldEqual List(1, 2, 4, 5, 7)
      flowCreations.get() shouldEqual 3
    }

    "provide attributes to inner flow" taggedAs TimingTest in {
      val promisedAttributes = Promise[Attributes]()
      RestartFlow
        .withBackoff(restartSettings) { () =>
          Flow.fromGraph(new AttributesFlow().named("inner-name")).mapMaterializedValue(promisedAttributes.success)
        }
        .withAttributes(whateverAttribute("other-thing"))
        .named("outer-name")
        .runWith(Source.empty, Sink.ignore)

      val attributes = Await.result(promisedAttributes.future, 1.second)
      val name = attributes.get[Name]
      name shouldBe Some(Name("inner-name"))
      val whatever = attributes.get[WhateverAttribute]
      whatever shouldBe Some(WhateverAttribute("other-thing"))
    }

    "fail the stream when restartOn returns false" taggedAs TimingTest in {
      val created = new AtomicInteger(0)
      val settings = shortRestartSettings.withRestartOn {
        case TE("failed") => false
        case _            => true
      }
      val probe = Source(List("a", "b", "c", "d"))
        .via(RestartFlow.onFailuresWithBackoff(settings) { () =>
          created.incrementAndGet()
          Flow[String].map {
            case "c"   => throw TE("failed")
            case other => other
          }
        })
        .runWith(TestSink())

      probe.requestNext("a")
      probe.requestNext("b")
      probe.request(1).expectError(TE("failed"))

      created.get() shouldEqual 1
    }
  }

  "A restart with backoff source with context" should {
    "run normally" taggedAs TimingTest in {
      val created = new AtomicInteger
      val probe = RestartSourceWithContext
        .withBackoff(shortRestartSettings) { () =>
          created.incrementAndGet()
          SourceWithContext.fromTuples(Source.unfold(0) { state =>
            Some((state + 1) -> ("a" -> state))
          })
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 0)
      probe.requestNext("a" -> 1)
      probe.requestNext("a" -> 2)
      probe.requestNext("a" -> 3)

      created.get shouldBe 1

      probe.cancel()
    }

    "restart on completion" taggedAs TimingTest in {
      val created = new AtomicInteger

      val probe = RestartSourceWithContext
        .withBackoff(shortRestartSettings) { () =>
          val count = created.getAndIncrement() * 2
          SourceWithContext.fromTuples(Source(Seq("a" -> count, "b" -> (count + 1))))
        }
        .runWith(TestSink())

      EventFilter.info(start = "Restarting stream due to completion", occurrences = 2).intercept {
        probe.requestNext("a" -> 0)
        probe.requestNext("b" -> 1)
        probe.requestNext("a" -> 2)
        probe.requestNext("b" -> 3)
        probe.requestNext("a" -> 4)
      }

      created.get() shouldBe 3

      probe.cancel()
    }

    "restart on failure" taggedAs TimingTest in {
      val created = new AtomicInteger

      val sourceFactory = { () =>
        SourceWithContext
          .fromTuples(Source(Seq("a", "b", "c")).statefulMap(() => created.getAndIncrement() * 3)({ (offset, elem) =>
            (offset + 1) -> (elem -> offset)
          }, _ => None))
          .map { (elem: String) =>
            if (elem == "c") throw TE("failed")
            else elem
          }
      }

      val probe =
        RestartSourceWithContext.withBackoff(shortRestartSettings)(sourceFactory).runWith(TestSink())

      EventFilter.info(start = "Restarting stream due to failure", occurrences = 2).intercept {
        probe.requestNext("a" -> 0)
        probe.requestNext("b" -> 1)
        // offset 2 is "c" which blew up, triggering a restart
        probe.requestNext("a" -> 3)
        probe.requestNext("b" -> 4)
        // offset 5 is "c", dropped in the restarting
        probe.requestNext("a" -> 6)
      }

      created.get() shouldBe 3

      probe.cancel()
    }

    "backoff before restart" taggedAs TimingTest in {
      val created = new AtomicInteger

      val probe = RestartSourceWithContext
        .withBackoff(restartSettings) { () =>
          val count = created.getAndIncrement() * 2
          SourceWithContext.fromTuples(Source(Seq("a" -> count, "b" -> (count + 1))))
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 0)
      probe.requestNext("b" -> 1)

      val deadline = (minBackoff - 1.millis).fromNow
      probe.request(1)

      probe.expectNext("a" -> 2)
      deadline.isOverdue() shouldBe true

      created.get() shouldBe 2

      probe.cancel()
    }

    "reset exponential backoff back to minimum when source runs for at least minimum backoff without completing" taggedAs TimingTest in {
      val created = new AtomicInteger
      val probe = RestartSourceWithContext
        .withBackoff(restartSettings) { () =>
          val count = created.getAndIncrement() * 2
          SourceWithContext.fromTuples(Source(Seq("a" -> count, "b" -> (count + 1))))
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 0)
      probe.requestNext("b" -> 1)

      val deadline = (minBackoff - 1.millis).fromNow
      probe.request(1)
      probe.expectNext("a" -> 2)
      deadline.isOverdue() shouldBe true
      probe.requestNext("b" -> 3)

      probe.request(1)
      // The probe should now back off again with increased backoff

      // Wait for the delay, then subsequent backoff, to pass, so the restart count is reset
      Thread.sleep(((minBackoff * 3) + 500.millis).toMillis)

      probe.expectNext("a" -> 4)
      probe.requestNext("b" -> 5)

      probe.requestNext(2 * minBackoff - 1.milli) should be("a" -> 6)

      created.get() shouldBe 4

      probe.cancel()
    }

    "cancel the currently running SourceWithContext when canceled" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val promise = Promise[Done]()
      val probe = RestartSourceWithContext
        .withBackoff(shortRestartSettings) { () =>
          SourceWithContext.fromTuples(Source.repeat("a").map { _ -> created.getAndIncrement() }.watchTermination() {
            (_, term) =>
              promise.completeWith(term)
          })
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 0)
      probe.cancel()

      promise.future.futureValue shouldBe Done

      // wait to ensure that it isn't restarted
      Thread.sleep(200)
      created.get() shouldBe 1
    }

    "not restart the SourceWithContext when cancelled while backing off" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSourceWithContext
        .withBackoff(restartSettings) { () =>
          created.getAndIncrement()
          SourceWithContext.fromTuples(Source.single("a" -> 1))
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 1)
      probe.request(1)
      // back-off delays the restart (racy...)
      probe.cancel()

      // wait to ensure it isn't restarted
      Thread.sleep((minBackoff + 100.millis).toMillis)
      created.get() shouldBe 1
    }

    "stop on completion if it should only be restarted on failures" taggedAs TimingTest in {
      val created = new AtomicInteger()

      val cgai = { () =>
        created.getAndIncrement()
      }

      val probe = RestartSourceWithContext
        .onFailuresWithBackoff(shortRestartSettings) { () =>
          cgai()
          SourceWithContext.fromTuples(Source(Seq("a" -> cgai(), "b" -> cgai(), "c" -> cgai()))).map {
            case "c"   => if (created.get() <= 4) throw new TE("failed") else "c"
            case other => other
          }
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 1)
      probe.requestNext("b" -> 2)
      // fails and restarts
      probe.requestNext("a" -> 5)
      probe.requestNext("b" -> 6)
      probe.requestNext("c" -> 7)
      probe.expectComplete()

      created.get() shouldBe 8

      probe.cancel()
    }

    "restart on failure when only due to failures should be restarted" taggedAs TimingTest in {
      val created = new AtomicInteger()

      val sourceFactory = { () =>
        SourceWithContext
          .fromTuples(Source(Seq("a", "b", "c")).statefulMap(() => created.getAndIncrement() * 3)({ (offset, elem) =>
            (offset + 1) -> (elem -> offset)
          }, _ => None))
          .map { elem =>
            if (elem == "c") throw TE("failed")
            else elem
          }
      }

      val probe =
        RestartSourceWithContext.onFailuresWithBackoff(shortRestartSettings)(sourceFactory).runWith(TestSink())

      probe.requestNext("a" -> 0)
      probe.requestNext("b" -> 1)
      // offset 2 is "c" which blew up, triggering a restart
      probe.requestNext("a" -> 3)
      probe.requestNext("b" -> 4)
      // offset 5 is "c", dropped in the restarting
      probe.requestNext("a" -> 6)

      created.get() shouldBe 3

      probe.cancel()
    }

    "not restart when maxRestarts is reached" taggedAs TimingTest in {
      val created = new AtomicInteger()
      val probe = RestartSourceWithContext
        .withBackoff(shortRestartSettings.withMaxRestarts(1, shortMinBackoff)) { () =>
          SourceWithContext.fromTuples(Source.single("a").map(_ -> created.getAndIncrement()))
        }
        .runWith(TestSink())

      probe.requestNext("a" -> 0)
      probe.requestNext("a" -> 1)
      probe.expectComplete()

      created.get() shouldBe 2

      probe.cancel()
    }
  }
}
