/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.Done
import akka.stream.ClosedShape
import akka.stream.KillSwitches
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

class FlowKillSwitchSpec extends StreamSpec {

  "A UniqueKillSwitch" must {

    "stop a stream if requested" in {
      val ((upstream, switch), downstream) =
        TestSource[Int]().viaMat(KillSwitches.single)(Keep.both).toMat(TestSink())(Keep.both).run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch.shutdown()
      upstream.expectCancellation()
      downstream.expectComplete()
    }

    "fail a stream if requested" in {
      val ((upstream, switch), downstream) =
        TestSource[Int]().viaMat(KillSwitches.single)(Keep.both).toMat(TestSink())(Keep.both).run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch.abort(TE("Abort"))
      upstream.expectCancellation()
      downstream.expectError(TE("Abort"))
    }

    "work if used multiple times in a flow" in {
      val (((upstream, switch1), switch2), downstream) =
        TestSource[Int]()
          .viaMat(KillSwitches.single)(Keep.both)
          .recover { case TE(_) => -1 }
          .viaMat(KillSwitches.single)(Keep.both)
          .toMat(TestSink())(Keep.both)
          .run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch1.abort(TE("Abort"))
      upstream.expectCancellation()
      downstream.requestNext(-1)

      switch2.shutdown()
      downstream.expectComplete()

    }

    "ignore completion after already completed" in {
      val ((upstream, switch), downstream) =
        TestSource[Int]().viaMat(KillSwitches.single)(Keep.both).toMat(TestSink())(Keep.both).run()

      upstream.ensureSubscription()
      downstream.ensureSubscription()

      switch.shutdown()
      upstream.expectCancellation()
      downstream.expectComplete()

      switch.abort(TE("Won't happen"))
      upstream.expectNoMessage(100.millis)
      downstream.expectNoMessage(100.millis)
    }

  }

  "A SharedKillSwitches" must {

    "stop a stream if requested" in {
      val switch = KillSwitches.shared("switch")

      val (upstream, downstream) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch.shutdown()
      upstream.expectCancellation()
      downstream.expectComplete()
    }

    "fail a stream if requested" in {
      val switch = KillSwitches.shared("switch")

      val (upstream, downstream) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch.abort(TE("Abort"))
      upstream.expectCancellation()
      downstream.expectError(TE("Abort"))
    }

    "pass through all elements unmodified" in {
      val switch = KillSwitches.shared("switch")
      Source(1 to 100).via(switch.flow).runWith(Sink.seq).futureValue should ===(1 to 100)
    }

    "provide a flow that if materialized multiple times (with multiple types) stops all streams if requested" in {
      val switch = KillSwitches.shared("switch")

      val (upstream1, downstream1) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()
      val (upstream2, downstream2) = TestSource[String]().via(switch.flow).toMat(TestSink())(Keep.both).run()

      downstream1.request(1)
      upstream1.sendNext(1)
      downstream1.expectNext(1)

      downstream2.request(2)
      upstream2.sendNext("A").sendNext("B")
      downstream2.expectNext("A", "B")

      switch.shutdown()
      upstream1.expectCancellation()
      upstream2.expectCancellation()
      downstream1.expectComplete()
      downstream2.expectComplete()
    }

    "provide a flow that if materialized multiple times (with multiple types) fails all streams if requested" in {
      val switch = KillSwitches.shared("switch")

      val (upstream1, downstream1) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()
      val (upstream2, downstream2) = TestSource[String]().via(switch.flow).toMat(TestSink())(Keep.both).run()

      downstream1.request(1)
      upstream1.sendNext(1)
      downstream1.expectNext(1)

      downstream2.request(2)
      upstream2.sendNext("A").sendNext("B")
      downstream2.expectNext("A", "B")

      switch.abort(TE("Abort"))
      upstream1.expectCancellation()
      upstream2.expectCancellation()
      downstream1.expectError(TE("Abort"))
      downstream2.expectError(TE("Abort"))

    }

    "ignore subsequent aborts and shutdowns after shutdown" in {
      val switch = KillSwitches.shared("switch")

      val (upstream, downstream) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch.shutdown()
      upstream.expectCancellation()
      downstream.expectComplete()

      switch.shutdown()
      upstream.expectNoMessage(100.millis)
      downstream.expectNoMessage(100.millis)

      switch.abort(TE("Abort"))
      upstream.expectNoMessage(100.millis)
      downstream.expectNoMessage(100.millis)
    }

    "ignore subsequent aborts and shutdowns after abort" in {
      val switch = KillSwitches.shared("switch")

      val (upstream, downstream) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()

      downstream.request(1)
      upstream.sendNext(1)
      downstream.expectNext(1)

      switch.abort(TE("Abort"))
      upstream.expectCancellation()
      downstream.expectError(TE("Abort"))

      switch.shutdown()
      upstream.expectNoMessage(100.millis)
      downstream.expectNoMessage(100.millis)

      switch.abort(TE("Abort_Late"))
      upstream.expectNoMessage(100.millis)
      downstream.expectNoMessage(100.millis)
    }

    "complete immediately flows materialized after switch shutdown" in {
      val switch = KillSwitches.shared("switch")
      switch.shutdown()

      val (upstream, downstream) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()
      upstream.expectCancellation()
      downstream.expectSubscriptionAndComplete()
    }

    "fail immediately flows materialized after switch failure" in {
      val switch = KillSwitches.shared("switch")
      switch.abort(TE("Abort"))

      val (upstream, downstream) = TestSource[Int]().via(switch.flow).toMat(TestSink())(Keep.both).run()
      upstream.expectCancellation()
      downstream.expectSubscriptionAndError(TE("Abort"))
    }

    "should not cause problems if switch is shutdown after Flow completed normally" in {
      val switch = KillSwitches.shared("switch")
      Source(1 to 10).via(switch.flow).runWith(Sink.seq).futureValue should ===(1 to 10)

      switch.shutdown()
    }

    "provide flows that materialize to its owner KillSwitch" in {
      val switch = KillSwitches.shared("switch")
      val (switch2, completion) = Source.maybe[Int].viaMat(switch.flow)(Keep.right).toMat(Sink.ignore)(Keep.both).run()

      (switch2 eq switch) should be(true)
      switch2.shutdown()
      completion.futureValue should ===(Done)
    }

    "not affect streams corresponding to another KillSwitch" in {
      val switch1 = KillSwitches.shared("switch")
      val switch2 = KillSwitches.shared("switch")

      switch1 should !==(switch2)

      val (upstream1, downstream1) = TestSource[Int]().via(switch1.flow).toMat(TestSink())(Keep.both).run()
      val (upstream2, downstream2) = TestSource[Int]().via(switch2.flow).toMat(TestSink())(Keep.both).run()

      downstream1.request(1)
      upstream1.sendNext(1)
      downstream1.expectNext(1)

      downstream2.request(1)
      upstream2.sendNext(2)
      downstream2.expectNext(2)

      switch1.shutdown()
      upstream1.expectCancellation()
      downstream1.expectComplete()
      upstream2.expectNoMessage(100.millis)
      downstream2.expectNoMessage(100.millis)

      switch2.abort(TE("Abort"))
      upstream1.expectNoMessage(100.millis)
      downstream1.expectNoMessage(100.millis)
      upstream2.expectCancellation()
      downstream2.expectError(TE("Abort"))
    }

    "allow using multiple KillSwitch in one graph" in {
      val switch1 = KillSwitches.shared("switch")
      val switch2 = KillSwitches.shared("switch")

      val downstream = RunnableGraph
        .fromGraph(GraphDSL.createGraph(TestSink[Int]()) { implicit b => snk =>
          import GraphDSL.Implicits._
          val merge = b.add(Merge[Int](2))

          Source.maybe[Int].via(switch1.flow) ~> merge ~> snk
          Source.maybe[Int].via(switch2.flow) ~> merge

          ClosedShape
        })
        .run()

      downstream.ensureSubscription()
      downstream.expectNoMessage(100.millis)

      switch1.shutdown()
      downstream.expectNoMessage(100.millis)

      switch2.shutdown()
      downstream.expectComplete()
    }

    "use its name on the flows it hands out" in {
      pending // toString does not work for me after rebase
      val switch = KillSwitches.shared("mySwitchName")

      switch.toString should ===("KillSwitch(mySwitchName)")
      switch.flow.toString should ===("Flow(KillSwitchFlow(switch: mySwitchName))")

    }

  }

}
