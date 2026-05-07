/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import com.typesafe.config.ConfigFactory
import org.reactivestreams.{ Publisher, Subscriber }

import akka.NotUsed
import akka.stream._
import akka.stream.impl._
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler }
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }
import akka.testkit.TestDuration

object FlowSpec {
  class Fruit extends Serializable
  class Apple extends Fruit
  class Orange extends Fruit
  val fruits = () =>
    new Iterator[Fruit] {
      override def hasNext: Boolean = true
      override def next(): Fruit = if (ThreadLocalRandom.current().nextBoolean()) new Apple else new Orange
    }

}

@nowarn // tests type assignments compile
class FlowSpec extends StreamSpec(ConfigFactory.parseString("akka.actor.debug.receive=off\nakka.loglevel=INFO")) {
  import FlowSpec._

  val settings = ActorMaterializerSettings(system).withInputBuffer(initialSize = 2, maxSize = 2)

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  val identity: Flow[Any, Any, NotUsed] => Flow[Any, Any, NotUsed] = in => in.map(e => e)
  val identity2: Flow[Any, Any, NotUsed] => Flow[Any, Any, NotUsed] = in => identity(in)

  val toPublisher: (Source[Any, _], Materializer) => Publisher[Any] =
    (f, m) => f.runWith(Sink.asPublisher(false))(m)

  def toFanoutPublisher[In, Out](elasticity: Int): (Source[Out, _], Materializer) => Publisher[Out] =
    (f, m) => f.runWith(Sink.asPublisher(true).withAttributes(Attributes.inputBuffer(elasticity, elasticity)))(m)

  def materializeIntoSubscriberAndPublisher[In, Out](flow: Flow[In, Out, _]): (Subscriber[In], Publisher[Out]) = {
    flow.runWith(Source.asSubscriber[In], Sink.asPublisher[Out](false))
  }

  "A Flow" must {

    for ((name, op) <- List("identity" -> identity, "identity2" -> identity2); n <- List(1, 2, 4)) {
      s"request initial elements from upstream ($name, $n)" in {
        new ChainSetup(op, settings.withInputBuffer(initialSize = n, maxSize = n), toPublisher) {
          upstream.expectRequest(upstreamSubscription, this.settings.maxInputBufferSize)
        }
      }
    }

    "request more elements from upstream when downstream requests more elements" in {
      new ChainSetup(identity, settings, toPublisher) {
        upstream.expectRequest(upstreamSubscription, this.settings.maxInputBufferSize)
        downstreamSubscription.request(1)
        upstream.expectNoMessage(100.millis)
        downstreamSubscription.request(2)
        upstream.expectNoMessage(100.millis)
        upstreamSubscription.sendNext("a")
        downstream.expectNext("a")
        upstream.expectRequest(upstreamSubscription, 1)
        upstream.expectNoMessage(100.millis)
        upstreamSubscription.sendNext("b")
        upstreamSubscription.sendNext("c")
        upstreamSubscription.sendNext("d")
        downstream.expectNext("b")
        downstream.expectNext("c")
      }
    }

    "deliver events when publisher sends elements and then completes" in {
      new ChainSetup(identity, settings, toPublisher) {
        downstreamSubscription.request(1)
        upstreamSubscription.sendNext("test")
        upstreamSubscription.sendComplete()
        downstream.expectNext("test")
        downstream.expectComplete()
      }
    }

    "deliver complete signal when publisher immediately completes" in {
      new ChainSetup(identity, settings, toPublisher) {
        upstreamSubscription.sendComplete()
        downstream.expectComplete()
      }
    }

    "deliver error signal when publisher immediately fails" in {
      new ChainSetup(identity, settings, toPublisher) {
        object WeirdError extends RuntimeException("weird test exception")
        upstreamSubscription.sendError(WeirdError)
        downstream.expectError(WeirdError)
      }
    }

    "cancel upstream when single subscriber cancels subscription while receiving data" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toPublisher) {
        downstreamSubscription.request(5)
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("test")
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("test2")
        upstreamSubscription.expectRequest(1)
        downstream.expectNext("test")
        downstream.expectNext("test2")
        downstreamSubscription.cancel()

        // because of the "must cancel its upstream Subscription if its last downstream Subscription has been canceled" rule
        upstreamSubscription.expectCancellation()
      }
    }

    "materialize into Publisher/Subscriber" in {
      val flow = Flow[String]
      val (flowIn: Subscriber[String], flowOut: Publisher[String]) = materializeIntoSubscriberAndPublisher(flow)

      val c1 = TestSubscriber.manualProbe[String]()
      flowOut.subscribe(c1)

      val source: Publisher[String] = Source(List("1", "2", "3")).runWith(Sink.asPublisher(false))
      source.subscribe(flowIn)

      val sub1 = c1.expectSubscription()
      sub1.request(3)
      c1.expectNext("1")
      c1.expectNext("2")
      c1.expectNext("3")
      c1.expectComplete()
    }

    "materialize into Publisher/Subscriber and transformation processor" in {
      val flow = Flow[Int].map((i: Int) => i.toString)
      val (flowIn: Subscriber[Int], flowOut: Publisher[String]) = materializeIntoSubscriberAndPublisher(flow)

      val c1 = TestSubscriber.manualProbe[String]()
      flowOut.subscribe(c1)
      val sub1 = c1.expectSubscription()
      sub1.request(3)
      c1.expectNoMessage(200.millis)

      val source: Publisher[Int] = Source(List(1, 2, 3)).runWith(Sink.asPublisher(false))
      source.subscribe(flowIn)

      c1.expectNext("1")
      c1.expectNext("2")
      c1.expectNext("3")
      c1.expectComplete()
    }

    "materialize into Publisher/Subscriber and multiple transformation processors" in {
      val flow = Flow[Int].map(_.toString).map("elem-" + _)
      val (flowIn, flowOut) = materializeIntoSubscriberAndPublisher(flow)

      val c1 = TestSubscriber.manualProbe[String]()
      flowOut.subscribe(c1)
      val sub1 = c1.expectSubscription()
      sub1.request(3)
      c1.expectNoMessage(200.millis)

      val source: Publisher[Int] = Source(List(1, 2, 3)).runWith(Sink.asPublisher(false))
      source.subscribe(flowIn)

      c1.expectNext("elem-1")
      c1.expectNext("elem-2")
      c1.expectNext("elem-3")
      c1.expectComplete()
    }

    "subscribe Subscriber" in {
      val flow: Flow[String, String, _] = Flow[String]
      val c1 = TestSubscriber.manualProbe[String]()
      val sink: Sink[String, _] = flow.to(Sink.fromSubscriber(c1))
      val publisher: Publisher[String] = Source(List("1", "2", "3")).runWith(Sink.asPublisher(false))
      Source.fromPublisher(publisher).to(sink).run()

      val sub1 = c1.expectSubscription()
      sub1.request(3)
      c1.expectNext("1")
      c1.expectNext("2")
      c1.expectNext("3")
      c1.expectComplete()
    }

    "perform transformation operation" in {
      val flow = Flow[Int].map(i => { testActor ! i.toString; i.toString })

      val publisher = Source(List(1, 2, 3)).runWith(Sink.asPublisher(false))
      Source.fromPublisher(publisher).via(flow).to(Sink.ignore).run()

      expectMsg("1")
      expectMsg("2")
      expectMsg("3")
    }

    "perform contramap operation" in {
      val flow = Flow[Int].contramap(Integer.parseInt)
      val sub = Source(List("1", "2", "3")).via(flow).runWith(TestSink())
      sub.request(3)
      sub.expectNextN(List(1, 2, 3))
      sub.expectComplete()
    }

    "perform transformation operation and subscribe Subscriber" in {
      val flow = Flow[Int].map(_.toString)
      val c1 = TestSubscriber.manualProbe[String]()
      val sink: Sink[Int, _] = flow.to(Sink.fromSubscriber(c1))
      val publisher: Publisher[Int] = Source(List(1, 2, 3)).runWith(Sink.asPublisher(false))
      Source.fromPublisher(publisher).to(sink).run()

      val sub1 = c1.expectSubscription()
      sub1.request(3)
      c1.expectNext("1")
      c1.expectNext("2")
      c1.expectNext("3")
      c1.expectComplete()
    }

    "be materializable several times with fanout publisher" in {
      val flow = Source(List(1, 2, 3)).map(_.toString)
      val p1 = flow.runWith(Sink.asPublisher(true))
      val p2 = flow.runWith(Sink.asPublisher(true))
      val s1 = TestSubscriber.manualProbe[String]()
      val s2 = TestSubscriber.manualProbe[String]()
      val s3 = TestSubscriber.manualProbe[String]()
      p1.subscribe(s1)
      p2.subscribe(s2)
      p2.subscribe(s3)

      val sub1 = s1.expectSubscription()
      val sub2 = s2.expectSubscription()
      val sub3 = s3.expectSubscription()

      sub1.request(3)
      s1.expectNext("1")
      s1.expectNext("2")
      s1.expectNext("3")
      s1.expectComplete()

      sub2.request(3)
      sub3.request(3)
      s2.expectNext("1")
      s2.expectNext("2")
      s2.expectNext("3")
      s2.expectComplete()
      s3.expectNext("1")
      s3.expectNext("2")
      s3.expectNext("3")
      s3.expectComplete()
    }

    "be covariant" in {
      val f1: Source[Fruit, _] = Source.fromIterator[Fruit](fruits)
      val p1: Publisher[Fruit] = Source.fromIterator[Fruit](fruits).runWith(Sink.asPublisher(false))
      val f2: SubFlow[Fruit, _, Source[Fruit, NotUsed]#Repr, _] =
        Source.fromIterator[Fruit](fruits).splitWhen(_ => true)
      val f3: SubFlow[Fruit, _, Source[Fruit, NotUsed]#Repr, _] =
        Source.fromIterator[Fruit](fruits).groupBy(2, _ => true)
      val f4: Source[(immutable.Seq[Fruit], Source[Fruit, _]), _] = Source.fromIterator[Fruit](fruits).prefixAndTail(1)
      val d1: SubFlow[Fruit, _, Flow[String, Fruit, NotUsed]#Repr, _] =
        Flow[String].map(_ => new Apple).splitWhen(_ => true)
      val d2: SubFlow[Fruit, _, Flow[String, Fruit, NotUsed]#Repr, _] =
        Flow[String].map(_ => new Apple).groupBy(2, _ => true)
      val d3: Flow[String, (immutable.Seq[Apple], Source[Fruit, _]), _] =
        Flow[String].map(_ => new Apple).prefixAndTail(1)
    }

    "be possible to convert to a processor, and should be able to take a Processor" in {
      val identity1 = Flow[Int].toProcessor
      val identity2 = Flow.fromProcessor(() => identity1.run())
      Await.result(Source(1 to 10).via(identity2).limit(100).runWith(Sink.seq), 3.seconds) should ===(1 to 10)

      // Reusable:
      Await.result(Source(1 to 10).via(identity2).limit(100).runWith(Sink.seq), 3.seconds) should ===(1 to 10)
    }

    "eliminate passed in when matval from passed in not used" in {
      val map = Flow.fromFunction((n: Int) => n + 1)
      val result = map.viaMat(Flow[Int])(Keep.left)
      result shouldBe theSameInstanceAs(map)
    }

    "not eliminate passed in when matval from passed in is used" in {
      val map = Flow.fromFunction((n: Int) => n + 1)
      val result = map.viaMat(Flow[Int])(Keep.right)
      (result shouldNot be).theSameInstanceAs(map)
    }

    "eliminate itself if identity" in {
      val map = Flow.fromFunction((n: Int) => n + 1)
      val result = Flow[Int].viaMat(map)(Keep.right)
      result shouldBe theSameInstanceAs(map)
    }

    "not eliminate itself if identity but matval is used" in {
      val map = Flow.fromFunction((n: Int) => n + 1)
      val result = Flow[Int].viaMat(map)(Keep.left)
      (result shouldNot be).theSameInstanceAs(map)
    }

  }

  "A Flow with multiple subscribers (FanOutBox)" must {
    "adapt speed to the currently slowest subscriber" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toFanoutPublisher(1)) {
        val downstream2 = TestSubscriber.manualProbe[Any]()
        publisher.subscribe(downstream2)
        val downstream2Subscription = downstream2.expectSubscription()

        downstreamSubscription.request(5)
        upstream.expectRequest(upstreamSubscription, 1) // because initialInputBufferSize=1

        upstreamSubscription.sendNext("firstElement")
        downstream.expectNext("firstElement")

        upstream.expectRequest(upstreamSubscription, 1)
        upstreamSubscription.sendNext("element2")

        downstream.expectNoMessage(1.second)
        downstream2Subscription.request(1)
        downstream2.expectNext("firstElement")

        downstream.expectNext("element2")

        downstream2Subscription.request(1)
        downstream2.expectNext("element2")
      }
    }

    "support slow subscriber with fan-out 2" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toFanoutPublisher(2)) {
        val downstream2 = TestSubscriber.manualProbe[Any]()
        publisher.subscribe(downstream2)
        val downstream2Subscription = downstream2.expectSubscription()

        downstreamSubscription.request(5)

        upstream.expectRequest(upstreamSubscription, 1) // because initialInputBufferSize=1
        upstreamSubscription.sendNext("element1")
        downstream.expectNext("element1")
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("element2")
        downstream.expectNext("element2")
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("element3")
        // downstream2 has not requested anything, fan-out buffer 2
        downstream.expectNoMessage(100.millis.dilated)

        downstream2Subscription.request(2)
        downstream.expectNext("element3")
        downstream2.expectNext("element1")
        downstream2.expectNext("element2")
        downstream2.expectNoMessage(100.millis.dilated)

        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("element4")
        downstream.expectNext("element4")

        downstream2Subscription.request(2)
        downstream2.expectNext("element3")
        downstream2.expectNext("element4")

        upstreamSubscription.sendComplete()
        downstream.expectComplete()
        downstream2.expectComplete()
      }
    }

    "support incoming subscriber while elements were requested before" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toFanoutPublisher(1)) {
        downstreamSubscription.request(5)
        upstream.expectRequest(upstreamSubscription, 1)
        upstreamSubscription.sendNext("a1")
        downstream.expectNext("a1")

        upstream.expectRequest(upstreamSubscription, 1)
        upstreamSubscription.sendNext("a2")
        downstream.expectNext("a2")

        upstream.expectRequest(upstreamSubscription, 1)

        // link now while an upstream element is already requested
        val downstream2 = TestSubscriber.manualProbe[Any]()
        publisher.subscribe(downstream2)
        val downstream2Subscription = downstream2.expectSubscription()

        // situation here:
        // downstream 1 now has 3 outstanding
        // downstream 2 has 0 outstanding

        upstreamSubscription.sendNext("a3")
        downstream.expectNext("a3")
        downstream2.expectNoMessage(100.millis.dilated) // as nothing was requested yet, fanOutBox needs to cache element in this case

        downstream2Subscription.request(1)
        downstream2.expectNext("a3")

        // d1 now has 2 outstanding
        // d2 now has 0 outstanding
        // buffer should be empty so we should be requesting one new element

        upstream.expectRequest(upstreamSubscription, 1) // because of buffer size 1
      }
    }

    "be unblocked when blocking subscriber cancels subscription" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toFanoutPublisher(1)) {
        val downstream2 = TestSubscriber.manualProbe[Any]()
        publisher.subscribe(downstream2)
        val downstream2Subscription = downstream2.expectSubscription()

        downstreamSubscription.request(5)
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("firstElement")
        downstream.expectNext("firstElement")

        downstream2Subscription.request(1)
        downstream2.expectNext("firstElement")
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("element2")

        downstream.expectNext("element2")
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.sendNext("element3")
        upstreamSubscription.expectRequest(1)

        downstream.expectNoMessage(200.millis.dilated)
        downstream2.expectNoMessage(200.millis.dilated)
        upstream.expectNoMessage(200.millis.dilated)

        // should unblock fanoutbox
        downstream2Subscription.cancel()
        downstream.expectNext("element3")
        upstreamSubscription.sendNext("element4")
        downstream.expectNext("element4")

        upstreamSubscription.sendComplete()
        downstream.expectComplete()
      }
    }

    "call future subscribers' onError after onSubscribe if initial upstream was completed" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toFanoutPublisher(1)) {
        val downstream2 = TestSubscriber.manualProbe[Any]()
        // don't link it just yet

        downstreamSubscription.request(5)
        upstream.expectRequest(upstreamSubscription, 1)
        upstreamSubscription.sendNext("a1")
        downstream.expectNext("a1")

        upstream.expectRequest(upstreamSubscription, 1)
        upstreamSubscription.sendNext("a2")
        downstream.expectNext("a2")

        upstream.expectRequest(upstreamSubscription, 1)

        // link now while an upstream element is already requested
        publisher.subscribe(downstream2)
        val downstream2Subscription = downstream2.expectSubscription()

        upstreamSubscription.sendNext("a3")
        upstreamSubscription.sendComplete()
        downstream.expectNext("a3")
        downstream.expectComplete()

        downstream2.expectNoMessage(100.millis.dilated) // as nothing was requested yet, fanOutBox needs to cache element in this case

        downstream2Subscription.request(1)
        downstream2.expectNext("a3")
        downstream2.expectComplete()

        val downstream3 = TestSubscriber.manualProbe[Any]()
        publisher.subscribe(downstream3)
        downstream3.expectSubscription()
        downstream3.expectError() should ===(ActorPublisher.NormalShutdownReason)
      }
    }

    "call future subscribers' onError should be called instead of onSubscribed after initial upstream reported an error" in {
      new ChainSetup[Int, String, NotUsed](
        _.map(_ => throw TestException),
        settings.withInputBuffer(initialSize = 1, maxSize = 1),
        toFanoutPublisher(1)) {
        downstreamSubscription.request(1)
        upstreamSubscription.expectRequest(1)

        upstreamSubscription.sendNext(5)
        upstreamSubscription.expectRequest(1)
        upstreamSubscription.expectCancellation()
        downstream.expectError(TestException)

        val downstream2 = TestSubscriber.manualProbe[String]()
        publisher.subscribe(downstream2)
        downstream2.expectSubscriptionAndError() should be(TestException)
      }
    }

    "call future subscribers' onError when all subscriptions were cancelled" in {
      new ChainSetup(identity, settings.withInputBuffer(initialSize = 1, maxSize = 1), toFanoutPublisher(16)) {
        upstreamSubscription.expectRequest(1)
        downstreamSubscription.cancel()
        upstreamSubscription.expectCancellation()

        val downstream2 = TestSubscriber.manualProbe[Any]()
        publisher.subscribe(downstream2)
        // IllegalStateException shut down
        downstream2.expectSubscriptionAndError().isInstanceOf[IllegalStateException] should be(true)
      }
    }

    "should be created from a function easily" in {
      Source(0 to 9).via(Flow.fromFunction(_ + 1)).runWith(Sink.seq).futureValue should ===(1 to 10)
    }
  }

  /**
   * Count elements that passing by this flow
   * */
  private class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], AtomicLong] {
    private val in = Inlet[T]("ElementCounterFlow.in")
    private val out = Outlet[T]("ElementCounterFlow.out")
    val shape = FlowShape(in, out)
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, AtomicLong) = {
      val counter = new AtomicLong()
      val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
        override def onPush(): Unit = {
          counter.incrementAndGet()
          push(out, grab(in))
        }
        override def onPull(): Unit = pull(in)
        setHandlers(in, out, this)
      }
      (logic, counter)
    }
  }

  "Flow pre-materialization" must {
    "passing elements to downstream" in {
      val (counter, flow) = Flow.fromGraph(new CounterFlow[Int]).preMaterialize()
      val probe = Source(List(1, 2, 3)).via(flow).reduce((a, b) => a + b).runWith(TestSink[Int]())
      probe.request(1)
      probe.expectNext(6)
      probe.request(1)
      probe.expectComplete()
      counter.get() should (be(3))
    }

    "propagate failures to downstream" in {
      val (queue, source) = Source.queue[Int](1).preMaterialize()
      val (counter, flow) = Flow.fromGraph(new CounterFlow[Int]).preMaterialize()
      val probe = source.via(flow).runWith(TestSink[Int]())
      queue.offer(1)
      probe.request(1)
      probe.expectNext(1)
      queue.fail(new RuntimeException("boom"))
      probe.expectError().getMessage should ===("boom")
      counter.get() should (be(1))
    }

    "disallow materialize multiple times" in {
      val (counter, flow) = Flow.fromGraph(new CounterFlow[Int]).preMaterialize()
      val probe1 = Source(List(1, 2, 3)).via(flow).reduce((a, b) => a + b).runWith(TestSink[Int]())
      probe1.request(1)
      probe1.expectNext(6)
      probe1.request(1)
      probe1.expectComplete()
      counter.get() should (be(3))
      val probe2 = Source(List(1, 2, 3)).via(flow).reduce((a, b) => a + b).runWith(TestSink[Int]())
      probe2.request(1)
      probe2.expectError()
    }

    "propagate failure to downstream when materializing" in {
      a[RuntimeException] shouldBe thrownBy(
        Flow
          .fromGraph(new CounterFlow[Int])
          .mapMaterializedValue(_ => throw new RuntimeException("boom"))
          .preMaterialize())
    }

    "propagate cancel to upstream" in {
      val (counter, flow) = Flow.fromGraph(new CounterFlow[Int]).preMaterialize()
      val probSource = TestSource[Int]().via(flow).toMat(Sink.cancelled[Int])(Keep.left).run()
      probSource.ensureSubscription()
      probSource.expectCancellation()
      counter.get() should (be(0))
    }

  }

  object TestException extends RuntimeException with NoStackTrace

}
