/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.testkit.{ StreamSpec, TestPublisher, TestSubscriber }
import akka.testkit.{ AkkaSpec, ExplicitlyTriggeredScheduler }

class AggregateWithBoundarySpec extends StreamSpec {

  "split aggregator by size" in {

    val stream = collection.immutable.Seq(1, 2, 3, 4, 5, 6, 7)
    val groupSize = 3
    val result = Source(stream)
      .aggregateWithBoundary(allocate = () => ListBuffer.empty[Int])(aggregate = (buffer, i) => {
        buffer += i
        (buffer, buffer.size >= groupSize)
      }, harvest = buffer => buffer.toSeq, emitOnTimer = None)
      .runWith(Sink.collection)

    Await.result(result, 10.seconds) should be(stream.grouped(groupSize).toSeq)

  }

  "split aggregator by size and harvest" in {
    val stream = collection.immutable.Seq(1, 2, 3, 4, 5, 6, 7)
    val groupSize = 3
    val result = Source(stream)
      .aggregateWithBoundary(allocate = () => ListBuffer.empty[Int])(
        aggregate = (buffer, i) => {
          buffer += i
          (buffer, buffer.size >= groupSize)
        },
        harvest = buffer => buffer.toSeq :+ -1, // append -1 to output to demonstrate the effect of harvest
        emitOnTimer = None)
      .runWith(Sink.collection)

    Await.result(result, 10.seconds) should be(stream.grouped(groupSize).toSeq.map(seq => seq :+ -1))

  }

  "split aggregator by custom weight condition" in {
    val stream = collection.immutable.Seq(1, 2, 3, 4, 5, 6, 7)
    val weight = 10

    val result = Source(stream)
      .aggregateWithBoundary(allocate = () => ListBuffer.empty[Int])(aggregate = (buffer, i) => {
        buffer += i
        (buffer, buffer.sum >= weight)
      }, harvest = buffer => buffer.toSeq, emitOnTimer = None)
      .runWith(Sink.collection)

    Await.result(result, 10.seconds) should be(Seq(Seq(1, 2, 3, 4), Seq(5, 6), Seq(7)))
  }

}

// To run multiple tests in parallel using simulated timer,
// the tests must be in separate Specs with different instances of the ActorSystem
class AggregateWithTimeBoundaryAndSimulatedTimeSpec extends AnyWordSpecLike with Matchers {

  private def createActorSystem(id: String) =
    ActorSystem(
      s"ActorSystemWithExplicitlyTriggeredScheduler-$id",
      ConfigFactory.load(
        AkkaSpec.testConf.withValue(
          "akka.scheduler.implementation",
          ConfigValueFactory.fromAnyRef("akka.testkit.ExplicitlyTriggeredScheduler"))))

  private def getEts(actor: ActorSystem): ExplicitlyTriggeredScheduler = {
    actor.scheduler match {
      case ets: ExplicitlyTriggeredScheduler => ets
      case other                             => throw new Exception(s"expecting ${classOf[ExplicitlyTriggeredScheduler]} but got ${other.getClass}")
    }
  }

  private def timePasses(amount: FiniteDuration)(implicit actorSystem: ActorSystem): Unit =
    getEts(actorSystem).timePasses(amount)

  private def schedulerTimeMs(implicit actorSystem: ActorSystem): Long = getEts(actorSystem).currentTimeMs

  implicit class SourceWrapper[+Out, +Mat](val source: Source[Out, Mat]) {

    /**
     * This is a convenient wrapper of [[aggregateWithBoundary]] to handle additional time constraints
     * @param maxGap        the gap allowed between consecutive aggregate operations
     * @param maxDuration   the duration of the sequence of aggregate operations from initial seed until emit is triggered
     * @param interval      interval of the timer to check the maxGap and maxDuration condition
     * @param currentTimeMs source of the system time, can be simulated time in testing
     */
    def aggregateWithTimeBoundary[Agg, Emit](allocate: => Agg)(
        aggregate: (Agg, Out) => (Agg, Boolean),
        harvest: Agg => Emit,
        maxGap: Option[FiniteDuration],
        maxDuration: Option[FiniteDuration],
        interval: FiniteDuration,
        currentTimeMs: => Long): source.Repr[Emit] = {
      require(
        maxDuration.nonEmpty || maxGap.nonEmpty,
        s"required timing condition maxGap and maxDuration are both missing, use aggregateWithBoundary if it's intended")

      class ValueTimeWrapper[T](var value: T) {
        var firstTime: Long = -1
        var lastTime: Long = -1
        def updateTime(time: Long): Unit = {
          if (firstTime == -1) firstTime = time
          lastTime = time
        }
      }

      source.aggregateWithBoundary(allocate = () => new ValueTimeWrapper(value = allocate))(aggregate = (agg, in) => {
        agg.updateTime(currentTimeMs)
        // user provided Agg type must be mutable
        val (updated, result) = aggregate(agg.value, in)
        agg.value = updated
        (agg, result)
      }, harvest = agg => harvest(agg.value), emitOnTimer = Some((agg => {
        val currentTime = currentTimeMs
        maxDuration.exists(md => currentTime - agg.firstTime >= md.toMillis) ||
        maxGap.exists(mg => currentTime - agg.lastTime >= mg.toMillis)
      }, interval)))
    }
  }

  "split aggregator by gap for slow upstream" in {

    implicit val actorSystem = createActorSystem("1")

    val maxGap = 20.seconds

    val p = TestPublisher.probe[Int]()

    val result = Source
      .fromPublisher(p)
      .aggregateWithTimeBoundary(allocate = ListBuffer.empty[Int])(
        aggregate = (buffer, i) => {
          buffer += i
          (buffer, false)
        },
        harvest = seq => seq.toSeq,
        maxGap = Some(maxGap), // elements with longer gap will put put to next aggregator
        maxDuration = None,
        currentTimeMs = schedulerTimeMs,
        interval = 1.milli)
      .buffer(1, OverflowStrategy.backpressure)
      .runWith(Sink.collection)

    p.sendNext(1)
    timePasses(maxGap / 2) // less than maxGap should not cause emit
    p.sendNext(2)
    timePasses(maxGap)

    p.sendNext(3)
    timePasses(maxGap / 2) // less than maxGap should not cause emit
    p.sendNext(4)
    timePasses(maxGap)

    p.sendNext(5)
    timePasses(maxGap / 2) // less than maxGap should not cause emit
    p.sendNext(6)
    timePasses(maxGap / 2) // less than maxGap should not cause emit and it does not accumulate
    p.sendNext(7)
    p.sendComplete()

    Await.result(result, 10.seconds) should be(Seq(Seq(1, 2), Seq(3, 4), Seq(5, 6, 7)))

  }

  "split aggregator by total duration" in {

    implicit val actorSystem = createActorSystem("2")

    val maxDuration = 400.seconds

    val p = TestPublisher.probe[Int]()

    val result = Source
      .fromPublisher(p)
      .aggregateWithTimeBoundary(allocate = ListBuffer.empty[Int])(
        aggregate = (buffer, i) => {
          buffer += i
          (buffer, false)
        },
        harvest = seq => seq.toSeq,
        maxGap = None,
        maxDuration = Some(maxDuration), // elements with longer gap will put put to next aggregator
        currentTimeMs = schedulerTimeMs,
        interval = 1.milli)
      .buffer(1, OverflowStrategy.backpressure)
      .runWith(Sink.collection)

    p.sendNext(1)
    timePasses(maxDuration / 4)
    p.sendNext(2)
    timePasses(maxDuration / 4)
    p.sendNext(3)
    timePasses(maxDuration / 4)
    p.sendNext(4)
    timePasses(maxDuration / 4) // maxDuration will accumulate and reach threshold here

    p.sendNext(5)
    p.sendNext(6)
    p.sendNext(7)
    p.sendComplete()

    Await.result(result, 10.seconds) should be(Seq(Seq(1, 2, 3, 4), Seq(5, 6, 7)))

  }

  "down stream back pressure should not miss data on completion with pull on start" in {

    implicit val actorSystem = createActorSystem("3")

    val maxGap = 1.second
    val upstream = TestPublisher.probe[Int]()
    val downstream = TestSubscriber.probe[Seq[Int]]()

    Source
      .fromPublisher(upstream)
      .aggregateWithTimeBoundary(allocate = ListBuffer.empty[Int])(
        aggregate = (buffer, i) => {
          buffer += i
          (buffer, false)
        },
        harvest = buffer => buffer.toSeq,
        maxGap = Some(maxGap),
        maxDuration = None,
        currentTimeMs = schedulerTimeMs,
        interval = 1.milli)
      .buffer(1, OverflowStrategy.backpressure)
      .to(Sink.fromSubscriber(downstream))
      .run()

    downstream.ensureSubscription()
    upstream.sendNext(1) // onPush(1) -> aggregator=Seq(1), due to the preStart pull, will pull upstream again since queue is empty
    timePasses(maxGap) // harvest onTimer, queue=Queue(Seq(1)), aggregator=null
    upstream.sendNext(2) // onPush(2) -> aggregator=Seq(2), due to the previous pull, even the queue is already full at this point due to timer, but it won't pull upstream again
    timePasses(maxGap) // harvest onTimer, queue=(Seq(1), Seq(2)), aggregator=null, note queue size can be 1 more than the threshold
    upstream.sendNext(3) // 3 will not be pushed to the stage until the stage pull upstream
    timePasses(maxGap) // since 3 stayed outside of the stage, this gap will not cause 3 to be emitted
    downstream.request(1).expectNext(Seq(1)) // onPull emit Seq(1), queue=(Seq(2))
    timePasses(maxGap) // since 3 stayed outside of the stage, this gap will not cause 3 to be emitted
    downstream
      .request(1)
      .expectNext(Seq(2)) // onPull emit Seq(2). queue is empty now, pull upstream and 3 will be pushed into the stage
    // onPush(3) -> aggregator=Seq(3) pull upstream since queue is empty
    downstream
      .request(1)
      .expectNoMessage() // onPull, no data to emit, won't pull upstream again since it's already pulled
    timePasses(maxGap) // emit Seq(3) onTimer
    downstream.expectNext(Seq(3))
    upstream.sendNext(4) // onPush(4) -> aggregator=Seq(4) will follow, and pull upstream again
    upstream.sendNext(5) // onPush(5) -> aggregator=Seq(4,5) will happen right after due to the previous pull from onPush(4), eagerly pull even out is not available
    upstream.sendNext(6) // onPush(6) -> aggregator=Seq(4,5,6) will happen right after due to the previous pull from onPush(5), even the queue is full at this point
    timePasses(maxGap) // harvest queue=(Seq(4,5,6))
    upstream.sendNext(7) // onPush(7), aggregator=Seq(7), queue=(Seq(4,5,6) no pulling upstream due to queue is full
    // if sending another message it will stay in upstream, prevent the upstream completion from happening
    upstream.sendComplete() // emit remaining queue=Queue(Seq(4,5,6)) + harvest and emit aggregator=Seq(7)
    // since there is no pending push from upstream, onUpstreamFinish will be triggered to emit the queue and pending aggregator
    downstream.request(2).expectNext(Seq(4, 5, 6), Seq(7)) // clean up the emit queue and complete downstream
    downstream.expectComplete()
  }
}
