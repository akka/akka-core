/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import org.scalatest.matchers.{ MatchResult, Matcher }

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.testkit.{ StreamSpec, TestPublisher, TestSubscriber }
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }

class RetryFlowSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 1
    akka.stream.materializer.max-input-buffer-size = 1
  """) with CustomMatchers {

  final val Failed = new Exception("prepared failure")
  final val FailedElem: Try[Int] = Failure(Failed)

  val failEvenValuesFlow: FlowWithContext[Int, Int, Try[Int], Int, NotUsed] =
    FlowWithContext.fromTuples(Flow.fromFunction {
      case (i, ctx) if i % 2 == 0 => (FailedElem, ctx)
      case (i, ctx)               => (Success(i), ctx)
    })

  val failAllValuesFlow: FlowWithContext[Int, Int, Try[Int], Int, NotUsed] =
    FlowWithContext.fromTuples(Flow.fromFunction {
      case (_, j) => (FailedElem, j)
    })

  val alwaysRecoveringFunc: ((Int, Int), (Try[Int], Int)) => Option[(Int, Int)] = {
    case (_, (Failure(_), i)) => Some(i -> i)
    case _                    => None
  }

  /** increments the value and the context with every failure */
  def incrementFailedValues[OutData](in: (Int, Int), out: (Try[OutData], Int)): Option[(Int, Int)] = {
    (in, out) match {
      case ((in, _), (Failure(_), outCtx)) => Some((in + 1, outCtx + 1))
      case _                               => None
    }
  }

  "RetryFlow.withBackoff" should {

    val failEvenValuesFlow: Flow[Int, Try[Int], NotUsed] =
      Flow.fromFunction {
        case i if i % 2 == 0 => FailedElem
        case i               => Success(i)
      }

    def incrementFailedValues[OutData](in: Int, out: Try[OutData]): Option[Int] = {
      (in, out) match {
        case (in, Failure(_)) => Some(in + 1)
        case _                => None
      }
    }

    "send elements through" in {
      val sink =
        Source(List(13, 17, 19, 23, 27))
          .via(
            RetryFlow.withBackoff(10.millis, 5.second, 0d, maxRetries = 3, failEvenValuesFlow)(incrementFailedValues))
          .runWith(Sink.seq)
      sink.futureValue should contain inOrderElementsOf List(
        Success(13),
        Success(17),
        Success(19),
        Success(23),
        Success(27))
    }

    "allow retrying a successful element" in {
      // #withBackoff-demo
      val flow: Flow[Int, Int, NotUsed] = // ???
        // #withBackoff-demo
        Flow.fromFunction(i => i / 2)

      // #withBackoff-demo

      val retryFlow: Flow[Int, Int, NotUsed] =
        RetryFlow.withBackoff(minBackoff = 10.millis, maxBackoff = 5.seconds, randomFactor = 0d, maxRetries = 3, flow)(
          decideRetry = {
            case (_, result) if result > 0 => Some(result)
            case _                         => None
          })
      // #withBackoff-demo

      val (source, sink) = TestSource[Int]().via(retryFlow).toMat(TestSink())(Keep.both).run()

      sink.request(4)

      source.sendNext(5)
      sink.expectNext(0)

      source.sendNext(2)
      sink.expectNext(0)

      source.sendComplete()
      sink.expectComplete()
    }

    "retry up to maxRetries times and then return failed element" in {

      // The flow will ignore input and always return -1
      val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(_ => -1)

      // The retry flow will retry on negative input values
      val retryFlow: Flow[Int, Int, NotUsed] =
        RetryFlow.withBackoff(minBackoff = 10.millis, maxBackoff = 100.millis, randomFactor = 0d, maxRetries = 5, flow)(
          decideRetry = {
            case (x, result) if result < 0 => Some(x)
            case (_, _)                    => None
          })

      val (source, sink) = TestSource[Int]().via(retryFlow).toMat(TestSink())(Keep.both).run()

      sink.request(5)

      source.sendNext(1)

      // The sink will get -1 after the retryFlow has retried maxRetries number of times
      sink.expectNext(-1)

      source.sendComplete()
      sink.expectComplete()
    }
  }

  "RetryFlow.withBackoffAndContext" should {

    "send elements through" in {
      val sink =
        Source(List(13, 17, 19, 23, 27).map(_ -> 0))
          .via(RetryFlow.withBackoffAndContext(10.millis, 5.second, 0d, maxRetries = 3, failEvenValuesFlow)(
            incrementFailedValues))
          .runWith(Sink.seq)
      sink.futureValue should contain inOrderElementsOf List(
        Success(13) -> 0,
        Success(17) -> 0,
        Success(19) -> 0,
        Success(23) -> 0,
        Success(27) -> 0)
    }

    "send failures through (after retrying)" in {
      val maxRetries = 2
      val sink =
        Source(List(13, 17).map(_ -> 0))
          .via(RetryFlow.withBackoffAndContext(1.millis, 5.millis, 0d, maxRetries, failAllValuesFlow)(
            incrementFailedValues))
          .runWith(Sink.seq)
      sink.futureValue should contain inOrderElementsOf List(FailedElem -> maxRetries, FailedElem -> maxRetries)
    }

    "send elements through (with retrying)" in {
      val sink =
        Source(List(12, 13, 14).map(_ -> 0))
          .via(RetryFlow.withBackoffAndContext(1.millis, 5.millis, 0d, maxRetries = 3, failEvenValuesFlow)(
            incrementFailedValues))
          .runWith(Sink.seq)
      sink.futureValue should contain inOrderElementsOf List(Success(13) -> 1, Success(13) -> 0, Success(15) -> 1)
    }

    "allow retrying a successful element" in {
      class SomeContext

      //#retry-success
      val flow: FlowWithContext[Int, SomeContext, Int, SomeContext, NotUsed] = // ???
        //#retry-success
        FlowWithContext.fromTuples[Int, SomeContext, Int, SomeContext, NotUsed](Flow.fromFunction {
          case (i, ctx) => i / 2 -> ctx
        })

      //#retry-success

      val retryFlow: FlowWithContext[Int, SomeContext, Int, SomeContext, NotUsed] =
        RetryFlow.withBackoffAndContext(
          minBackoff = 10.millis,
          maxBackoff = 5.seconds,
          randomFactor = 0d,
          maxRetries = 3,
          flow)(decideRetry = {
          case ((_, _), (result, ctx)) if result > 0 => Some(result -> ctx)
          case _                                     => None
        })
      //#retry-success

      val (source, sink) = TestSource[(Int, SomeContext)]().via(retryFlow).toMat(TestSink())(Keep.both).run()

      sink.request(4)

      val ctx = new SomeContext
      source.sendNext(5 -> ctx)
      sink.expectNext(0 -> ctx)

      source.sendNext(2 -> ctx)
      sink.expectNext(0 -> ctx)

      source.sendComplete()
      sink.expectComplete()
    }

    "work with a buffer in the inner flow" in {
      val flow: FlowWithContext[Int, Int, Try[Int], Int, NotUsed] =
        FlowWithContext.fromTuples(Flow[(Int, Int)].buffer(10, OverflowStrategy.backpressure).via(failEvenValuesFlow))
      val (source, sink) = TestSource[(Int, Int)]()
        .via(RetryFlow.withBackoffAndContext(10.millis, 5.seconds, 0d, 3, flow)((_, _) => None))
        .toMat(TestSink())(Keep.both)
        .run()

      sink.request(99)

      source.sendNext(1 -> 0)
      source.sendNext(3 -> 0)
      sink.expectNext(Success(1) -> 0)
      sink.expectNext(Success(3) -> 0)
    }

  }

  "Backing off" should {
    "have min backoff" in {
      val minBackoff = 200.millis
      val (source, sink) = TestSource[(Int, Int)]()
        .via(RetryFlow.withBackoffAndContext(minBackoff, 5.second, 0d, 3, failEvenValuesFlow)(incrementFailedValues))
        .toMat(TestSink())(Keep.both)
        .run()

      sink.request(99)

      source.sendNext(1 -> 0)
      sink.expectNext(80.millis, Success(1) -> 0)

      source.sendNext(2 -> 0)
      sink.expectNoMessage(minBackoff)
      sink.expectNext(Success(3) -> 1)
    }

    "use min backoff for every try" in {
      val minBackoff = 50.millis
      val maxRetries = 3
      val (source, sink) = TestSource[(Int, Int)]()
        .via(RetryFlow.withBackoffAndContext(minBackoff, 5.seconds, 0d, maxRetries, failAllValuesFlow)(
          incrementFailedValues))
        .toMat(TestSink())(Keep.both)
        .run()

      sink.request(1)

      source.sendNext(10 -> 0)
      sink.expectNoMessage(minBackoff * maxRetries)
      sink.expectNext(FailedElem -> maxRetries)
    }

    "exponentially backoff between retries" in {
      val NumRetries = 7

      val nanoTimeOffset = System.nanoTime()
      case class State(retriedAt: List[Long])

      val flow = FlowWithContext.fromTuples[State, NotUsed, State, NotUsed, NotUsed](Flow.fromFunction {
        case (State(retriedAt), _) => State(System.nanoTime - nanoTimeOffset :: retriedAt) -> NotUsed
      })

      val (source, sink) = TestSource[(State, NotUsed)]()
        .via(RetryFlow.withBackoffAndContext(10.millis, 5.seconds, 0d, NumRetries, flow) {
          case (_, (s, _)) => Some(s -> NotUsed)
        })
        .toMat(TestSink())(Keep.both)
        .run()

      sink.request(1)

      source.sendNext(State(Nil) -> NotUsed)
      val (State(retriedAt), _) = sink.expectNext()

      val timesBetweenRetries = retriedAt
        .sliding(2)
        .collect {
          case before :: foundAfter :: Nil => before - foundAfter
        }
        .toIndexedSeq

      timesBetweenRetries.reverse should strictlyIncrease

      source.sendComplete()
      sink.expectComplete()
    }

  }

  "Aborting" should {
    "propagate error from upstream" in {
      val retryFlow: FlowWithContext[Int, Int, Try[Int], Int, NotUsed] =
        RetryFlow.withBackoffAndContext(
          minBackoff = 10.millis,
          maxBackoff = 5.seconds,
          randomFactor = 0d,
          maxRetries = 3,
          flow = failEvenValuesFlow)(decideRetry = {
          case ((in, _), (Failure(_), ctx)) => Some((in + 1, ctx))
          case _                            => None
        })

      val (source, sink) = TestSource[(Int, Int)]().via(retryFlow).toMat(TestSink())(Keep.both).run()

      sink.request(99)

      source.sendNext(1 -> 1)
      sink.expectNext(Success(1) -> 1)

      source.sendNext(2 -> 2)
      sink.expectNext(Success(3) -> 2)

      source.sendError(Failed)
      sink.expectError(Failed)
    }

    "propagate error from upstream on start" in new AllSucceedBench[Int, Int, Int] {
      externalOut.request(99)
      externalIn.sendError(Failed)
      externalOut.expectError(Failed)
    }

    "propagate error from upstream before start" in new AllSucceedBench[Int, Int, Int] {
      externalIn.sendError(Failed)
      externalOut.request(1)
      externalOut.expectError(Failed)
    }

    "propagate error on the inner flow after start" in new AllSucceedBench[Int, Int, Int] {
      externalOut.request(99)

      // send one element through
      externalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)
      internalIn.sendNext(1 -> 0)
      externalOut.expectNext(1 -> 0)

      // fail inner flow
      internalIn.sendError(Failed)
      externalOut.expectError(Failed)
      externalIn.expectCancellation()
    }

    "propagate error on the inner flow on start" in new AllSucceedBench[Int, Int, Int] {
      externalOut.request(29)
      internalIn.sendError(Failed)
      externalOut.expectError(Failed)
      externalIn.expectCancellation()
    }

    "propagate non-error cancel on the inner flow on start" in new AllSucceedBench[Int, Int, Int] {
      externalOut.request(29)
      internalOut.cancel()
      externalOut.expectComplete()
      externalIn.expectCancellation()
    }

    "propagate error on the inner flow before start" in new AllSucceedBench[Int, Int, Int] {
      internalIn.sendError(Failed)
      externalOut.request(13)
      externalOut.expectError(Failed)
      externalIn.expectCancellation()
    }

    "propagate error before the RetryFlow, while on retry spin" in new ConstructBench[Int, Int, Int]((v, _) => Some(v)) {
      externalOut.request(92)
      // spinning message
      externalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)
      internalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)

      externalOut.expectNoMessage()
      externalIn.sendError(Failed)
      externalOut.expectError(Failed)
    }

    "propagate error on the inner flow, while on retry spin" in new ConstructBench[Int, Int, Int]((v, _) => Some(v)) {
      externalOut.request(35)
      // spinning message
      externalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)
      internalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)

      externalOut.expectNoMessage()
      internalIn.sendError(Failed)
      externalOut.expectError(Failed)
      externalIn.expectCancellation()
    }

    "allow for downstream cancel while element is in flow" in new ConstructBench[Int, Int, Int]((v, _) => Some(v)) {
      externalOut.request(78)
      // spinning message
      externalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)

      externalOut.cancel()

      internalIn.expectCancellation()
      externalIn.expectCancellation()
    }

    "allow for downstream cancel while retry is backing off" in new ConstructBench[Int, Int, Int]((v, _) => Some(v)) {
      externalOut.request(35)
      // spinning message
      externalIn.sendNext(1 -> 0)
      internalOut.requestNext(1 -> 0)
      internalIn.sendNext(1 -> 0)

      externalOut.cancel()

      internalIn.expectCancellation()
      externalIn.expectCancellation()
    }

    "finish only after processing all elements in stream" in new AllSucceedBench[Int, Int, Int] {
      externalOut.request(32)

      // send one element and complete
      externalIn.sendNext(1 -> 0)
      externalIn.sendComplete()

      internalOut.requestNext(1 -> 0)
      internalIn.sendNext(1 -> 0)
      externalOut.requestNext(1 -> 0)

      externalOut.expectComplete()
    }
  }

  "Coordinator" should {

    type InData = String
    type Ctx2 = Int
    type OutData = Try[String]

    "send elements across" in new AllSucceedBench[InData, Ctx2, OutData] {
      // push element
      val elA = "A" -> 123
      externalIn.sendNext(elA)

      // let element go via retryable flow
      internalOut.requestNext(elA)
      private val resA = Success("result A") -> 123
      internalIn.sendNext(resA)

      // expect result
      externalOut.requestNext(resA)
    }

    "retry for a failure" in new ConstructBench[InData, Ctx2, OutData]((_, out) =>
      out._1 match {
        case Success(_) => None
        case Failure(_) => Some("A'" -> 2)
      }) {
      // demand on stream end
      externalOut.request(1)

      // push element
      val element1 = "A" -> 1
      externalIn.sendNext(element1)

      // let element go via retryable flow
      internalOut.requestNext(element1)
      internalIn.sendNext(Failure(new RuntimeException("boom")) -> 1)

      // let element go via retryable flow
      val try2 = internalOut.requestNext(3.seconds)
      try2._1 shouldBe "A'"
      try2._2 shouldBe 2
      val res1 = Success("Ares") -> 1
      internalIn.sendNext(res1)

      // expect result
      externalOut.requestNext(res1)
    }

    "fail if inner flow emits twice" in new AllSucceedBench[InData, Ctx2, OutData] {
      externalOut.request(99)

      // push element
      val elA = "A" -> 123
      externalIn.sendNext(elA)

      // let element go via retryable flow
      internalOut.requestNext(elA)
      val resA = Success("result A") -> 123
      internalIn.sendNext(resA)
      internalIn.sendNext(Success("result B") -> 222)

      // expect result
      externalOut.requestNext(resA)
      externalOut.expectError() shouldBe an[IllegalStateException]
    }

    "allow more demand in inner flow (but never pass in more than one element into the retrying cycle)" in new AllSucceedBench[
      InData,
      Ctx2,
      OutData] {
      externalOut.request(1)
      internalIn.expectRequest() shouldBe 1L
      internalOut.request(1)
      externalIn.expectRequest() shouldBe 1L

      // push element
      val elA = "A" -> 123
      externalIn.sendNext(elA)

      // let element go via retryable flow
      internalOut.expectNext(elA)

      // more demand on retryable flow
      internalOut.request(1)
      internalOut.expectNoMessage(50.millis)

      // emit from retryable flow, push to external
      val resA = Success("result A") -> 123
      internalIn.sendNext(resA)

      // element should NOT reach internal before pushed from externalOut
      internalOut.expectNoMessage(50.millis)

      // put a new element in external in buffer
      val elB = "B" -> 567
      externalIn.sendNext(elB)
      // element reaches internal flow before elA has bean fetched from externalOut
      internalOut.expectNext(elB)

      externalOut.expectNext(resA)
    }
  }

  class ConstructBench[In, Ctx, Out](retryWith: ((In, Ctx), (Out, Ctx)) => Option[(In, Ctx)]) {

    val throughDangerousFlow
        : FlowWithContext[In, Ctx, Out, Ctx, (TestSubscriber.Probe[(In, Ctx)], TestPublisher.Probe[(Out, Ctx)])] =
      FlowWithContext.fromTuples(Flow.fromSinkAndSourceMat(TestSink[(In, Ctx)](), TestSource[(Out, Ctx)]())(Keep.both))

    val ((externalIn, (internalOut, internalIn)), externalOut) =
      TestSource[(In, Ctx)]()
        .viaMat(
          RetryFlow.withBackoffAndContext(
            minBackoff = 10.millis,
            maxBackoff = 1.second,
            randomFactor = 0d,
            maxRetries = 3,
            throughDangerousFlow)(retryWith))(Keep.both)
        .toMat(TestSink[(Out, Ctx)]())(Keep.both)
        .run()
  }

  class AllSucceedBench[In, Ctx, Out] extends ConstructBench[In, Ctx, Out]((_, _) => None)

}

trait CustomMatchers {

  class StrictlyIncreasesMatcher() extends Matcher[Seq[Long]] {

    def apply(left: Seq[Long]): MatchResult = {
      val result = left.sliding(2).map(pair => pair.head < pair.last).reduceOption(_ && _).getOrElse(false)

      MatchResult(
        result,
        s"""Collection $left elements are not increasing strictly""",
        s"""Collection $left elements are increasing strictly""")
    }
  }

  def strictlyIncrease = new StrictlyIncreasesMatcher()
}
