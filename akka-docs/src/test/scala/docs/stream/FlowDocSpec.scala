/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream

import akka.Done
import akka.NotUsed
import akka.actor.{ Actor, Cancellable }
import akka.stream.CompletionStrategy
import akka.stream.Materializer
import akka.stream.{ ClosedShape, FlowShape, OverflowStrategy }
import akka.stream.scaladsl._
import akka.testkit.AkkaSpec
import docs.CompileOnlySpec

import scala.annotation.nowarn
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success }
import scala.concurrent.ExecutionContext

@nowarn("msg=never used") // sample snippets
class FlowDocSpec extends AkkaSpec with CompileOnlySpec {

  implicit val ec: ExecutionContext = system.dispatcher

  "source is immutable" in {
    //#source-immutable
    val source = Source(1 to 10)
    source.map(_ => 0) // has no effect on source, since it's immutable
    source.runWith(Sink.fold(0)(_ + _)) // 55

    val zeroes = source.map(_ => 0) // returns new Source[Int], with `map()` appended
    zeroes.runWith(Sink.fold(0)(_ + _)) // 0
    //#source-immutable
  }

  "materialization in steps" in {
    //#materialization-in-steps
    val source = Source(1 to 10)
    val sink = Sink.fold[Int, Int](0)(_ + _)

    // connect the Source to the Sink, obtaining a RunnableGraph
    val runnable: RunnableGraph[Future[Int]] = source.toMat(sink)(Keep.right)

    // materialize the flow and get the value of the sink
    val sum: Future[Int] = runnable.run()

    //#materialization-in-steps
  }

  "materialization runWith" in {
    //#materialization-runWith
    val source = Source(1 to 10)
    val sink = Sink.fold[Int, Int](0)(_ + _)

    // materialize the flow, getting the Sink's materialized value
    val sum: Future[Int] = source.runWith(sink)
    //#materialization-runWith
  }

  "materialization is unique" in {
    //#stream-reuse
    // connect the Source to the Sink, obtaining a RunnableGraph
    val sink = Sink.fold[Int, Int](0)(_ + _)
    val runnable: RunnableGraph[Future[Int]] =
      Source(1 to 10).toMat(sink)(Keep.right)

    // get the materialized value of the sink
    val sum1: Future[Int] = runnable.run()
    val sum2: Future[Int] = runnable.run()

    // sum1 and sum2 are different Futures!
    //#stream-reuse
  }

  "compound source cannot be used as key" in {
    import scala.concurrent.duration._
    case object Tick

    val timer = Source.tick(initialDelay = 1.second, interval = 1.seconds, tick = () => Tick)

    val timerCancel: Cancellable = Sink.ignore.runWith(timer)
    timerCancel.cancel()

    val timerMap = timer.map(tick => "tick")
    // materialize the flow and retrieve the timers Cancellable
    val timerCancellable = Sink.ignore.runWith(timerMap)
    timerCancellable.cancel()

    val timerCancellable2 = timerMap.to(Sink.ignore).run()
    timerCancellable2.cancel()
  }

  "creating sources, sinks" in {
    //#source-sink
    // Create a source from an Iterable
    Source(List(1, 2, 3))

    // Create a source from a Future
    Source.future(Future.successful("Hello Streams!"))

    // Create a source from a single element
    Source.single("only one element")

    // an empty source
    Source.empty

    // Sink that folds over the stream and returns a Future
    // of the final result as its materialized value
    Sink.fold[Int, Int](0)(_ + _)

    // Sink that returns a Future as its materialized value,
    // containing the first element of the stream
    Sink.head

    // A Sink that consumes a stream without doing anything with the elements
    Sink.ignore

    // A Sink that executes a side-effecting call for every element of the stream
    Sink.foreach[String](println(_))
    //#source-sink
  }

  "various ways of connecting source, sink, flow" in {
    //#flow-connecting
    // Explicitly creating and wiring up a Source, Sink and Flow
    Source(1 to 6).via(Flow[Int].map(_ * 2)).to(Sink.foreach(println(_)))

    // Starting from a Source
    val source = Source(1 to 6).map(_ * 2)
    source.to(Sink.foreach(println(_)))

    // Starting from a Sink
    val sink: Sink[Int, NotUsed] = Flow[Int].map(_ * 2).to(Sink.foreach(println(_)))
    Source(1 to 6).to(sink)

    // Broadcast to a sink inline
    val otherSink: Sink[Int, NotUsed] =
      Flow[Int].alsoTo(Sink.foreach(println(_))).to(Sink.ignore)
    Source(1 to 6).to(otherSink)

    //#flow-connecting
  }

  "various ways of transforming materialized values" in {
    import scala.concurrent.duration._

    val throttler = Flow.fromGraph(GraphDSL.createGraph(Source.tick(1.second, 1.second, "test")) {
      implicit builder => tickSource =>
        import GraphDSL.Implicits._
        val zip = builder.add(ZipWith[String, Int, Int](Keep.right))
        tickSource ~> zip.in0
        FlowShape(zip.in1, zip.out)
    })

    //#flow-mat-combine
    // A source that can be signalled explicitly from the outside
    val source: Source[Int, Promise[Option[Int]]] = Source.maybe[Int]

    // A flow that internally throttles elements to 1/second, and returns a Cancellable
    // which can be used to shut down the stream
    val flow: Flow[Int, Int, Cancellable] = throttler

    // A sink that returns the first element of a stream in the returned Future
    val sink: Sink[Int, Future[Int]] = Sink.head[Int]

    // By default, the materialized value of the leftmost stage is preserved
    val r1: RunnableGraph[Promise[Option[Int]]] = source.via(flow).to(sink)

    // Simple selection of materialized values by using Keep.right
    val r2: RunnableGraph[Cancellable] = source.viaMat(flow)(Keep.right).to(sink)
    val r3: RunnableGraph[Future[Int]] = source.via(flow).toMat(sink)(Keep.right)

    // Using runWith will always give the materialized values of the stages added
    // by runWith() itself
    val r4: Future[Int] = source.via(flow).runWith(sink)
    val r5: Promise[Option[Int]] = flow.to(sink).runWith(source)
    val r6: (Promise[Option[Int]], Future[Int]) = flow.runWith(source, sink)

    // Using more complex combinations
    val r7: RunnableGraph[(Promise[Option[Int]], Cancellable)] =
      source.viaMat(flow)(Keep.both).to(sink)

    val r8: RunnableGraph[(Promise[Option[Int]], Future[Int])] =
      source.via(flow).toMat(sink)(Keep.both)

    val r9: RunnableGraph[((Promise[Option[Int]], Cancellable), Future[Int])] =
      source.viaMat(flow)(Keep.both).toMat(sink)(Keep.both)

    val r10: RunnableGraph[(Cancellable, Future[Int])] =
      source.viaMat(flow)(Keep.right).toMat(sink)(Keep.both)

    // It is also possible to map over the materialized values. In r9 we had a
    // doubly nested pair, but we want to flatten it out
    val r11: RunnableGraph[(Promise[Option[Int]], Cancellable, Future[Int])] =
      r9.mapMaterializedValue {
        case ((promise, cancellable), future) =>
          (promise, cancellable, future)
      }

    // Now we can use pattern matching to get the resulting materialized values
    val (promise, cancellable, future) = r11.run()

    // Type inference works as expected
    promise.success(None)
    cancellable.cancel()
    future.map(_ + 3)

    // The result of r11 can be also achieved by using the Graph API
    val r12: RunnableGraph[(Promise[Option[Int]], Cancellable, Future[Int])] =
      RunnableGraph.fromGraph(GraphDSL.createGraph(source, flow, sink)((_, _, _)) { implicit builder => (src, f, dst) =>
        import GraphDSL.Implicits._
        src ~> f ~> dst
        ClosedShape
      })

    //#flow-mat-combine
  }

  "defining asynchronous boundaries" in {
    //#flow-async
    Source(List(1, 2, 3)).map(_ + 1).async.map(_ * 2).to(Sink.ignore)
    //#flow-async
  }

  "source pre-materialization" in {
    //#source-prematerialization
    val completeWithDone: PartialFunction[Any, CompletionStrategy] = { case Done => CompletionStrategy.immediately }
    val matValuePoweredSource =
      Source.actorRef[String](
        completionMatcher = completeWithDone,
        failureMatcher = PartialFunction.empty,
        bufferSize = 100,
        overflowStrategy = OverflowStrategy.fail)

    val (actorRef, source) = matValuePoweredSource.preMaterialize()

    actorRef ! "Hello!"

    // pass source around for materialization
    source.runWith(Sink.foreach(println))
    //#source-prematerialization
  }
}

object FlowDocSpec {

  //#materializer-from-actor-context
  final class RunWithMyself extends Actor {
    implicit val mat: Materializer = Materializer(context)

    Source.maybe.runWith(Sink.onComplete {
      case Success(done) => println(s"Completed: $done")
      case Failure(ex)   => println(s"Failed: ${ex.getMessage}")
    })

    def receive = {
      case "boom" =>
        context.stop(self) // will also terminate the stream
    }
  }
  //#materializer-from-actor-context

  //#materializer-from-system-in-actor
  final class RunForever(implicit val mat: Materializer) extends Actor {

    Source.maybe.runWith(Sink.onComplete {
      case Success(done) => println(s"Completed: $done")
      case Failure(ex)   => println(s"Failed: ${ex.getMessage}")
    })

    def receive = {
      case "boom" =>
        context.stop(self) // will NOT terminate the stream (it's bound to the system!)
    }
  }
  //#materializer-from-system-in-actor

}
