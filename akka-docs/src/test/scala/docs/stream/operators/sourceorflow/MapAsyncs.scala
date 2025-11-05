/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.Timeout

import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object CommonMapAsync {
  sealed trait Event {
    def sequenceNumber: Int
  }

  object Event {
    def apply(sequenceNumber: Int): PlainEvent = PlainEvent(sequenceNumber)
  }

  case class PlainEvent(sequenceNumber: Int) extends Event
  case class EntityEvent(entityId: Int, sequenceNumber: Int) extends Event

  // dummy objects for our pretend Kafka
  val settings = new AnyRef
  val subscription = new AnyRef

  // for access from java
  def consumer = Consumer
  def committer = Committer

  object Consumer {
    // almost but not quite like Alpakka Kafka
    def committableSource(settings: AnyRef, subscription: AnyRef): Source[EntityEvent, NotUsed] =
      // pro forma to use the dummy arguments
      if (settings != null && subscription != null) {
        Source
          .fromIterator { () =>
            Iterator.from(1)
          }
          .statefulMap(() => immutable.Map.empty[Int, Int])(
            { (countsPerEntity, n) =>
              val entityId =
                if (Random.nextBoolean() || countsPerEntity.isEmpty) Random.nextInt(n)
                else {
                  val m = Random.nextInt(countsPerEntity.size) + 1
                  countsPerEntity.keys.iterator.take(m).drop(m - 1).next()
                }
              val seqNr = countsPerEntity.getOrElse(entityId, 0) + 1
              (countsPerEntity + (entityId -> seqNr), EntityEvent(entityId, seqNr))
            }, { _ =>
              None
            })
      } else throw new AssertionError("pro forma")

    // likewise ape the Alpakka Kafka API
    def plainSource(settings: AnyRef, subscription: AnyRef): Source[PlainEvent, NotUsed] =
      // pro forma use the dummy arguments
      if (settings != null && subscription != null) {
        Source
          .fromIterator { () =>
            Iterator.from(1)
          }
          .map(Event.apply _)
      } else throw new AssertionError("pro forma")
  }

  object Committer {
    def sink(prependTo: String): Sink[String, Future[Done]] =
      Flow[String].map(in => s"$prependTo emitted $in").toMat(Sink.foreach(println _))(Keep.right)
  }

  implicit val sys: ActorSystem = ActorSystem("mapAsync-stream")
  implicit val exCtx: ExecutionContextExecutor = sys.dispatcher
  implicit val timeout: Timeout = 3.seconds

  // #mapasync-strict-order
  // #mapasync-concurrent
  // #mapasyncunordered

  val events: Source[Event, NotUsed] = Consumer.plainSource(settings, subscription).throttle(1, 50.millis)

  def eventHandler(event: Event): Future[Int] = {
    println(s"Processing event $event...")
    // #mapasync-strict-order
    // #mapasync-concurrent
    // #mapasyncunordered
    val result =
      if (Random.nextInt(5) == 0) {
        akka.pattern.after(500.millis)(Future.successful(event.sequenceNumber))
      } else {
        Future.successful(event.sequenceNumber)
      }
    result.map { x =>
      println(s"Completed processing $x")
      x
    }
    // #mapasync-strict-order
    // #mapasync-concurrent
    // #mapasyncunordered
  }
  // #mapasync-strict-order
  // #mapasync-concurrent
  // #mapasyncunordered

}

object MapAsyncStrictOrder extends App {
  import CommonMapAsync._
  // #mapasync-strict-order

  events
    .mapAsync(1) { in =>
      eventHandler(in)
    }
    .map { in =>
      println(s"`mapAsync` emitted event number: $in")
    }
    // #mapasync-strict-order
    .runWith(Sink.ignore)

}

object MapAsync extends App {
  import CommonMapAsync._
  // #mapasync-concurrent

  events
    .mapAsync(3) { in =>
      eventHandler(in)
    }
    .map { in =>
      println(s"`mapAsync` emitted event number: $in")
    }
    // #mapasync-concurrent
    .runWith(Sink.ignore)

}

object MapAsyncUnordered extends App {
  import CommonMapAsync._
  // #mapasyncunordered

  events
    .mapAsyncUnordered(3) { in =>
      eventHandler(in)
    }
    .map { in =>
      println(s"`mapAsyncUnordered` emitted event number: $in")
    }
    // #mapasyncunordered
    .runWith(Sink.ignore)

}

object MapAsyncPartitioned extends App {
  import CommonMapAsync._

  // pretending this is an Alpakka Kafka-style CommitterSettings
  val commitSettings = "`mapAsyncPartitioned`"

  // #mapAsyncPartitioned
  val eventsForEntities: Source[EntityEvent, NotUsed] =
    Consumer.committableSource(settings, subscription).take(1000)

  val partitioner: EntityEvent => Int = { event =>
    val partition = event.entityId
    println(s"Assigned event $event to partition $partition")
    partition
  }

  eventsForEntities.zipWithIndex
    .map {
      case (event, count) =>
        println(s"Received event $event at offset $count from message broker")
        event
    }
    .mapAsyncPartitioned(parallelism = 10, perPartition = 1)(partitioner) { (event, partition) =>
      println(s"Processing event $event from partition $partition")

      // processing result is "partition-sequenceNr"
      // val processEvent: (EntityEvent, Int) => Future[String]
      processEvent(event, partition).map { x =>
        println(s"Completed processing $x")
        x
      }
    }
    // for the purpose of this example, will print every element, prepended with "`mapAsyncPartitioned` emitted "
    .runWith(Committer.sink(commitSettings))
    // #mapAsyncPartitioned
    .flatMap { _ =>
      // give the printlns a chance to print
      akka.pattern.after(1.second)(Future.unit)
    }
    .foreach { _ =>
      System.out.flush()
      sys.terminate()
    }

  def processEvent(event: EntityEvent, partition: Int): Future[String] = {
    val fut = Future.successful(s"$partition-${event.sequenceNumber}")
    if (Random.nextBoolean()) {
      akka.pattern.after(Random.nextInt(1000).millis)(fut)
    } else fut
  }
}
