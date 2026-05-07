/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

import scala.concurrent.duration._

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object Split {
  def splitWhenExample(): Unit = {
    import akka.actor.ActorSystem

    implicit val system: ActorSystem = ActorSystem()

    //#splitWhen
    Source(1 to 100)
      .throttle(1, 100.millis)
      .map(elem => (elem, Instant.now()))
      .statefulMapConcat(() => {
        // stateful decision in statefulMapConcat
        // keep track of time bucket (one per second)
        var currentTimeBucket = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC)

        {
          case (elem, timestamp) =>
            val time = LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC)
            val bucket = time.withNano(0)
            val newBucket = bucket != currentTimeBucket
            if (newBucket)
              currentTimeBucket = bucket
            List((elem, newBucket))
        }
      })
      .splitWhen(_._2) // split when time bucket changes
      .map(_._1)
      .fold(0)((acc, _) => acc + 1) // sum
      .to(Sink.foreach(println))
      .run()
    // 3
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 7
    //#splitWhen
  }

  def splitAfterExample(): Unit = {
    import akka.actor.ActorSystem

    implicit val system: ActorSystem = ActorSystem()

    //#splitAfter
    Source(1 to 100)
      .throttle(1, 100.millis)
      .map(elem => (elem, Instant.now()))
      .sliding(2)
      .splitAfter { slidingElements =>
        if (slidingElements.size == 2) {
          val current = slidingElements.head
          val next = slidingElements.tail.head
          val currentBucket = LocalDateTime.ofInstant(current._2, ZoneOffset.UTC).withNano(0)
          val nextBucket = LocalDateTime.ofInstant(next._2, ZoneOffset.UTC).withNano(0)
          currentBucket != nextBucket
        } else {
          false
        }
      }
      .map(_.head._1)
      .fold(0)((acc, _) => acc + 1) // sum
      .to(Sink.foreach(println))
      .run()
    // 3
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 6
    // note that the very last element is never included due to sliding,
    // but that would not be problem for an infinite stream
    //#splitAfter
  }

}
