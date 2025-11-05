/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import scala.concurrent.duration._

/**
 *
 */
object Throttle extends App {

  implicit val sys: ActorSystem = ActorSystem("25fps-stream")

  val frameSource: Source[Int, NotUsed] =
    Source.fromIterator(() => Iterator.from(0))

  // #throttle
  val framesPerSecond = 24

  // val frameSource: Source[Frame,_]
  val videoThrottling = frameSource.throttle(framesPerSecond, 1.second)
  // serialize `Frame` and send over the network.
  // #throttle

  // #throttle-with-burst
  // val frameSource: Source[Frame,_]
  val videoThrottlingWithBurst = frameSource.throttle(
    framesPerSecond,
    1.second,
    framesPerSecond * 30, // maximumBurst
    ThrottleMode.Shaping)
  // serialize `Frame` and send over the network.
  // #throttle-with-burst

  videoThrottling.take(1000).to(Sink.foreach(println)).run()
  videoThrottlingWithBurst.take(1000).to(Sink.foreach(println)).run()
}

object ThrottleCommon {

  // used in ThrottleJava
  case class Frame(i: Int)

}
