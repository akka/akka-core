/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.DelayStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import docs.stream.operators.sourceorflow.ExtrapolateAndExpand.fps
import docs.stream.operators.sourceorflow.ExtrapolateAndExpand.nowInSeconds
import docs.stream.operators.sourceorflow.ExtrapolateAndExpand.periodInMillis
import docs.stream.operators.sourceorflow.ExtrapolateAndExpand.videoAt25Fps

import scala.concurrent.duration._
import scala.util.Random

/**
 *
 */
object ExtrapolateAndExpandMain extends App {
  implicit val sys: ActorSystem = ActorSystem("25fps-stream")
  videoAt25Fps.map(_.pixels.utf8String).map(frame => s"$nowInSeconds - $frame").to(Sink.foreach(println)).run()

}
object ExtrapolateAndExpand {

  val periodInMillis = 40
  val fps = 1000 / periodInMillis

  import ExtrapolateAndExpandCommon._

  val decode: Flow[ByteString, Frame, NotUsed] =
    Flow[ByteString].map(Frame.decode)

  // #extrapolate
  // if upstream is too slow, produce copies of the last frame but grayed out.
  val rateControl: Flow[Frame, Frame, NotUsed] =
    Flow[Frame].extrapolate((frame: Frame) => {
      val grayedOut = frame.withFilter(Gray)
      Iterator.continually(grayedOut)
    }, Some(Frame.blackFrame))

  val videoSource: Source[Frame, NotUsed] = networkSource.via(decode).via(rateControl)

  // let's create a 25fps stream (a Frame every 40.millis)
  val tickSource: Source[Tick.type, Cancellable] = Source.tick(0.seconds, 40.millis, Tick)

  val videoAt25Fps: Source[Frame, Cancellable] =
    tickSource.zip(videoSource).map(_._2)
  // #extrapolate

  // #expand
  // each element flowing through the stream is expanded to a watermark copy
  // of the upstream frame and grayed out copies. The grayed out copies should
  // only be used downstream if the producer is too slow.
  val watermarkerRateControl: Flow[Frame, Frame, NotUsed] =
    Flow[Frame].expand((frame: Frame) => {
      val watermarked = frame.withFilter(Watermark)
      val grayedOut = frame.withFilter(Gray)
      (Iterator.single(watermarked) ++ Iterator.continually(grayedOut))
    })

  val watermarkedVideoSource: Source[Frame, NotUsed] =
    networkSource.via(decode).via(rateControl)

  // let's create a 25fps stream (a Frame every 40.millis)
  val ticks: Source[Tick.type, Cancellable] = Source.tick(0.seconds, 40.millis, Tick)

  val watermarkedVideoAt25Fps: Source[Frame, Cancellable] =
    ticks.zip(watermarkedVideoSource).map(_._2)

  // #expand

  def nowInSeconds = System.nanoTime() / 1000000000
}

object ExtrapolateAndExpandCommon {
  // This `networkSource` simulates a client sending frames over the network. There's a
  // stage throttling the elements at 24fps and then a `delayWith` that randomly delays
  // frames simulating network latency and bandwidth limitations (uses buffer of
  // default capacity).
  val networkSource: Source[ByteString, NotUsed] =
    Source
      .fromIterator(() => Iterator.from(0)) // produce frameIds
      .throttle(fps, 1.second)
      .map(i => ByteString.fromString(s"fakeFrame-$i"))
      .delayWith(
        () =>
          new DelayStrategy[ByteString] {
            override def nextDelay(elem: ByteString): FiniteDuration =
              Random.nextInt(periodInMillis * 10).millis
          },
        DelayOverflowStrategy.dropBuffer)

  case object Tick

  sealed trait Filter {
    def filter(fr: Frame): Frame
  }
  object Gray extends Filter {
    override def filter(fr: Frame): Frame =
      Frame(ByteString.fromString(s"gray frame!! - ${fr.pixels.utf8String}"))
  }
  object Watermark extends Filter {
    override def filter(fr: Frame): Frame =
      Frame(fr.pixels.++(ByteString.fromString(" - watermark")))
  }

  case class Frame(pixels: ByteString) {
    def withFilter(f: Filter): Frame = f.filter(this)
  }
  object Frame {
    val blackFrame: Frame = Frame(ByteString.empty)
    def decode(bs: ByteString): Frame = Frame(bs)
  }
}
