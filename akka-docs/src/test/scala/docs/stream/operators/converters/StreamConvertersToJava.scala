/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.converters

// #import
import java.util.stream
import java.util.stream.IntStream

import akka.NotUsed
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
// #import
import akka.testkit.AkkaSpec
import org.scalatest.concurrent.Futures

import scala.collection.immutable
import scala.concurrent.Future

class StreamConvertersToJava extends AkkaSpec with Futures {

  "demonstrate materialization to Java8 streams" in {
    //#asJavaStream
    val source: Source[Int, NotUsed] = Source(0 to 9).filter(_ % 2 == 0)

    val sink: Sink[Int, stream.Stream[Int]] = StreamConverters.asJavaStream[Int]()

    val jStream: java.util.stream.Stream[Int] = source.runWith(sink)
    //#asJavaStream
    jStream.count should be(5)
  }

  "demonstrate conversion from Java8 streams" in {
    //#fromJavaStream
    def factory(): IntStream = IntStream.rangeClosed(0, 9)
    val source: Source[Int, NotUsed] = StreamConverters.fromJavaStream(() => factory()).map(_.intValue())
    val sink: Sink[Int, Future[immutable.Seq[Int]]] = Sink.seq[Int]

    val futureInts: Future[immutable.Seq[Int]] = source.toMat(sink)(Keep.right).run()

    //#fromJavaStream
    whenReady(futureInts) { ints =>
      ints should be((0 to 9).toSeq)
    }

  }

}
