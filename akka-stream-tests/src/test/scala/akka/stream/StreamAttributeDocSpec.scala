/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.concurrent.Future

import akka.Done
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.TcpAttributes
import akka.stream.testkit.StreamSpec

class StreamAttributeDocSpec extends StreamSpec("my-stream-dispatcher = \"akka.test.stream-dispatcher\"") {

  "Setting attributes on the runnable stream" must {

    "be shown" in {
      // no stdout from tests thank you
      val println = (_: Any) => ()

      val done = {
        // #attributes-on-stream
        val stream: RunnableGraph[Future[Done]] =
          Source(1 to 10)
            .map(_.toString)
            .toMat(Sink.foreach(println))(Keep.right)
            .withAttributes(Attributes.inputBuffer(4, 4) and
            ActorAttributes.dispatcher("my-stream-dispatcher") and
            TcpAttributes.tcpWriteBufferSize(2048))

        stream.run()
        // #attributes-on-stream
      }
      done.futureValue // block until stream is done

    }

  }

}
