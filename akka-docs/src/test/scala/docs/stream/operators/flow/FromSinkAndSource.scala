/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.flow

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergeHub
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Tcp
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.util.ByteString

import scala.concurrent.duration._

object FromSinkAndSource {

  implicit val system: ActorSystem = ???

  def halfClosedTcpServer(): Unit = {
    // #halfClosedTcpServer
    // close in immediately
    val sink = Sink.cancelled[ByteString]
    // periodic tick out
    val source =
      Source.tick(1.second, 1.second, "tick").map(_ => ByteString(System.currentTimeMillis().toString + "\n"))

    val serverFlow = Flow.fromSinkAndSource(sink, source)

    Tcp(system).bind("127.0.0.1", 9999, halfClose = true).runForeach { incomingConnection =>
      incomingConnection.handleWith(serverFlow)
    }
    // #halfClosedTcpServer
  }

  def chat(): Unit = {
    // #chat
    val (sink, source) = MergeHub.source[String].toMat(BroadcastHub.sink[String])(Keep.both).run()

    val framing = Framing.delimiter(ByteString("\n"), 1024)

    val sinkWithFraming = framing.map(bytes => bytes.utf8String).to(sink)
    val sourceWithFraming = source.map(text => ByteString(text + "\n"))

    val serverFlow = Flow.fromSinkAndSource(sinkWithFraming, sourceWithFraming)

    Tcp(system).bind("127.0.0.1", 9999).runForeach { incomingConnection =>
      incomingConnection.handleWith(serverFlow)
    }
    // #chat
  }

  def testing(): Unit = {
    def myApiThatTakesAFlow[In, Out](flow: Flow[In, Out, NotUsed]): Unit = ???
    // #testing
    val inProbe = TestSubscriber.probe[String]()
    val outProbe = TestPublisher.probe[String]()
    val testFlow = Flow.fromSinkAndSource(Sink.fromSubscriber(inProbe), Source.fromPublisher(outProbe))

    myApiThatTakesAFlow(testFlow)
    inProbe.expectNext("first")
    outProbe.expectRequest()
    outProbe.sendError(new RuntimeException("test error"))
    // ...
    // #testing
  }
}
