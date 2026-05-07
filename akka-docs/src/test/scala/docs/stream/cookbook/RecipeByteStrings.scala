/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.util.ByteString

import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeByteStrings extends RecipeSpec {

  "Recipes for bytestring streams" must {

    "have a working chunker" in {
      val rawBytes = Source(List(ByteString(1, 2), ByteString(3), ByteString(4, 5, 6), ByteString(7, 8, 9)))
      val ChunkLimit = 2

      //#bytestring-chunker
      import akka.stream.stage._

      class Chunker(val chunkSize: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {
        val in = Inlet[ByteString]("Chunker.in")
        val out = Outlet[ByteString]("Chunker.out")
        override val shape = FlowShape.of(in, out)
        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
          private var buffer = ByteString.empty

          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              emitChunk()
            }
          })
          setHandler(
            in,
            new InHandler {
              override def onPush(): Unit = {
                val elem = grab(in)
                buffer ++= elem
                emitChunk()
              }

              override def onUpstreamFinish(): Unit = {
                if (buffer.isEmpty) completeStage()
                else {
                  // There are elements left in buffer, so
                  // we keep accepting downstream pulls and push from buffer until emptied.
                  //
                  // It might be though, that the upstream finished while it was pulled, in which
                  // case we will not get an onPull from the downstream, because we already had one.
                  // In that case we need to emit from the buffer.
                  if (isAvailable(out)) emitChunk()
                }
              }
            })

          private def emitChunk(): Unit = {
            if (buffer.isEmpty) {
              if (isClosed(in)) completeStage()
              else pull(in)
            } else {
              val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
              buffer = nextBuffer
              push(out, chunk)
            }
          }
        }
      }

      val chunksStream = rawBytes.via(new Chunker(ChunkLimit))
      //#bytestring-chunker

      val chunksFuture = chunksStream.limit(10).runWith(Sink.seq)
      val chunks = Await.result(chunksFuture, 3.seconds)

      chunks.forall(_.size <= 2) should be(true)
      chunks.fold(ByteString.empty)(_ ++ _) should be(ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9))
    }

    "have a working bytes limiter" in {
      val SizeLimit = 9

      //#bytes-limiter
      import akka.stream.stage._
      class ByteLimiter(val maximumBytes: Long) extends GraphStage[FlowShape[ByteString, ByteString]] {
        val in = Inlet[ByteString]("ByteLimiter.in")
        val out = Outlet[ByteString]("ByteLimiter.out")
        override val shape = FlowShape.of(in, out)

        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
          private var count = 0

          setHandlers(in, out, new InHandler with OutHandler {

            override def onPull(): Unit = {
              pull(in)
            }

            override def onPush(): Unit = {
              val chunk = grab(in)
              count += chunk.size
              if (count > maximumBytes) failStage(new IllegalStateException("Too much bytes"))
              else push(out, chunk)
            }
          })
        }
      }

      val limiter = Flow[ByteString].via(new ByteLimiter(SizeLimit))
      //#bytes-limiter

      val bytes1 = Source(List(ByteString(1, 2), ByteString(3), ByteString(4, 5, 6), ByteString(7, 8, 9)))
      val bytes2 = Source(List(ByteString(1, 2), ByteString(3), ByteString(4, 5, 6), ByteString(7, 8, 9, 10)))

      Await.result(bytes1.via(limiter).limit(10).runWith(Sink.seq), 3.seconds).fold(ByteString.empty)(_ ++ _) should be(
        ByteString(1, 2, 3, 4, 5, 6, 7, 8, 9))

      an[IllegalStateException] must be thrownBy {
        Await.result(bytes2.via(limiter).limit(10).runWith(Sink.seq), 3.seconds)
      }
    }

    "demonstrate compacting" in {

      val data = Source(List(ByteString(1, 2), ByteString(3), ByteString(4, 5, 6), ByteString(7, 8, 9)))

      //#compacting-bytestrings
      val compacted: Source[ByteString, NotUsed] = data.map(_.compact)
      //#compacting-bytestrings

      Await.result(compacted.limit(10).runWith(Sink.seq), 3.seconds).forall(_.isCompact) should be(true)
    }

  }

}
