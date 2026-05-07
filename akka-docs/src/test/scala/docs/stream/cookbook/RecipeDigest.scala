/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeDigest extends RecipeSpec {

  "Recipe for calculating digest" must {

    "work" in {

      //#calculating-digest
      import java.security.MessageDigest

      import akka.NotUsed
      import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
      import akka.stream.scaladsl.{ Sink, Source }
      import akka.util.ByteString

      import akka.stream.stage._

      val data: Source[ByteString, NotUsed] = Source.single(ByteString("abc"))

      class DigestCalculator(algorithm: String) extends GraphStage[FlowShape[ByteString, ByteString]] {
        val in = Inlet[ByteString]("DigestCalculator.in")
        val out = Outlet[ByteString]("DigestCalculator.out")
        override val shape = FlowShape(in, out)

        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
          private val digest = MessageDigest.getInstance(algorithm)

          setHandler(out, new OutHandler {
            override def onPull(): Unit = pull(in)
          })

          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              val chunk = grab(in)
              digest.update(chunk.toArray)
              pull(in)
            }

            override def onUpstreamFinish(): Unit = {
              emit(out, ByteString(digest.digest()))
              completeStage()
            }
          })
        }
      }

      val digest: Source[ByteString, NotUsed] = data.via(new DigestCalculator("SHA-256"))
      //#calculating-digest

      Await.result(digest.runWith(Sink.head), 3.seconds) should be(
        ByteString(0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0,
          0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad))
    }
  }
}
