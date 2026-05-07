/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Processor

import akka.stream.Attributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.scaladsl.Flow
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

class TransformProcessorTest extends AkkaIdentityProcessorVerification[Int] {

  override def createIdentityProcessor(maxBufferSize: Int): Processor[Int, Int] = {
    val stage =
      new SimpleLinearGraphStage[Int] {
        override def createLogic(inheritedAttributes: Attributes) =
          new GraphStageLogic(shape) with InHandler with OutHandler {
            override def onPush(): Unit = push(out, grab(in))
            override def onPull(): Unit = pull(in)
            setHandlers(in, out, this)
          }
      }

    Flow[Int]
      .via(stage)
      .toProcessor
      .withAttributes(Attributes.inputBuffer(initial = maxBufferSize / 2, max = maxBufferSize))
      .run()
  }

  override def createElement(element: Int): Int = element

}
