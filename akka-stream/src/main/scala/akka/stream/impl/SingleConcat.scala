/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

/**
 * Concatenating a single element to a stream is common enough that it warrants this optimization
 * which avoids the actual fan-out for such cases.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final class SingleConcat[E](singleElem: E) extends GraphStage[FlowShape[E, E]] {

  val in = Inlet[E]("SingleConcat.in")
  val out = Outlet[E]("SingleConcat.out")

  override val shape: FlowShape[E, E] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      override def onPush(): Unit = {
        push(out, grab(in))
      }

      override def onPull(): Unit = pull(in)

      override def onUpstreamFinish(): Unit = {
        emit(out, singleElem, () => completeStage())
      }
      setHandlers(in, out, this)
    }

  override def toString: String = s"SingleConcat($singleElem)"
}
