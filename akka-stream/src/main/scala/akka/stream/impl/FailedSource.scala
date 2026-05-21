/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.annotation.InternalApi
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class FailedSource[T](val failure: Throwable) extends GraphStage[SourceShape[T]] {
  val out = Outlet[T]("FailedSource.out")
  override val shape = SourceShape(out)

  override protected def initialAttributes: Attributes = DefaultAttributes.failedSource

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      override def onPull(): Unit = ()

      override def preStart(): Unit = {
        failStage(failure)
      }
      setHandler(out, this)
    }

  override def toString = s"FailedSource(${failure.getClass.getName})"
}
