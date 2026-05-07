/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EmptySource extends GraphStage[SourceShape[Nothing]] {
  val out = Outlet[Nothing]("EmptySource.out")
  override val shape = SourceShape(out)

  override protected def initialAttributes = DefaultAttributes.emptySource

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      override def preStart(): Unit = completeStage()
      override def onPull(): Unit = completeStage()

      setHandler(out, this)
    }

  override def toString = "EmptySource"
}
