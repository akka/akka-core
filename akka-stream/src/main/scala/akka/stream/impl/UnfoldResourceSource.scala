/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.tailrec
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class UnfoldResourceSource[R, T](
    create: () => R,
    readData: R => Option[T],
    close: R => Unit)
    extends GraphStage[SourceShape[T]] {
  val out = Outlet[T]("UnfoldResourceSource.out")
  override val shape = SourceShape(out)
  override def initialAttributes: Attributes =
    DefaultAttributes.unfoldResourceSource and SourceLocation.forLambda(create)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic with OutHandler =
    new GraphStageLogic(shape) with OutHandler {
      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
      var open = false
      var resource: R = _
      setHandler(out, this)

      override def preStart(): Unit = {
        resource = create()
        open = true
      }

      @tailrec
      final override def onPull(): Unit = {
        var resumingMode = false
        try {
          readData(resource) match {
            case Some(data) => push(out, data)
            case None       => closeStage()
          }
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop =>
                open = false
                close(resource)
                failStage(ex)
              case Supervision.Restart =>
                restartState()
                resumingMode = true
              case Supervision.Resume =>
                resumingMode = true
            }
        }
        if (resumingMode) onPull()
      }

      override def onDownstreamFinish(cause: Throwable): Unit = closeStage()

      private def restartState(): Unit = {
        open = false
        close(resource)
        resource = create()
        open = true
      }

      private def closeStage(): Unit =
        try {
          close(resource)
          open = false
          completeStage()
        } catch {
          case NonFatal(ex) => failStage(ex)
        }

      override def postStop(): Unit = {
        if (open) close(resource)
      }

    }
  override def toString = "UnfoldResourceSource"
}
