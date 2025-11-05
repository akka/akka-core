/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://akka.io>
 */

package akka.stream.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }

/** INTERNAL API */
@InternalApi private[stream] final class JavaStreamSource[T, S <: java.util.stream.BaseStream[T, S]](
    open: () => java.util.stream.BaseStream[T, S])
    extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("JavaStreamSource")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private[this] var stream: java.util.stream.BaseStream[T, S] = _
      private[this] var iter: java.util.Iterator[T] = _

      setHandler(out, this)

      override def preStart(): Unit = {
        stream = open()
        iter = stream.iterator()
      }

      override def postStop(): Unit = {
        if (stream ne null)
          stream.close()
      }

      override def onPull(): Unit = {
        if (iter.hasNext) {
          push(out, iter.next())
        } else {
          complete(out)
        }
      }
    }
}
