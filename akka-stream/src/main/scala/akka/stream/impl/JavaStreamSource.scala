/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }

import java.util.function.Consumer

/** INTERNAL API */
@InternalApi private[stream] final class JavaStreamSource[T, S <: java.util.stream.BaseStream[T, S]](
    val open: () => java.util.stream.BaseStream[T, S])
    extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("JavaStreamSource")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with Consumer[T] {
      private[this] var stream: java.util.stream.BaseStream[T, S] = _
      private[this] var iter: java.util.Spliterator[T] = _

      setHandler(out, this)

      override def accept(t: T): Unit = push(out, t)

      override def preStart(): Unit = {
        stream = open()
        iter = stream.spliterator()
      }

      override def postStop(): Unit = {
        if (stream ne null)
          stream.close()
      }

      override def onPull(): Unit = {
        if (!iter.tryAdvance(this))
          complete(out)
      }
    }
}
