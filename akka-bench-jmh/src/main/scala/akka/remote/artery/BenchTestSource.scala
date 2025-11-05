/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.OutHandler

/**
 * Emits integers from 1 to the given `elementCount`. The `java.lang.Integer`
 * objects are allocated in the constructor of the operator, so it should be created
 * before the benchmark is started.
 */
class BenchTestSource(elementCount: Int) extends GraphStage[SourceShape[java.lang.Integer]] {

  private val elements = new Array[java.lang.Integer](elementCount)
  (1 to elementCount).foreach(n => elements(n - 1) = n)

  val out: Outlet[java.lang.Integer] = Outlet("BenchTestSource")
  override val shape: SourceShape[java.lang.Integer] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      var n = 0

      override def onPull(): Unit = {
        n += 1
        if (n > elementCount)
          complete(out)
        else
          push(out, elements(n - 1))
      }

      setHandler(out, this)
    }
}

class BenchTestSourceSameElement[T](elements: Int, elem: T) extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("BenchTestSourceSameElement")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      var n = 0

      override def onPull(): Unit = {
        n += 1
        if (n > elements)
          complete(out)
        else
          push(out, elem)
      }

      setHandler(out, this)
    }
}
