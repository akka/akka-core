/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.scaladsl.{ Keep, Source }
import akka.stream.stage._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object LazySource {
  def apply[T, M](sourceFactory: () => Source[T, M]) = new LazySource[T, M](sourceFactory)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class LazySource[T, M](sourceFactory: () => Source[T, M])
    extends GraphStageWithMaterializedValue[SourceShape[T], Future[M]] {
  val out = Outlet[T]("LazySource.out")
  override val shape = SourceShape(out)

  override protected def initialAttributes = DefaultAttributes.lazySource and SourceLocation.forLambda(sourceFactory)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[M]) = {
    val matPromise = Promise[M]()
    val logic = new GraphStageLogic(shape) with OutHandler {

      override def onDownstreamFinish(cause: Throwable): Unit = {
        matPromise.failure(new NeverMaterializedException(cause))
        completeStage()
      }

      override def onPull(): Unit = {
        val source = try {
          sourceFactory()
        } catch {
          case NonFatal(ex) =>
            matPromise.tryFailure(ex)
            throw ex
        }
        val subSink = new SubSinkInlet[T]("LazySource")
        subSink.pull()

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            subSink.pull()
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            subSink.cancel(cause)
            completeStage()
          }
        })

        subSink.setHandler(new InHandler {
          override def onPush(): Unit = {
            push(out, subSink.grab())
          }
        })

        try {
          val matVal = subFusingMaterializer.materialize(source.toMat(subSink.sink)(Keep.left), inheritedAttributes)
          matPromise.trySuccess(matVal)
        } catch {
          case NonFatal(ex) =>
            subSink.cancel()
            failStage(ex)
            matPromise.tryFailure(ex)
        }
      }

      setHandler(out, this)

      override def postStop() = {
        if (!matPromise.isCompleted) matPromise.tryFailure(new AbruptStageTerminationException(this))
      }
    }

    (logic, matPromise.future)
  }

  override def toString = "LazySource"
}
