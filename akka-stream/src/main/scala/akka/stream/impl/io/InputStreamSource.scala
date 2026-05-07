/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import java.io.InputStream

import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.stream.{
  AbruptStageTerminationException,
  Attributes,
  IOOperationIncompleteException,
  IOResult,
  Outlet,
  SourceShape,
  SubscriptionWithCancelException
}
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage.{ GraphStageLogic, GraphStageLogicWithLogging, GraphStageWithMaterializedValue, OutHandler }
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class InputStreamSource(factory: () => InputStream, chunkSize: Int)
    extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[IOResult]] {

  require(chunkSize > 0, s"chunkSize must be > 0 (was $chunkSize)")

  private val out: Outlet[ByteString] = Outlet("InputStreamSource.out")

  override def shape: SourceShape[ByteString] = SourceShape(out)

  override protected def initialAttributes: Attributes = DefaultAttributes.inputStreamSource

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val mat = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) with OutHandler {
      private val buffer = new Array[Byte](chunkSize)
      private var readBytesTotal = 0L
      private var inputStream: InputStream = _
      private def isClosed = mat.isCompleted

      override protected def logSource: Class[_] = classOf[InputStreamSource]

      override def preStart(): Unit = {
        try {
          inputStream = factory()
        } catch {
          case NonFatal(t) =>
            mat.failure(new IOOperationIncompleteException(0, t))
            failStage(t)
        }
      }

      override def onPull(): Unit =
        try {
          inputStream.read(buffer) match {
            case -1 =>
              closeStage()
            case readBytes =>
              readBytesTotal += readBytes
              push(out, ByteString.fromArray(buffer, 0, readBytes))
          }
        } catch {
          case NonFatal(t) =>
            failStream(t)
            failStage(t)
        }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        if (!isClosed) {
          closeInputStream()
          cause match {
            case _: SubscriptionWithCancelException.NonFailureCancellation =>
              mat.trySuccess(IOResult(readBytesTotal))
            case ex: Throwable =>
              mat.tryFailure(
                new IOOperationIncompleteException(
                  "Downstream failed before input stream reached end",
                  readBytesTotal,
                  ex))
          }
        }
      }

      override def postStop(): Unit = {
        if (!isClosed) {
          mat.tryFailure(new AbruptStageTerminationException(this))
        }
      }

      private def closeStage(): Unit = {
        closeInputStream()
        mat.trySuccess(IOResult(readBytesTotal))
        completeStage()
      }

      private def failStream(reason: Throwable): Unit = {
        closeInputStream()
        mat.tryFailure(new IOOperationIncompleteException(readBytesTotal, reason))
      }

      private def closeInputStream(): Unit = {
        try {
          if (inputStream != null)
            inputStream.close()
        } catch {
          case NonFatal(ex) =>
            mat.tryFailure(new IOOperationIncompleteException(readBytesTotal, ex))
            failStage(ex)
        }
      }

      setHandler(out, this)
    }
    (logic, mat.future)

  }
  override def toString: String = "InputStreamSource"
}
