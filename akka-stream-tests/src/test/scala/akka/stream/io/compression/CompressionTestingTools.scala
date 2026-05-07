/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io.compression

import scala.annotation.tailrec
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString

// a few useful helpers copied over from akka-http
object CompressionTestingTools {
  implicit class AddFutureAwaitResult[T](val future: Future[T]) extends AnyVal {

    /** "Safe" Await.result that doesn't throw away half of the stacktrace */
    def awaitResult(atMost: Duration): T = {
      Await.ready(future, atMost)
      future.value.get match {
        case Success(t) => t
        case Failure(ex) =>
          throw new RuntimeException(
            "Trying to await result of failed Future, see the cause for the original problem.",
            ex)
      }
    }
  }
  implicit class EnhancedByteStringTraversableOnce(val byteStrings: IterableOnce[ByteString]) extends AnyVal {
    def join: ByteString = byteStrings.iterator.foldLeft(ByteString.empty)(_ ++ _)
  }
  implicit class EnhancedByteStringSource[Mat](val byteStringStream: Source[ByteString, Mat]) extends AnyVal {
    def join(implicit materializer: Materializer): Future[ByteString] =
      byteStringStream.runFold(ByteString.empty)(_ ++ _)
    def utf8String(implicit materializer: Materializer, ec: ExecutionContext): Future[String] =
      join.map(_.utf8String)
  }

  implicit class EnhancedThrowable(val throwable: Throwable) extends AnyVal {
    def ultimateCause: Throwable = {
      @tailrec def rec(ex: Throwable): Throwable =
        if (ex.getCause == null) ex
        else rec(ex.getCause)

      rec(throwable)
    }
  }
}
