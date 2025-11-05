/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import java.nio.ByteBuffer
import java.nio.channels.{ CompletionHandler, FileChannel }
import java.nio.file.{ Files, NoSuchFileException, Path, StandardOpenOption }

import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import akka.Done
import akka.stream.{ IOResult, _ }
import akka.stream.Attributes.InputBuffer
import akka.stream.stage._
import akka.util.ByteString

/**
 * INTERNAL API
 */
private[akka] object FileSource {

  val completionHandler = new CompletionHandler[Integer, Try[Int] => Unit] {

    override def completed(result: Integer, attachment: Try[Int] => Unit): Unit = {
      attachment(Success(result))
    }

    override def failed(ex: Throwable, attachment: Try[Int] => Unit): Unit = {
      attachment(Failure(ex))
    }
  }
}

/**
 * INTERNAL API
 * Creates simple asynchronous Source backed by the given file.
 */
private[akka] final class FileSource(path: Path, chunkSize: Int, startPosition: Long)
    extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[IOResult]] {
  require(chunkSize > 0, "chunkSize must be greater than 0")
  val out = Outlet[ByteString]("FileSource.out")

  override val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val ioResultPromise = Promise[IOResult]()

    val logic = new GraphStageLogic(shape) with OutHandler {
      handler =>
      val buffer = ByteBuffer.allocate(chunkSize)
      val maxReadAhead = inheritedAttributes.get[InputBuffer](InputBuffer(16, 16)).max
      var channel: FileChannel = _
      var position = startPosition
      var chunkCallback: Try[Int] => Unit = _
      var eofEncountered = false
      var availableChunks: Vector[ByteString] = Vector.empty[ByteString]

      setHandler(out, this)

      override def preStart(): Unit = {
        try {
          // this is a bit weird but required to keep existing semantics
          if (!Files.exists(path)) throw new NoSuchFileException(path.toString)

          require(!Files.isDirectory(path), s"Path '$path' is a directory")
          require(Files.isReadable(path), s"Missing read permission for '$path'")

          channel = FileChannel.open(path, StandardOpenOption.READ)
          channel.position(position)
        } catch {
          case ex: Exception =>
            ioResultPromise.trySuccess(IOResult(position, Failure(ex)))
            throw ex
        }
      }

      override def onPull(): Unit = {
        if (availableChunks.size < maxReadAhead && !eofEncountered)
          availableChunks = readAhead(maxReadAhead, availableChunks)
        //if already read something and try
        if (availableChunks.nonEmpty) {
          emitMultiple(out, availableChunks.iterator, () => if (eofEncountered) success() else setHandler(out, handler))
          availableChunks = Vector.empty[ByteString]
        } else if (eofEncountered) success()
      }

      private def success(): Unit = {
        completeStage()
        ioResultPromise.trySuccess(IOResult(position, Success(Done)))
      }

      /** BLOCKING I/O READ */
      @tailrec def readAhead(maxChunks: Int, chunks: Vector[ByteString]): Vector[ByteString] =
        if (chunks.size < maxChunks && !eofEncountered) {
          val readBytes = try channel.read(buffer, position)
          catch {
            case NonFatal(ex) =>
              failStage(ex)
              ioResultPromise.trySuccess(IOResult(position, Failure(ex)))
              throw ex
          }

          if (readBytes > 0) {
            buffer.flip()
            position += readBytes
            val newChunks = chunks :+ ByteString.fromByteBuffer(buffer)
            buffer.clear()

            if (readBytes < chunkSize) {
              eofEncountered = true
              newChunks
            } else readAhead(maxChunks, newChunks)
          } else {
            eofEncountered = true
            chunks
          }
        } else chunks

      override def onDownstreamFinish(cause: Throwable): Unit = {
        cause match {
          case _: SubscriptionWithCancelException.NonFailureCancellation =>
            success()
          case ex =>
            ioResultPromise.tryFailure(
              new IOOperationIncompleteException("Downstream failed before reaching file end", position, ex))
            completeStage()
        }
      }

      override def postStop(): Unit = {
        ioResultPromise.trySuccess(IOResult(position, Success(Done)))
        if ((channel ne null) && channel.isOpen) channel.close()
      }
    }

    (logic, ioResultPromise.future)
  }

  override def toString = s"FileSource($path, $chunkSize)"
}
