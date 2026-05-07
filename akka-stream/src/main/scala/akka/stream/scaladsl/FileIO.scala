/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.io.File
import java.nio.file.{ OpenOption, Path }
import java.nio.file.StandardOpenOption._

import scala.concurrent.Future

import akka.stream.IOResult
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.io._
import akka.util.ByteString

/**
 * Factories to create sinks and sources from files
 */
object FileIO {

  /**
   * Creates a Source from a files contents.
   * Emitted elements are `chunkSize` sized [[akka.util.ByteString]] elements,
   * except the final element, which will be up to `chunkSize` in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[Future]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * @param f         the file to read from
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  @deprecated("Use `fromPath` instead", "2.4.5")
  def fromFile(f: File, chunkSize: Int = 8192): Source[ByteString, Future[IOResult]] =
    fromPath(f.toPath, chunkSize)

  /**
   * Creates a Source from a files contents.
   * Emitted elements are `chunkSize` sized [[akka.util.ByteString]] elements,
   * except the final element, which will be up to `chunkSize` in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[Future]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * It is not possible to read FIFOs, also known as named pipes, with `fromPath`, trying to do so will potentially first block
   * and then fail the stream.
   *
   * @param f         the file path to read from
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  def fromPath(f: Path, chunkSize: Int = 8192): Source[ByteString, Future[IOResult]] =
    fromPath(f, chunkSize, startPosition = 0)

  /**
   * Creates a Source from a files contents.
   * Emitted elements are `chunkSize` sized [[akka.util.ByteString]] elements,
   * except the final element, which will be up to `chunkSize` in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[Future]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * It is not possible to read FIFOs, also known as named pipes, with `fromPath`, trying to do so will potentially first block
   * and then fail the stream.
   *
   * @param f         the file path to read from
   * @param chunkSize the size of each read operation, defaults to 8192
   * @param startPosition the start position to read from
   */
  def fromPath(f: Path, chunkSize: Int, startPosition: Long): Source[ByteString, Future[IOResult]] =
    Source.fromGraph(new FileSource(f, chunkSize, startPosition)).withAttributes(DefaultAttributes.fileSource)

  /**
   * Creates a Sink which writes incoming [[ByteString]] elements to the given file. Overwrites existing files
   * by truncating their contents as default.
   *
   * Materializes a [[Future]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * This source is backed by an Actor which will use the dedicated `akka.stream.blocking-io-dispatcher`,
   * unless configured otherwise by using [[akka.stream.ActorAttributes]].
   *
   * @param f the file to write to
   * @param options File open options, see [[java.nio.file.StandardOpenOption]], defaults to Set(WRITE, TRUNCATE_EXISTING, CREATE)
   */
  @deprecated("Use `toPath` instead", "2.4.5")
  def toFile(
      f: File,
      options: Set[OpenOption] = Set(WRITE, TRUNCATE_EXISTING, CREATE)): Sink[ByteString, Future[IOResult]] =
    toPath(f.toPath, options)

  /**
   * Creates a Sink which writes incoming [[ByteString]] elements to the given file path. Overwrites existing files
   * by truncating their contents as default.
   *
   * Materializes a [[Future]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * This source is backed by an Actor which will use the dedicated `akka.stream.blocking-io-dispatcher`,
   * unless configured otherwise by using [[akka.stream.ActorAttributes]].
   *
   * Accepts as arguments a set of [[java.nio.file.StandardOpenOption]], which will determine
   * the underlying behavior when writing the file. If [[java.nio.file.StandardOpenOption.SYNC]] is
   * provided, every update to the file's content be written synchronously to the underlying storage
   * device. Otherwise (the default), the write will be written to the storage device asynchronously
   * by the OS, and may not be stored durably on the storage device at the time the stream completes.
   *
   * @param f the file path to write to
   * @param options File open options, see [[java.nio.file.StandardOpenOption]], defaults to Set(WRITE, TRUNCATE_EXISTING, CREATE)
   */
  def toPath(
      f: Path,
      options: Set[OpenOption] = Set(WRITE, TRUNCATE_EXISTING, CREATE)): Sink[ByteString, Future[IOResult]] =
    toPath(f, options, startPosition = 0)

  /**
   * Creates a Sink which writes incoming [[ByteString]] elements to the given file path. Overwrites existing files
   * by truncating their contents as default.
   *
   * Materializes a [[Future]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * This source is backed by an Actor which will use the dedicated `akka.stream.blocking-io-dispatcher`,
   * unless configured otherwise by using [[akka.stream.ActorAttributes]].
   *
   * Accepts as arguments a set of [[java.nio.file.StandardOpenOption]], which will determine
   * the underlying behavior when writing the file. If [[java.nio.file.StandardOpenOption.SYNC]] is
   * provided, every update to the file's content be written synchronously to the underlying storage
   * device. Otherwise (the default), the write will be written to the storage device asynchronously
   * by the OS, and may not be stored durably on the storage device at the time the stream completes.
   *
   * @param f the file path to write to
   * @param options File open options, see [[java.nio.file.StandardOpenOption]], defaults to Set(WRITE, CREATE)
   * @param startPosition the start position to write to
   */
  def toPath(f: Path, options: Set[OpenOption], startPosition: Long): Sink[ByteString, Future[IOResult]] =
    Sink.fromGraph(new FileOutputStage(f, startPosition, options))
}
