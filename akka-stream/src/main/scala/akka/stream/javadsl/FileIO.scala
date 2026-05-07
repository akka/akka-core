/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.io.File
import java.nio.file.{ OpenOption, Path }
import java.util
import java.util.concurrent.CompletionStage

import akka.stream.{ javadsl, scaladsl, IOResult }
import akka.stream.scaladsl.SinkToCompletionStage
import akka.stream.scaladsl.SourceToCompletionStage
import akka.util.ByteString
import scala.jdk.CollectionConverters._

/**
 * Java API: Factories to create sinks and sources from files
 */
object FileIO {

  /**
   * Creates a Sink that writes incoming [[ByteString]] elements to the given file.
   * Overwrites existing files by truncating their contents, if you want to append to an existing file use
   * [[toFile(File, util.Set[OpenOption])]] with [[java.nio.file.StandardOpenOption.APPEND]].
   *
   * Materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * @param f The file to write to
   */
  @deprecated("Use `toPath` instead.", "2.4.5")
  def toFile(f: File): javadsl.Sink[ByteString, CompletionStage[IOResult]] = toPath(f.toPath)

  /**
   * Creates a Sink that writes incoming [[ByteString]] elements to the given file path.
   * Overwrites existing files by truncating their contents, if you want to append to an existing file
   * [[toPath(Path, util.Set[OpenOption])]] with [[java.nio.file.StandardOpenOption.APPEND]].
   *
   * Materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * Accepts as arguments a set of [[java.nio.file.StandardOpenOption]], which will determine
   * the underlying behavior when writing the file. If [[java.nio.file.StandardOpenOption.SYNC]] is
   * provided, every update to the file's content be written synchronously to the underlying storage
   * device. Otherwise (the default), the write will be written to the storage device asynchronously
   * by the OS, and may not be stored durably on the storage device at the time the stream completes.
   *
   * @param f The file path to write to
   */
  def toPath(f: Path): javadsl.Sink[ByteString, CompletionStage[IOResult]] =
    new Sink(scaladsl.FileIO.toPath(f).toCompletionStage())

  /**
   * Creates a Sink that writes incoming [[ByteString]] elements to the given file.
   *
   * Materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * @param f The file to write to
   * @param options File open options, see [[java.nio.file.StandardOpenOption]]
   */
  @deprecated("Use `toPath` instead.", "2.4.5")
  def toFile[Opt <: OpenOption](f: File, options: util.Set[Opt]): javadsl.Sink[ByteString, CompletionStage[IOResult]] =
    toPath(f.toPath)

  /**
   * Creates a Sink that writes incoming [[ByteString]] elements to the given file path.
   *
   * Materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * Accepts as arguments a set of [[java.nio.file.StandardOpenOption]], which will determine
   * the underlying behavior when writing the file. If [[java.nio.file.StandardOpenOption.SYNC]] is
   * provided, every update to the file's content be written synchronously to the underlying storage
   * device. Otherwise (the default), the write will be written to the storage device asynchronously
   * by the OS, and may not be stored durably on the storage device at the time the stream completes.
   *
   * @param f The file path to write to
   * @param options File open options, see [[java.nio.file.StandardOpenOption]]
   */
  def toPath[Opt <: OpenOption](f: Path, options: util.Set[Opt]): javadsl.Sink[ByteString, CompletionStage[IOResult]] =
    new Sink(scaladsl.FileIO.toPath(f, options.asScala.toSet).toCompletionStage())

  /**
   * Creates a Sink that writes incoming [[ByteString]] elements to the given file path.
   *
   * Materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * Accepts as arguments a set of [[java.nio.file.StandardOpenOption]], which will determine
   * the underlying behavior when writing the file. If [[java.nio.file.StandardOpenOption.SYNC]] is
   * provided, every update to the file's content be written synchronously to the underlying storage
   * device. Otherwise (the default), the write will be written to the storage device asynchronously.
   * by the OS, and may not be stored durably on the storage device at the time the stream completes.
   *
   * @param f The file path to write to
   * @param options File open options, see [[java.nio.file.StandardOpenOption]]
   * @param startPosition startPosition the start position to read from, defaults to 0
   */
  def toPath[Opt <: OpenOption](
      f: Path,
      options: util.Set[Opt],
      startPosition: Long): javadsl.Sink[ByteString, CompletionStage[IOResult]] =
    new Sink(scaladsl.FileIO.toPath(f, options.asScala.toSet, startPosition).toCompletionStage())

  /**
   * Creates a Source from a files contents.
   * Emitted elements are [[ByteString]] elements, chunked by default by 8192 bytes,
   * except the last element, which will be up to 8192 in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * @param f         the file to read from
   */
  @deprecated("Use `fromPath` instead.", "2.4.5")
  def fromFile(f: File): javadsl.Source[ByteString, CompletionStage[IOResult]] = fromPath(f.toPath)

  /**
   * Creates a Source from a files contents.
   * Emitted elements are [[ByteString]] elements, chunked by default by 8192 bytes,
   * except the last element, which will be up to 8192 in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * It is not possible to read FIFOs, also known as named pipes, with `fromPath`, trying to do so will potentially first block
   * and then fail the stream.
   *
   * @param f         the file path to read from
   */
  def fromPath(f: Path): javadsl.Source[ByteString, CompletionStage[IOResult]] = fromPath(f, 8192)

  /**
   * Creates a synchronous Source from a files contents.
   * Emitted elements are `chunkSize` sized [[ByteString]] elements,
   * except the last element, which will be up to `chunkSize` in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   *  not give any guarantee that the bytes were seen by downstream stages.
   *
   * @param f         the file to read from
   * @param chunkSize the size of each read operation
   */
  @deprecated("Use `fromPath` instead.", "2.4.5")
  def fromFile(f: File, chunkSize: Int): javadsl.Source[ByteString, CompletionStage[IOResult]] =
    fromPath(f.toPath, chunkSize)

  /**
   * Creates a synchronous Source from a files contents.
   * Emitted elements are `chunkSize` sized [[ByteString]] elements,
   * except the last element, which will be up to `chunkSize` in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * It is not possible to read FIFOs, also known as named pipes, with `fromPath`, trying to do so will potentially first block
   * and then fail the stream.
   *
   * @param f         the file path to read from
   * @param chunkSize the size of each read operation
   */
  def fromPath(f: Path, chunkSize: Int): javadsl.Source[ByteString, CompletionStage[IOResult]] =
    new Source(scaladsl.FileIO.fromPath(f, chunkSize).toCompletionStage())

  /**
   * Creates a synchronous Source from a files contents.
   * Emitted elements are `chunkSize` sized [[ByteString]] elements,
   * except the last element, which will be up to `chunkSize` in size.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[java.util.concurrent.CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * @param f         the file path to read from
   * @param chunkSize the size of each read operation
   * @param startPosition startPosition the start position to read from, defaults to 0
   */
  def fromPath(f: Path, chunkSize: Int, startPosition: Long): javadsl.Source[ByteString, CompletionStage[IOResult]] =
    new Source(scaladsl.FileIO.fromPath(f, chunkSize, startPosition).toCompletionStage())
}
