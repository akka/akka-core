/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.io.{ InputStream, OutputStream }
import java.util.concurrent.CompletionStage
import java.util.stream.Collector

import akka.NotUsed
import akka.japi.function
import akka.stream.{ javadsl, scaladsl }
import akka.stream.IOResult
import akka.stream.scaladsl.SinkToCompletionStage
import akka.stream.scaladsl.SourceToCompletionStage
import akka.util.ByteString

/**
 * Converters for interacting with the blocking `java.io` streams APIs and Java 8 Streams
 */
object StreamConverters {

  /**
   * Sink which writes incoming [[ByteString]]s to an [[OutputStream]] created by the given function.
   *
   * Materializes a [[CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * This method uses no auto flush for the [[java.io.OutputStream]] @see [[#fromOutputStream(function.Creator, Boolean)]] if you want to override it.
   *
   * The [[OutputStream]] will be closed when the stream flowing into this [[Sink]] is completed. The [[Sink]]
   * will cancel the stream when the [[OutputStream]] is no longer writable.
   *
   * @param f A Creator which creates an OutputStream to write to
   */
  def fromOutputStream(f: function.Creator[OutputStream]): javadsl.Sink[ByteString, CompletionStage[IOResult]] =
    fromOutputStream(f, autoFlush = false)

  /**
   * Sink which writes incoming [[ByteString]]s to an [[OutputStream]] created by the given function.
   *
   * Materializes a [[CompletionStage]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * The [[OutputStream]] will be closed when the stream flowing into this [[Sink]] is completed. The [[Sink]]
   * will cancel the stream when the [[OutputStream]] is no longer writable.
   *
   * @param f A Creator which creates an OutputStream to write to
   * @param autoFlush If true the OutputStream will be flushed whenever a byte array is written
   */
  def fromOutputStream(
      f: function.Creator[OutputStream],
      autoFlush: Boolean): javadsl.Sink[ByteString, CompletionStage[IOResult]] =
    new Sink(scaladsl.StreamConverters.fromOutputStream(() => f.create(), autoFlush).toCompletionStage())

  /**
   * Creates a Sink which when materialized will return an [[java.io.InputStream]] which it is possible
   * to read the values produced by the stream this Sink is attached to.
   *
   * This method uses a default read timeout, use [[#inputStream(FiniteDuration)]] or [[#inputStream(java.time.Duration)]] to explicitly
   * configure the timeout.
   *
   * This Sink is intended for inter-operation with legacy APIs since it is inherently blocking.
   *
   * You can configure the internal buffer size by using [[akka.stream.ActorAttributes]].
   *
   * The [[InputStream]] will be closed when the stream flowing into this [[Sink]] completes, and
   * closing the [[InputStream]] will cancel this [[Sink]].
   */
  def asInputStream(): Sink[ByteString, InputStream] = new Sink(scaladsl.StreamConverters.asInputStream())

  /**
   * Creates a Sink which when materialized will return an [[java.io.InputStream]] which it is possible
   * to read the values produced by the stream this Sink is attached to.
   *
   * This Sink is intended for inter-operation with legacy APIs since it is inherently blocking.
   *
   * You can configure the internal buffer size by using [[akka.stream.ActorAttributes]].
   *
   * The [[InputStream]] will be closed when the stream flowing into this [[Sink]] completes, and
   * closing the [[InputStream]] will cancel this [[Sink]].
   *
   * @param readTimeout the max time the read operation on the materialized InputStream should block
   */
  def asInputStream(readTimeout: java.time.Duration): Sink[ByteString, InputStream] = {
    import scala.jdk.DurationConverters._
    new Sink(scaladsl.StreamConverters.asInputStream(readTimeout.toScala))
  }

  /**
   * Creates a Source from an [[java.io.InputStream]] created by the given function.
   * Emitted elements are up to `chunkSize` sized [[akka.util.ByteString]] elements.
   * The actual size of the emitted elements depends on how much data the underlying
   * [[java.io.InputStream]] returns on each read invocation. Such chunks will
   * never be larger than chunkSize though.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * The created [[InputStream]] will be closed when the [[Source]] is cancelled.
   */
  def fromInputStream(
      in: function.Creator[InputStream],
      chunkSize: Int): javadsl.Source[ByteString, CompletionStage[IOResult]] =
    new Source(scaladsl.StreamConverters.fromInputStream(() => in.create(), chunkSize).toCompletionStage())

  /**
   * Creates a Source from an [[java.io.InputStream]] created by the given function.
   * Emitted elements are up to 8192 bytes sized [[akka.util.ByteString]] elements.
   * The actual size of the emitted elements depends on how much data the underlying
   * [[java.io.InputStream]] returns on each read invocation. Such chunks will
   * never be larger than chunkSize though.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[CompletionStage]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * The created [[InputStream]] will be closed when the [[Source]] is cancelled.
   */
  def fromInputStream(in: function.Creator[InputStream]): javadsl.Source[ByteString, CompletionStage[IOResult]] =
    fromInputStream(in, 8192)

  /**
   * Creates a Source which when materialized will return an [[java.io.OutputStream]] which it is possible
   * to write the ByteStrings to the stream this Source is attached to.
   *
   * This Source is intended for inter-operation with legacy APIs since it is inherently blocking.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * The created [[OutputStream]] will be closed when the [[Source]] is cancelled, and closing the [[OutputStream]]
   * will complete this [[Source]].
   *
   * @param writeTimeout the max time the write operation on the materialized OutputStream should block
   */
  def asOutputStream(writeTimeout: java.time.Duration): javadsl.Source[ByteString, OutputStream] = {
    import scala.jdk.DurationConverters._
    new Source(scaladsl.StreamConverters.asOutputStream(writeTimeout.toScala))
  }

  /**
   * Creates a Source which when materialized will return an [[java.io.OutputStream]] which it is possible
   * to write the ByteStrings to the stream this Source is attached to. The write timeout for OutputStreams
   * materialized will default to 5 seconds, @see [[#outputStream(FiniteDuration)]] or [[#outputStream(java.time.Duration)]] if you want to override it.
   *
   * This Source is intended for inter-operation with legacy APIs since it is inherently blocking.
   *
   * You can configure the internal buffer size by using [[akka.stream.ActorAttributes]].
   *
   * The created [[OutputStream]] will be closed when the [[Source]] is cancelled, and closing the [[OutputStream]]
   * will complete this [[Source]].
   */
  def asOutputStream(): javadsl.Source[ByteString, OutputStream] =
    new Source(scaladsl.StreamConverters.asOutputStream())

  /**
   * Creates a sink which materializes into Java 8 ``Stream`` that can be run to trigger demand through the sink.
   * Elements emitted through the stream will be available for reading through the Java 8 ``Stream``.
   *
   * The Java 8 ``Stream`` will be ended when the stream flowing into this ``Sink`` completes, and closing the Java
   * ``Stream`` will cancel the inflow of this ``Sink``.
   *
   * Java 8 ``Stream`` throws exception in case reactive stream failed.
   *
   * Be aware that Java ``Stream`` blocks current thread while waiting on next element from downstream.
   * As it is interacting wit blocking API the implementation runs on a separate dispatcher
   * configured through the ``akka.stream.blocking-io-dispatcher``.
   */
  def asJavaStream[T](): Sink[T, java.util.stream.Stream[T]] = new Sink(scaladsl.StreamConverters.asJavaStream())

  /**
   * Creates a source that wraps a Java 8 ``Stream``. ``Source`` uses a stream iterator to get all its
   * elements and send them downstream on demand.
   *
   * Example usage: `Source.fromJavaStream(() -> IntStream.rangeClosed(1, 10))`
   *
   * You can use [[Source.async]] to create asynchronous boundaries between synchronous java stream
   * and the rest of flow.
   */
  def fromJavaStream[O, S <: java.util.stream.BaseStream[O, S]](
      stream: function.Creator[java.util.stream.BaseStream[O, S]]): javadsl.Source[O, NotUsed] =
    new Source(scaladsl.StreamConverters.fromJavaStream(stream.create _))

  /**
   * Creates a sink which materializes into a ``CompletionStage`` which will be completed with a result of the Java 8 ``Collector``
   * transformation and reduction operations. This allows usage of Java 8 streams transformations for reactive streams.
   * The Collector`` will trigger demand downstream. Elements emitted through the stream will be accumulated into a mutable
   * result container, optionally transformed into a final representation after all input elements have been processed.
   * The ``Collector`` can also do reduction at the end. Reduction processing is performed sequentially
   *
   * Note that a flow can be materialized multiple times, so the function producing the ``Collector`` must be able
   * to handle multiple invocations.
   */
  def javaCollector[T, R](collector: function.Creator[Collector[T, _ <: Any, R]]): Sink[T, CompletionStage[R]] =
    new Sink(scaladsl.StreamConverters.javaCollector[T, R](() => collector.create()).toCompletionStage())

  /**
   * Creates a sink which materializes into a ``CompletionStage`` which will be completed with a result of the Java 8 ``Collector``
   * transformation and reduction operations. This allows usage of Java 8 streams transformations for reactive streams.
   * The ``Collector`` will trigger demand downstream. Elements emitted through the stream will be accumulated into a mutable
   * result container, optionally transformed into a final representation after all input elements have been processed.
   * ``Collector`` can also do reduction at the end. Reduction processing is performed in parallel based on graph ``Balance``.
   *
   * Note that a flow can be materialized multiple times, so the function producing the ``Collector`` must be able
   * to handle multiple invocations.
   */
  def javaCollectorParallelUnordered[T, R](parallelism: Int)(
      collector: function.Creator[Collector[T, _ <: Any, R]]): Sink[T, CompletionStage[R]] =
    new Sink(
      scaladsl.StreamConverters
        .javaCollectorParallelUnordered[T, R](parallelism)(() => collector.create())
        .toCompletionStage())

}
