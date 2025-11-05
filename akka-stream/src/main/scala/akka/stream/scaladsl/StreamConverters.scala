/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.io.{ InputStream, OutputStream }
import java.util.Spliterators
import java.util.stream.{ Collector, StreamSupport }

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._

import akka.NotUsed
import akka.stream.{ Attributes, IOResult, SinkShape }
import akka.stream.impl._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.io.{ InputStreamSinkStage, InputStreamSource, OutputStreamGraphStage, OutputStreamSourceStage }
import akka.util.ByteString

/**
 * Converters for interacting with the blocking `java.io` streams APIs and Java 8 Streams
 */
object StreamConverters {

  /**
   * Creates a Source from an [[InputStream]] created by the given function.
   * Emitted elements are up to `chunkSize` sized [[akka.util.ByteString]] elements.
   * The actual size of the emitted elements depends on how much data the underlying
   * [[java.io.InputStream]] returns on each read invocation. Such chunks will
   * never be larger than chunkSize though.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   *
   * It materializes a [[Future]] of [[IOResult]] containing the number of bytes read from the source file upon completion,
   * and a possible exception if IO operation was not completed successfully. Note that bytes having been read by the source does
   * not give any guarantee that the bytes were seen by downstream stages.
   *
   * The created [[InputStream]] will be closed when the [[Source]] is cancelled.
   *
   * @param in a function which creates the InputStream to read from
   * @param chunkSize the size of each read operation, defaults to 8192
   */
  def fromInputStream(in: () => InputStream, chunkSize: Int = 8192): Source[ByteString, Future[IOResult]] = {
    Source.fromGraph(new InputStreamSource(in, chunkSize))
  }

  /**
   * Creates a Source which when materialized will return an [[OutputStream]] which it is possible
   * to write the ByteStrings to the stream this Source is attached to.
   *
   * This Source is intended for inter-operation with legacy APIs since it is inherently blocking.
   *
   * You can configure the internal buffer size by using [[akka.stream.ActorAttributes]].
   *
   * The created [[OutputStream]] will be closed when the [[Source]] is cancelled, and closing the [[OutputStream]]
   * will complete this [[Source]].
   *
   * @param writeTimeout the max time the write operation on the materialized OutputStream should block, defaults to 5 seconds
   */
  def asOutputStream(writeTimeout: FiniteDuration = 5.seconds): Source[ByteString, OutputStream] =
    Source.fromGraph(new OutputStreamSourceStage(writeTimeout))

  /**
   * Creates a Sink which writes incoming [[ByteString]]s to an [[OutputStream]] created by the given function.
   *
   * Materializes a [[Future]] of [[IOResult]] that will be completed with the size of the file (in bytes) at the streams completion,
   * and a possible exception if IO operation was not completed successfully.
   *
   * You can configure the default dispatcher for this Source by changing the `akka.stream.materializer.blocking-io-dispatcher` or
   * set it for a given Source by using [[akka.stream.ActorAttributes]].
   * If `autoFlush` is true the OutputStream will be flushed whenever a byte array is written, defaults to false.
   *
   * The [[OutputStream]] will be closed when the stream flowing into this [[Sink]] is completed. The [[Sink]]
   * will cancel the stream when the [[OutputStream]] is no longer writable.
   */
  def fromOutputStream(out: () => OutputStream, autoFlush: Boolean = false): Sink[ByteString, Future[IOResult]] =
    Sink.fromGraph(new OutputStreamGraphStage(out, autoFlush))

  /**
   * Creates a Sink which when materialized will return an [[InputStream]] which it is possible
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
  def asInputStream(readTimeout: FiniteDuration = 5.seconds): Sink[ByteString, InputStream] =
    Sink.fromGraph(new InputStreamSinkStage(readTimeout))

  /**
   * Creates a sink which materializes into a ``Future`` which will be completed with result of the Java 8 ``Collector`` transformation
   * and reduction operations. This allows usage of Java 8 streams transformations for reactive streams. The ``Collector`` will trigger
   * demand downstream. Elements emitted through the stream will be accumulated into a mutable result container, optionally transformed
   * into a final representation after all input elements have been processed. The ``Collector`` can also do reduction
   * at the end. Reduction processing is performed sequentially
   *
   * Note that a flow can be materialized multiple times, so the function producing the ``Collector`` must be able
   * to handle multiple invocations.
   */
  def javaCollector[T, R](collectorFactory: () => java.util.stream.Collector[T, _ <: Any, R]): Sink[T, Future[R]] =
    Flow[T]
      .fold {
        new FirstCollectorState[T, R](collectorFactory.asInstanceOf[() => java.util.stream.Collector[T, Any, R]]): CollectorState[
          T,
          R]
      } { (state, elem) =>
        state.update(elem)
      }
      .map(state => state.finish())
      .toMat(Sink.head)(Keep.right)
      .withAttributes(DefaultAttributes.javaCollector)

  /**
   * Creates a sink which materializes into a ``Future`` which will be completed with result of the Java 8 ``Collector`` transformation
   * and reduction operations. This allows usage of Java 8 streams transformations for reactive streams. The ``Collector`` will trigger demand
   * downstream. Elements emitted through the stream will be accumulated into a mutable result container, optionally transformed
   * into a final representation after all input elements have been processed. The ``Collector`` can also do reduction
   * at the end. Reduction processing is performed in parallel based on graph ``Balance``.
   *
   * Note that a flow can be materialized multiple times, so the function producing the ``Collector`` must be able
   * to handle multiple invocations.
   */
  def javaCollectorParallelUnordered[T, R](parallelism: Int)(
      collectorFactory: () => java.util.stream.Collector[T, _ <: Any, R]): Sink[T, Future[R]] = {
    if (parallelism == 1) javaCollector[T, R](collectorFactory)
    else {
      Sink
        .fromGraph(GraphDSL.createGraph(Sink.head[R]) { implicit b => sink =>
          import GraphDSL.Implicits._
          val factory = collectorFactory.asInstanceOf[() => Collector[T, Any, R]]
          val balance = b.add(Balance[T](parallelism))
          val merge = b.add(Merge[CollectorState[T, R]](parallelism))

          for (i <- 0 until parallelism) {
            val worker = Flow[T]
              .fold(new FirstCollectorState(factory): CollectorState[T, R]) { (state, elem) =>
                state.update(elem)
              }
              .async

            balance.out(i) ~> worker ~> merge.in(i)
          }

          merge.out
            .fold(new FirstReducerState(factory): ReducerState[T, R]) { (state, elem) =>
              state.update(elem.accumulated())
            }
            .map(state => state.finish()) ~> sink.in

          SinkShape(balance.in)
        })
        .withAttributes(DefaultAttributes.javaCollectorParallelUnordered)
    }
  }

  /**
   * Creates a sink which materializes into Java 8 ``Stream`` that can be run to trigger demand through the sink.
   * Elements emitted through the stream will be available for reading through the Java 8 ``Stream``.
   *
   * The Java 8 ``Stream`` will be ended when the stream flowing into this ``Sink`` completes, and closing the Java
   * ``Stream`` will cancel the inflow of this ``Sink``.
   *
   * If the Java 8 ``Stream`` throws exception the Akka stream is cancelled.
   *
   * Be aware that Java ``Stream`` blocks current thread while waiting on next element from downstream.
   * As it is interacting wit blocking API the implementation runs on a separate dispatcher
   * configured through the ``akka.stream.blocking-io-dispatcher``.
   */
  def asJavaStream[T](): Sink[T, java.util.stream.Stream[T]] = {
    // TODO removing the QueueSink name, see issue #22523
    Sink
      .fromGraph(new QueueSink[T](1).withAttributes(Attributes.none))
      .mapMaterializedValue(
        queue =>
          StreamSupport
            .stream(
              Spliterators.spliteratorUnknownSize(
                new java.util.Iterator[T] {
                  var nextElementFuture: Future[Option[T]] = queue.pull()
                  var nextElement: Option[T] = _

                  override def hasNext: Boolean = {
                    nextElement = Await.result(nextElementFuture, Inf)
                    nextElement.isDefined
                  }

                  override def next(): T = {
                    val next = nextElement.get
                    nextElementFuture = queue.pull()
                    next
                  }
                },
                0),
              false)
            .onClose(new Runnable { def run = queue.cancel() }))
      .withAttributes(DefaultAttributes.asJavaStream)
  }

  /**
   * Creates a source that wraps a Java 8 ``Stream``. ``Source`` uses a stream iterator to get all its
   * elements and send them downstream on demand.
   *
   * Example usage: `StreamConverters.fromJavaStream(() => IntStream.rangeClosed(1, 10))`
   *
   * You can use [[Source.async]] to create asynchronous boundaries between synchronous Java ``Stream``
   * and the rest of flow.
   */
  def fromJavaStream[T, S <: java.util.stream.BaseStream[T, S]](
      stream: () => java.util.stream.BaseStream[T, S]): Source[T, NotUsed] =
    Source.fromGraph(new JavaStreamSource[T, S](stream)).withAttributes(DefaultAttributes.fromJavaStream)
}
