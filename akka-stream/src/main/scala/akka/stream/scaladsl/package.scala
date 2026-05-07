/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.CompletionStage

import scala.jdk.FutureConverters._
import scala.concurrent.Future

/**
 * Scala API: The flow DSL allows the formulation of stream transformations based on some
 * input. The starting point is called [[Source]] and can be a collection, an iterator,
 * a block of code which is evaluated repeatedly or a [[org.reactivestreams.Publisher]].
 * A flow with an attached input and open output is also a [[Source]].
 *
 * A flow may also be defined without an attached input or output and that is then
 * a [[Flow]]. The `Flow` can be connected to the `Source` later by using [[Source#via]] with
 * the flow as argument, and it remains a [[Source]].
 *
 * Transformations can be appended to `Source` and `Flow` with the operations
 * defined in [[FlowOps]]. Each DSL element produces a new flow that can be further transformed,
 * building up a description of the complete transformation pipeline.
 *
 * The termination point of a flow is called [[Sink]] and can for example be a `Future` or
 * [[org.reactivestreams.Subscriber]]. A flow with an attached output and open input
 * is also a [[Sink]].
 *
 * If a flow has both an attached input and an attached output it becomes a [[RunnableGraph]].
 * In order to execute this pipeline the flow must be materialized by calling [[RunnableGraph#run]] on it.
 *
 * You can create your `Source`, `Flow` and `Sink` in any order and then wire them together before
 * they are materialized by connecting them using [[Flow#via]] and [[Flow#to]], or connecting them into a
 * [[GraphDSL]] with fan-in and fan-out elements.
 *
 * See <a href="https://github.com/reactive-streams/reactive-streams/">Reactive Streams</a> for
 * details on [[org.reactivestreams.Publisher]] and [[org.reactivestreams.Subscriber]].
 *
 * It should be noted that the streams modeled by this library are “hot”,
 * meaning that they asynchronously flow through a series of processors without
 * detailed control by the user. In particular it is not predictable how many
 * elements a given transformation step might buffer before handing elements
 * downstream, which means that transformation functions may be invoked more
 * often than for corresponding transformations on strict collections like
 * [[List]]. *An important consequence* is that elements that were produced
 * into a stream may be discarded by later processors, e.g. when using the
 * [[#take]] operator.
 *
 * By default every operation is executed within its own [[akka.actor.Actor]]
 * to enable full pipelining of the chained set of computations. This behavior
 * is determined by the [[akka.stream.Materializer]] which is required
 * by those methods that materialize the Flow into a series of
 * [[org.reactivestreams.Processor]] instances. The returned reactive stream
 * is fully started and active.
 */
package object scaladsl {
  implicit class SourceToCompletionStage[Out, T](val src: Source[Out, Future[T]]) extends AnyVal {
    def toCompletionStage(): Source[Out, CompletionStage[T]] =
      src.mapMaterializedValue(_.asJava)
  }
  implicit class SinkToCompletionStage[In, T](val sink: Sink[In, Future[T]]) extends AnyVal {
    def toCompletionStage(): Sink[In, CompletionStage[T]] =
      sink.mapMaterializedValue(_.asJava)
  }
}
