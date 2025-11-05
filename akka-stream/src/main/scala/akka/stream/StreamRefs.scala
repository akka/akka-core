/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.language.implicitConversions

import akka.NotUsed
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.annotation.DoNotInherit
import akka.stream.impl.streamref.StreamRefResolverImpl
import akka.stream.scaladsl.{ Sink, Source }

/**
 * See full documentation on [[SinkRef]].
 */
object SinkRef {

  /** Implicitly converts a [[SinkRef]] to a [[Sink]]. The same can be achieved by calling `.sink` on the reference. */
  implicit def convertRefToSink[T](sinkRef: SinkRef[T]): Sink[T, NotUsed] = sinkRef.sink()
}

/**
 * A [[SinkRef]] allows sharing a "reference" to a [[Sink]] with others, with the main purpose of crossing a network boundary.
 * Usually obtaining a SinkRef would be done via Actor messaging, in which one system asks a remote one,
 * to accept some data from it, and the remote one decides to accept the request to send data in a back-pressured
 * streaming fashion -- using a sink ref.
 *
 * To create a [[SinkRef]] you have to materialize the `Sink` that you want to obtain a reference to by attaching it to a `StreamRefs.sinkRef()`.
 *
 * Stream refs can be seen as Reactive Streams over network boundaries.
 * See also [[akka.stream.SourceRef]] which is the dual of a `SinkRef`.
 *
 * For additional configuration see `reference.conf` as well as [[akka.stream.StreamRefAttributes]].
 *
 * Not for user extension.
 */
@DoNotInherit
trait SinkRef[In] {

  /** Scala API: Get [[Sink]] underlying to this source ref. */
  def sink(): Sink[In, NotUsed]

  /** Java API: Get [[javadsl.Sink]] underlying to this source ref. */
  final def getSink(): javadsl.Sink[In, NotUsed] = sink().asJava
}

/**
 * See full documentation on [[SourceRef]].
 */
object SourceRef {

  /** Implicitly converts a SourceRef to a Source. The same can be achieved by calling `.source` on the SourceRef itself. */
  implicit def convertRefToSource[T](ref: SourceRef[T]): Source[T, NotUsed] =
    ref.source
}

/**
 * A SourceRef allows sharing a "reference" with others, with the main purpose of crossing a network boundary.
 * Usually obtaining a SourceRef would be done via Actor messaging, in which one system asks a remote one,
 * to share some data with it, and the remote one decides to do so in a back-pressured streaming fashion -- using a stream ref.
 *
 * To create a [[SourceRef]] you have to materialize the `Source` that you want to obtain a reference to by attaching it to a `Sink.sourceRef`.
 *
 * Stream refs can be seen as Reactive Streams over network boundaries.
 * See also [[akka.stream.SinkRef]] which is the dual of a `SourceRef`.
 *
 * For additional configuration see `reference.conf` as well as [[akka.stream.StreamRefAttributes]].
 *
 * Not for user extension.
 */
@DoNotInherit
trait SourceRef[T] {

  /** Scala API: Get [[Source]] underlying to this source ref. */
  def source: Source[T, NotUsed]

  /** Java API: Get [[javadsl.Source]] underlying to this source ref. */
  final def getSource: javadsl.Source[T, NotUsed] = source.asJava
}

// --- exceptions ---

final case class TargetRefNotInitializedYetException()
    extends IllegalStateException(
      "Internal remote target actor ref not yet resolved, yet attempted to send messages to it. " +
      "This should not happen due to proper flow-control, please open a ticket on the issue tracker: https://github.com/akka/akka")

final case class StreamRefSubscriptionTimeoutException(msg: String) extends IllegalStateException(msg)

final case class RemoteStreamRefActorTerminatedException(msg: String) extends RuntimeException(msg)
final case class InvalidSequenceNumberException(expectedSeqNr: Long, gotSeqNr: Long, msg: String)
    extends IllegalStateException(
      s"$msg (expected: $expectedSeqNr, got: $gotSeqNr). " +
      s"In most cases this means that message loss on this connection has occurred and the stream will fail eagerly.")

/**
 * Stream refs establish a connection between a local and remote actor, representing the origin and remote sides
 * of a stream. Each such actor refers to the other side as its "partner". We make sure that no other actor than
 * the initial partner can send demand/messages to the other side accidentally.
 *
 * This exception is thrown when a message is received from a non-partner actor,
 * which could mean a bug or some actively malicient behavior from the other side.
 *
 * This is not meant as a security feature, but rather as plain sanity-check.
 */
final case class InvalidPartnerActorException(expectedRef: ActorRef, gotRef: ActorRef, msg: String)
    extends IllegalStateException(
      s"$msg (expected: $expectedRef, got: $gotRef). " +
      s"This may happen due to 'double-materialization' on the other side of this stream ref. " +
      s"Do note that stream refs are one-shot references and have to be paired up in 1:1 pairs. " +
      s"Multi-cast such as broadcast etc can be implemented by sharing multiple new stream references. ")

/**
 * The stream ref resolver extension provides a way to serialize and deserialize streamrefs in user serializers.
 */
object StreamRefResolver extends ExtensionId[StreamRefResolver] {
  override def get(system: ActorSystem): StreamRefResolver = super.get(system)
  override def get(system: ClassicActorSystemProvider): StreamRefResolver = super.get(system)

  override def createExtension(system: ExtendedActorSystem): StreamRefResolver =
    new StreamRefResolverImpl(system)
}

/**
 * The stream ref resolver provides a way to serialize and deserialize streamrefs in user serializers.
 *
 * Not for user extension
 */
@DoNotInherit trait StreamRefResolver extends Extension {

  /**
   * Generate full String representation of the `SourceRef`.
   * This representation should be used as serialized representation.
   */
  def toSerializationFormat[T](ref: SourceRef[T]): String

  /**
   * Generate full String representation of the `SinkRef`.
   * This representation should be used as serialized representation.
   */
  def toSerializationFormat[T](ref: SinkRef[T]): String

  /**
   * Deserialize an `SourceRef` in the [[#toSerializationFormat]].
   */
  def resolveSourceRef[T](serializedSourceRef: String): SourceRef[T]

  /**
   * Deserialize an `SinkRef` in the [[#toSerializationFormat]].
   */
  def resolveSinkRef[T](serializedSinkRef: String): SinkRef[T]
}
