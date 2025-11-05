/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.annotation.unchecked.uncheckedVariance

import akka.{ actor => classic }
import akka.actor.typed.internal.InternalRecipientRef
import akka.annotation.DoNotInherit

/**
 * An ActorRef is the identity or address of an Actor instance. It is valid
 * only during the Actor’s lifetime and allows messages to be sent to that
 * Actor instance. Sending a message to an Actor that has terminated before
 * receiving the message will lead to that message being discarded; such
 * messages are delivered to the [[DeadLetter]] channel of the
 * [[akka.event.EventStream]] on a best effort basis
 * (i.e. this delivery is not reliable).
 *
 * Not for user extension
 */
@DoNotInherit
trait ActorRef[-T] extends RecipientRef[T] with java.lang.Comparable[ActorRef[_]] with java.io.Serializable {
  this: InternalRecipientRef[T] =>

  /**
   * Send a message to the Actor referenced by this ActorRef using *at-most-once*
   * messaging semantics.
   */
  def tell(msg: T): Unit

  /**
   * Narrow the type of this `ActorRef`, which is always a safe operation.
   */
  def narrow[U <: T]: ActorRef[U]

  /**
   * Unsafe utility method for widening the type accepted by this ActorRef;
   * provided to avoid having to use `asInstanceOf` on the full reference type,
   * which would unfortunately also work on non-ActorRefs. Use it with caution,it may cause a [[ClassCastException]] when you send a message
   * to the widened [[ActorRef[U]]].
   */
  def unsafeUpcast[U >: T @uncheckedVariance]: ActorRef[U]

  /**
   * The hierarchical path name of the referenced Actor. The lifecycle of the
   * ActorRef is fully contained within the lifecycle of the [[akka.actor.ActorPath]]
   * and more than one Actor instance can exist with the same path at different
   * points in time, but not concurrently.
   */
  def path: classic.ActorPath

  @throws(classOf[java.io.ObjectStreamException])
  private def writeReplace(): AnyRef = SerializedActorRef[T](this)
}

object ActorRef {

  implicit final class ActorRefOps[-T](val ref: ActorRef[T]) extends AnyVal {

    /**
     * Send a message to the Actor referenced by this ActorRef using *at-most-once*
     * messaging semantics.
     */
    def !(msg: T): Unit = ref.tell(msg)
  }
}

/**
 * INTERNAL API
 */
private[akka] object SerializedActorRef {
  def apply[T](actorRef: ActorRef[T]): SerializedActorRef[T] = {
    new SerializedActorRef(actorRef)
  }

  def toAddress[T](actorRef: ActorRef[T]) = {
    import akka.actor.typed.scaladsl.adapter._
    import akka.serialization.JavaSerializer.currentSystem
    val resolver = ActorRefResolver(currentSystem.value.toTyped)
    resolver.toSerializationFormat(actorRef)
  }
}

/**
 * Memento pattern for serializing ActorRefs transparently
 * INTERNAL API
 */
@SerialVersionUID(1L)
private[akka] final case class SerializedActorRef[T] private (address: String) {
  import akka.actor.typed.scaladsl.adapter._
  import akka.serialization.JavaSerializer.currentSystem

  def this(actorRef: ActorRef[T]) =
    this(SerializedActorRef.toAddress(actorRef))

  @throws(classOf[java.io.ObjectStreamException])
  def readResolve(): AnyRef = currentSystem.value match {
    case null =>
      throw new IllegalStateException(
        "Trying to deserialize a serialized typed ActorRef without an ActorSystem in scope." +
        " Use 'akka.serialization.Serialization.currentSystem.withValue(system) { ... }'")
    case someSystem =>
      val resolver = ActorRefResolver(someSystem.toTyped)
      resolver.resolveActorRef(address)
  }
}

/**
 * FIXME doc
 * - not serializable
 * - not watchable
 */
trait RecipientRef[-T] { this: InternalRecipientRef[T] =>

  /**
   * Send a message to the destination referenced by this `RecipientRef` using *at-most-once*
   * messaging semantics.
   */
  def tell(msg: T): Unit
}

object RecipientRef {

  implicit final class RecipientRefOps[-T](val ref: RecipientRef[T]) extends AnyVal {

    /**
     * Send a message to the destination referenced by this `RecipientRef` using *at-most-once*
     * messaging semantics.
     */
    def !(msg: T): Unit = ref.tell(msg)
  }
}
