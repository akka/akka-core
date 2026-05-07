/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package internal
package adapter

import akka.{ actor => classic }
import akka.actor.ActorRefProvider
import akka.actor.InvalidMessageException
import akka.annotation.InternalApi
import akka.dispatch.sysmsg

/**
 * INTERNAL API
 */
@InternalApi private[typed] class ActorRefAdapter[-T](val classicRef: classic.InternalActorRef)
    extends ActorRef[T]
    with internal.ActorRefImpl[T]
    with internal.InternalRecipientRef[T] {

  override def path: classic.ActorPath = classicRef.path

  override def tell(msg: T): Unit = {
    if (msg == null) throw new InvalidMessageException("[null] is not an allowed message")
    classicRef ! msg
  }

  // impl ActorRefImpl
  override def isLocal: Boolean = classicRef.isLocal
  // impl ActorRefImpl
  override def sendSystem(signal: internal.SystemMessage): Unit =
    ActorRefAdapter.sendSystemMessage(classicRef, signal)

  // impl InternalRecipientRef
  override def provider: ActorRefProvider = classicRef.provider
  // impl InternalRecipientRef
  def isTerminated: Boolean = classicRef.isTerminated

  override def refPrefix: String = path.name

  @throws(classOf[java.io.ObjectStreamException])
  private def writeReplace(): AnyRef = SerializedActorRef[T](this)
}

private[akka] object ActorRefAdapter {
  def apply[T](ref: classic.ActorRef): ActorRef[T] = new ActorRefAdapter(ref.asInstanceOf[classic.InternalActorRef])

  def toClassic[U](ref: ActorRef[U]): akka.actor.InternalActorRef =
    ref match {
      case adapter: ActorRefAdapter[_]    => adapter.classicRef
      case adapter: ActorSystemAdapter[_] => adapter.system.guardian
      case _ =>
        throw new UnsupportedOperationException(
          "Only adapted classic ActorRefs permissible " +
          s"($ref of class ${ref.getClass.getName})")
    }

  def sendSystemMessage(classicRef: akka.actor.InternalActorRef, signal: internal.SystemMessage): Unit =
    signal match {
      case internal.Create()    => throw new IllegalStateException("WAT? No, seriously.")
      case internal.Terminate() => classicRef.stop()
      case internal.Watch(watchee, watcher) =>
        classicRef.sendSystemMessage(sysmsg.Watch(toClassic(watchee), toClassic(watcher)))
      case internal.Unwatch(watchee, watcher) =>
        classicRef.sendSystemMessage(sysmsg.Unwatch(toClassic(watchee), toClassic(watcher)))
      case internal.DeathWatchNotification(ref, _) =>
        classicRef.sendSystemMessage(
          sysmsg.DeathWatchNotification(toClassic(ref), existenceConfirmed = true, addressTerminated = false))
      case internal.NoMessage => // just to suppress the warning
    }
}
