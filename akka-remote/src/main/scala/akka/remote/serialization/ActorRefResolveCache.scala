/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import scala.annotation.nowarn
import scala.reflect.ClassTag
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.EmptyLocalActorRef
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.InternalActorRef
import akka.remote.RemoteActorRef
import akka.remote.RemoteActorRefProvider
import akka.remote.artery.LruBoundedCache
import akka.util.Unsafe

/**
 * INTERNAL API: Thread local cache per actor system
 */
private[akka] object ActorRefResolveThreadLocalCache
    extends ExtensionId[ActorRefResolveThreadLocalCache]
    with ExtensionIdProvider {

  override def get(system: ActorSystem): ActorRefResolveThreadLocalCache = super.get(system)
  override def get(system: ClassicActorSystemProvider): ActorRefResolveThreadLocalCache = super.get(system)

  override def lookup = ActorRefResolveThreadLocalCache

  override def createExtension(system: ExtendedActorSystem): ActorRefResolveThreadLocalCache =
    new ActorRefResolveThreadLocalCache(system)
}

/**
 * INTERNAL API
 */
private[akka] class ActorRefResolveThreadLocalCache(val system: ExtendedActorSystem) extends Extension {

  private val provider = system.provider match {
    case r: RemoteActorRefProvider => r
    case _ =>
      throw new IllegalArgumentException(
        "ActorRefResolveThreadLocalCache can only be used with RemoteActorRefProvider, " +
        s"not with ${system.provider.getClass}")
  }

  private val current = new ThreadLocal[ActorRefResolveCache] {
    override def initialValue: ActorRefResolveCache = new ActorRefResolveCache(provider)
  }

  def threadLocalCache(@nowarn("msg=never used") provider: RemoteActorRefProvider): ActorRefResolveCache =
    current.get

}

/**
 * INTERNAL API
 */
private[akka] final class ActorRefResolveCache(provider: RemoteActorRefProvider)
    extends AbstractActorRefResolveCache[ActorRef] {

  override protected def compute(k: String): ActorRef =
    provider.internalResolveActorRef(k)
}

/**
 * INTERNAL API
 */
private[akka] abstract class AbstractActorRefResolveCache[R <: ActorRef: ClassTag]
    extends LruBoundedCache[String, R](capacity = 1024, evictAgeThreshold = 600) {

  /**
   * Compared to `getOrCompute` this will also invalidate cachedAssociation of RemoteActorRef
   * if the `Association` is removed.
   */
  def resolve(k: String): R = {
    val ref = getOrCompute(k)
    ref match {
      case r: RemoteActorRef =>
        val cachedAssociation = r.cachedAssociation
        if (cachedAssociation != null && cachedAssociation.isRemovedAfterQuarantined())
          r.cachedAssociation = null
      case _ =>
    }
    ref
  }

  override protected def compute(k: String): R

  override protected def hash(k: String): Int = Unsafe.fastHash(k)

  override protected def isKeyCacheable(k: String): Boolean = true
  override protected def isCacheable(ref: R): Boolean =
    ref match {
      case _: EmptyLocalActorRef => false
      case _                     =>
        // "temp" only for one request-response interaction so don't cache
        !InternalActorRef.isTemporaryRef(ref)
    }
}
