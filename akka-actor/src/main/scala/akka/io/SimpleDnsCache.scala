/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable

import akka.actor.NoSerializationVerificationNeeded
import akka.annotation.InternalApi
import akka.io.dns.{ AAAARecord, ARecord }
import akka.io.dns.CachePolicy.CachePolicy
import akka.io.dns.CachePolicy.Forever
import akka.io.dns.CachePolicy.Never
import akka.io.dns.CachePolicy.Ttl
import akka.io.dns.DnsProtocol
import akka.io.dns.DnsProtocol.{ Ip, RequestType, Resolved }

private[io] trait PeriodicCacheCleanup {
  def cleanup(): Unit
}

class SimpleDnsCache extends Dns with PeriodicCacheCleanup with NoSerializationVerificationNeeded {
  import SimpleDnsCache._
  private val cacheRef = new AtomicReference(
    new Cache[(String, RequestType), Resolved](
      immutable.SortedSet()(expiryEntryOrdering()),
      immutable.Map(),
      () => clock()))

  private val nanoBase = System.nanoTime()

  /**
   * Gets any IPv4 and IPv6 cached entries.
   * To get Srv or just one type use DnsProtocol
   *
   * This method is deprecated and involves a copy from the new protocol to
   * remain compatible
   */
  @nowarn("msg=deprecated")
  override def cached(name: String): Option[Dns.Resolved] = {
    // adapt response to the old protocol
    val ipv4 = cacheRef.get().get((name, Ip(ipv6 = false))).toList.flatMap(_.records)
    val ipv6 = cacheRef.get().get((name, Ip(ipv4 = false))).toList.flatMap(_.records)
    val both = cacheRef.get().get((name, Ip())).toList.flatMap(_.records)
    val all = (ipv4 ++ ipv6 ++ both).collect {
      case r: ARecord    => r.ip
      case r: AAAARecord => r.ip
    }
    if (all.isEmpty) None
    else Some(Dns.Resolved(name, all))
  }

  override def cached(request: DnsProtocol.Resolve): Option[DnsProtocol.Resolved] =
    cacheRef.get().get((request.name, request.requestType))

  // Milliseconds since start
  protected def clock(): Long = {
    val now = System.nanoTime()
    if (now - nanoBase < 0) 0
    else (now - nanoBase) / 1000000
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final def get(key: (String, RequestType)): Option[Resolved] = {
    cacheRef.get().get(key)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  @tailrec
  private[akka] final def put(key: (String, RequestType), records: Resolved, ttl: CachePolicy): Unit = {
    val c = cacheRef.get()
    if (!cacheRef.compareAndSet(c, c.put(key, records, ttl)))
      put(key, records, ttl)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final def drop(key: (String, RequestType)): Unit = {
    val c = cacheRef.get()
    if (!cacheRef.compareAndSet(c, c.drop(key)))
      drop(key)
  }

  @tailrec
  override final def cleanup(): Unit = {
    val c = cacheRef.get()
    if (!cacheRef.compareAndSet(c, c.cleanup()))
      cleanup()
  }

}
object SimpleDnsCache {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[io] class Cache[K, V](
      queue: immutable.SortedSet[ExpiryEntry[K]],
      cache: immutable.Map[K, CacheEntry[V]],
      clock: () => Long) {
    def get(name: K): Option[V] = {
      for {
        e <- cache.get(name)
        if e.isValid(clock())
      } yield e.answer
    }

    def put(name: K, answer: V, ttl: CachePolicy): Cache[K, V] = {
      val until = ttl match {
        case Forever  => Long.MaxValue
        case Never    => clock() - 1
        case ttl: Ttl => clock() + ttl.value.toMillis
      }

      new Cache[K, V](queue + new ExpiryEntry[K](name, until), cache + (name -> CacheEntry(answer, until)), clock)
    }

    def drop(name: K): Cache[K, V] = {
      new Cache(queue.filterNot(_.name == name), cache.filterNot { case (key, _) => key == name }, clock)
    }

    def cleanup(): Cache[K, V] = {
      val now = clock()
      var q = queue
      var c = cache
      while (q.nonEmpty && !q.head.isValid(now)) {
        val minEntry = q.head
        val name = minEntry.name
        q -= minEntry
        if (c.get(name).filterNot(_.isValid(now)).isDefined)
          c -= name
      }
      new Cache(q, c, clock)
    }
  }

  private[io] case class CacheEntry[T](answer: T, until: Long) {
    def isValid(clock: Long): Boolean = clock < until
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[io] class ExpiryEntry[K](val name: K, val until: Long) extends Ordered[ExpiryEntry[K]] {
    def isValid(clock: Long): Boolean = clock < until
    override def compare(that: ExpiryEntry[K]): Int = -until.compareTo(that.until)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[io] def expiryEntryOrdering[K]() = new Ordering[ExpiryEntry[K]] {
    override def compare(x: ExpiryEntry[K], y: ExpiryEntry[K]): Int = {
      x.until.compareTo(y.until)
    }
  }
}
