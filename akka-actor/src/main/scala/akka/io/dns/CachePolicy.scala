/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns

import scala.concurrent.duration.{ Duration, FiniteDuration, _ }
import scala.jdk.DurationConverters._

import akka.annotation.InternalApi

object CachePolicy {

  sealed trait CachePolicy
  case object Never extends CachePolicy
  case object Forever extends CachePolicy

  final class Ttl private (val value: FiniteDuration) extends CachePolicy {
    if (value < Duration.Zero)
      throw new IllegalArgumentException(s"TTL values must be a positive value (zero included).")
    import scala.jdk.DurationConverters._
    def getValue: java.time.Duration = value.toJava

    override def equals(other: Any): Boolean = other match {
      case that: Ttl => value == that.value
      case _         => false
    }

    override def hashCode(): Int = value.hashCode()

    override def toString = s"Ttl($value)"
  }

  object Ttl {
    def unapply(ttl: Ttl): Option[FiniteDuration] = Some(ttl.value)
    def fromPositive(value: FiniteDuration): Ttl = {
      if (value <= Duration.Zero)
        throw new IllegalArgumentException(
          s"Positive TTL values must be a strictly positive value. Use Ttl.never for zero.")
      new Ttl(value)
    }
    def fromPositive(value: java.time.Duration): Ttl = fromPositive(value.toScala)

    /**
     * INTERNAL API
     */
    @InternalApi
    private[akka] def toTll(policy: CachePolicy): Ttl = policy match {
      case Never    => Ttl.never
      case Forever  => Ttl.effectivelyForever
      case ttl: Ttl => ttl
    }

    // DNS RFC states that zero values are interpreted to mean that the RR should not be cached
    val never: Ttl = new Ttl(0.seconds)

    // There's places where only a Ttl makes sense (DNS RFC says TTL is a positive 32 bit integer)
    // but we know the value can be cached effectively forever (e.g. the Lookup name was the actual IP already)
    val effectivelyForever: Ttl = fromPositive(Int.MaxValue.seconds)

    implicit object TtlIsOrdered extends Ordering[Ttl] {
      def compare(a: Ttl, b: Ttl): Int = a.value.compare(b.value)
    }
  }

}
