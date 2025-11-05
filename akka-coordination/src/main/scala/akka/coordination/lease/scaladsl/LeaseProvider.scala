/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease.scaladsl

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{ Function => JFunction }

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.coordination.lease.LeaseSettings
import akka.coordination.lease.internal.LeaseAdapterToScala
import akka.event.Logging

object LeaseProvider extends ExtensionId[LeaseProvider] with ExtensionIdProvider {
  override def get(system: ActorSystem): LeaseProvider = super.get(system)
  override def get(system: ClassicActorSystemProvider): LeaseProvider = super.get(system)

  override def lookup = LeaseProvider

  override def createExtension(system: ExtendedActorSystem): LeaseProvider = new LeaseProvider(system)

  private final case class LeaseKey(leaseName: String, configPath: String, clientName: String)
}

final class LeaseProvider(system: ExtendedActorSystem) extends Extension {
  import LeaseProvider.LeaseKey

  private val log = Logging(system, classOf[LeaseProvider])
  private val leases = new ConcurrentHashMap[LeaseKey, Lease]()

  /**
   * The configuration define at `configPath` must have a property `lease-class` that defines
   * the fully qualified class name of the Lease implementation.
   * The class must implement [[Lease]] and have constructor with [[akka.coordination.lease.LeaseSettings]] parameter and
   * optionally ActorSystem parameter.
   *
   * @param leaseName the name of the lease resource
   * @param configPath the path of configuration for the lease
   * @param ownerName the owner that will `acquire` the lease, e.g. hostname and port of the ActorSystem
   */
  def getLease(leaseName: String, configPath: String, ownerName: String): Lease = {
    internalGetLease(leaseName, configPath, ownerName)
  }

  private[akka] def internalGetLease(leaseName: String, configPath: String, ownerName: String): Lease = {
    val leaseKey = LeaseKey(leaseName, configPath, ownerName)
    leases.computeIfAbsent(
      leaseKey,
      new JFunction[LeaseKey, Lease] {
        override def apply(t: LeaseKey): Lease = {
          val leaseConfig = system.settings.config
            .getConfig(configPath)
            .withFallback(system.settings.config.getConfig("akka.coordination.lease"))

          val settings = LeaseSettings(leaseConfig, leaseName, ownerName)

          // Try and load a scala implementation
          val lease: Try[Lease] =
            loadLease[Lease](settings).recoverWith {
              case _: ClassCastException =>
                // Try and load a java implementation
                loadLease[akka.coordination.lease.javadsl.Lease](settings).map(javaLease =>
                  new LeaseAdapterToScala(javaLease)(system.dispatchers.internalDispatcher))
            }

          lease match {
            case Success(value) => value
            case Failure(e) =>
              log.error(
                e,
                "Invalid lease configuration for leaseName [{}], configPath [{}] lease-class [{}]. " +
                "The class must implement scaladsl.Lease or javadsl.Lease and have constructor with LeaseSettings parameter and " +
                "optionally ActorSystem parameter.",
                settings.leaseName,
                configPath,
                settings.leaseConfig.getString("lease-class"))
              throw e
          }
        }
      })
  }

  /**
   * The Lease types are separate for Java and Scala and A java lease needs to be loadable
   * from Scala and vice versa as leases can be in libraries and user should not care what
   * language it is implemented in.
   */
  private def loadLease[T: ClassTag](leaseSettings: LeaseSettings): Try[T] = {
    val fqcn = leaseSettings.leaseConfig.getString("lease-class")
    require(fqcn.nonEmpty, "lease-class must not be empty")
    val dynamicAccess = system.dynamicAccess
    dynamicAccess.createInstanceFor[T](
      fqcn,
      immutable.Seq((classOf[LeaseSettings], leaseSettings), (classOf[ExtendedActorSystem], system))) match {
      case s: Success[T] =>
        s
      case Failure(_: NoSuchMethodException) =>
        dynamicAccess.createInstanceFor[T](fqcn, immutable.Seq((classOf[LeaseSettings], leaseSettings)))
      case f: Failure[_] =>
        f
    }

  }
}
