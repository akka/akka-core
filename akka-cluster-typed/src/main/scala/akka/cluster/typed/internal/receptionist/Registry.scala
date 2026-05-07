/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal.receptionist

import akka.actor.typed.ActorRef
import akka.actor.typed.internal.receptionist.AbstractServiceKey
import akka.actor.typed.receptionist.ServiceKey
import akka.annotation.InternalApi
import akka.cluster.UniqueAddress
import akka.cluster.ddata.{ ORMultiMap, ORMultiMapKey, SelfUniqueAddress }
import akka.cluster.typed.internal.receptionist.ClusterReceptionist.{ DDataKey, EmptyORMultiMap, Entry }

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ShardedServiceRegistry {
  def apply(numberOfKeys: Int): ShardedServiceRegistry = {
    val emptyRegistries = (0 until numberOfKeys).map { n =>
      val key = ORMultiMapKey[ServiceKey[_], Entry](s"ReceptionistKey_$n")
      key -> new ServiceRegistry(EmptyORMultiMap)
    }.toMap
    new ShardedServiceRegistry(emptyRegistries, Set.empty, Set.empty)
  }

}

/**
 * INTERNAL API
 *
 * Two level structure for keeping service registry to be able to shard entries over multiple ddata keys (to not
 * get too large ddata messages)
 *

 *
 */
@InternalApi private[akka] final case class ShardedServiceRegistry(
    serviceRegistries: Map[DDataKey, ServiceRegistry],
    nodes: Set[UniqueAddress],
    unreachable: Set[UniqueAddress]) {

  private val keys = serviceRegistries.keySet.toArray

  def registryFor(ddataKey: DDataKey): ServiceRegistry = serviceRegistries(ddataKey)

  def allDdataKeys: Iterable[DDataKey] = keys

  def ddataKeyFor(serviceKey: ServiceKey[_]): DDataKey =
    keys(math.abs(serviceKey.id.hashCode() % serviceRegistries.size))

  def allServices: Iterator[(ServiceKey[_], Set[Entry])] =
    serviceRegistries.valuesIterator.flatMap(_.entries.entries)

  def allEntries: Iterator[Entry] = allServices.flatMap(_._2)

  def actorRefsFor[T](key: ServiceKey[T]): Set[ActorRef[T]] = {
    val ddataKey = ddataKeyFor(key)
    serviceRegistries(ddataKey).actorRefsFor(key)
  }

  /**
   * @return keys that has a registered service instance on the given `address`
   */
  def keysFor(address: UniqueAddress)(implicit node: SelfUniqueAddress): Set[AbstractServiceKey] =
    serviceRegistries.valuesIterator.flatMap(_.keysFor(address)).toSet

  def withServiceRegistry(ddataKey: DDataKey, registry: ServiceRegistry): ShardedServiceRegistry =
    copy(serviceRegistries + (ddataKey -> registry))

  def allUniqueAddressesInState(selfUniqueAddress: UniqueAddress): Set[UniqueAddress] =
    allEntries.collect {
      // we don't care about local (empty host:port addresses)
      case entry if entry.ref.path.address.hasGlobalScope =>
        entry.uniqueAddress(selfUniqueAddress.address)
    }.toSet

  def collectChangedKeys(ddataKey: DDataKey, newRegistry: ServiceRegistry): Set[AbstractServiceKey] = {
    val previousRegistry = registryFor(ddataKey)
    ServiceRegistry.collectChangedKeys(previousRegistry, newRegistry)
  }

  def entriesPerDdataKey(
      entries: Map[AbstractServiceKey, Set[Entry]]): Map[DDataKey, Map[AbstractServiceKey, Set[Entry]]] =
    entries.foldLeft(Map.empty[DDataKey, Map[AbstractServiceKey, Set[Entry]]]) {
      case (acc, (key, entries)) =>
        val ddataKey = ddataKeyFor(key.asServiceKey)
        val updated = acc.getOrElse(ddataKey, Map.empty) + (key -> entries)
        acc + (ddataKey -> updated)
    }

  def addNode(node: UniqueAddress): ShardedServiceRegistry =
    copy(nodes = nodes + node)

  def removeNode(node: UniqueAddress): ShardedServiceRegistry =
    copy(nodes = nodes - node, unreachable = unreachable - node)

  def addUnreachable(uniqueAddress: UniqueAddress): ShardedServiceRegistry =
    copy(unreachable = unreachable + uniqueAddress)

  def removeUnreachable(uniqueAddress: UniqueAddress): ShardedServiceRegistry =
    copy(unreachable = unreachable - uniqueAddress)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class ServiceRegistry(entries: ORMultiMap[ServiceKey[_], Entry]) extends AnyVal {

  // let's hide all the ugly casts we can in here
  def actorRefsFor[T](key: AbstractServiceKey): Set[ActorRef[key.Protocol]] =
    entriesFor(key).map(_.ref.asInstanceOf[ActorRef[key.Protocol]])

  def entriesFor(key: AbstractServiceKey): Set[Entry] =
    entries.getOrElse(key.asServiceKey, Set.empty[Entry])

  def keysFor(address: UniqueAddress)(implicit node: SelfUniqueAddress): Set[ServiceKey[_]] =
    entries.entries.collect {
      case (key, entriesForKey) if entriesForKey.exists(_.uniqueAddress(node.uniqueAddress.address) == address) =>
        key
    }.toSet

  def addBinding[T](key: ServiceKey[T], value: Entry)(implicit node: SelfUniqueAddress): ServiceRegistry =
    copy(entries = entries.addBinding(node, key, value))

  def removeBinding[T](key: ServiceKey[T], value: Entry)(implicit node: SelfUniqueAddress): ServiceRegistry =
    copy(entries = entries.removeBinding(node, key, value))

  def removeAll(entries: Map[AbstractServiceKey, Set[Entry]])(implicit node: SelfUniqueAddress): ServiceRegistry = {
    entries.foldLeft(this) {
      case (acc, (key, entries)) =>
        entries.foldLeft(acc) {
          case (innerAcc, entry) =>
            innerAcc.removeBinding[key.Protocol](key.asServiceKey, entry)
        }
    }
  }

  def toORMultiMap: ORMultiMap[ServiceKey[_], Entry] = entries

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ServiceRegistry {
  final val Empty = ServiceRegistry(EmptyORMultiMap)

  def collectChangedKeys(previousRegistry: ServiceRegistry, newRegistry: ServiceRegistry): Set[AbstractServiceKey] = {
    val allKeys = previousRegistry.toORMultiMap.entries.keySet ++ newRegistry.toORMultiMap.entries.keySet
    allKeys.foldLeft(Set.empty[AbstractServiceKey]) { (acc, key) =>
      val oldValues = previousRegistry.entriesFor(key)
      val newValues = newRegistry.entriesFor(key)
      if (oldValues != newValues) acc + key
      else acc
    }
  }
}
