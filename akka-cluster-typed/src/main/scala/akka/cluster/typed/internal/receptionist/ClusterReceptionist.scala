/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal.receptionist

import scala.concurrent.duration._

import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.internal.receptionist.{ AbstractServiceKey, ReceptionistBehaviorProvider, ReceptionistMessages }
import akka.actor.typed.receptionist.Receptionist.Command
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.cluster.{ Cluster, ClusterEvent, UniqueAddress }
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.ClusterEvent.ClusterShuttingDown
import akka.cluster.ClusterEvent.MemberJoined
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberWeaklyUp
import akka.cluster.ClusterEvent.ReachabilityEvent
import akka.cluster.ClusterEvent.ReachableMember
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ddata.{ ORMultiMap, ORMultiMapKey, Replicator }
import akka.cluster.ddata.SelfUniqueAddress
import akka.util.TypedMultiMap

// just to provide a log class
/** INTERNAL API */
@InternalApi
private[typed] final class ClusterReceptionist

/** INTERNAL API */
@InternalApi
private[typed] object ClusterReceptionist extends ReceptionistBehaviorProvider {

  import ClusterReceptionistProtocol.SubscriptionsKV
  type SubscriptionRegistry = TypedMultiMap[AbstractServiceKey, SubscriptionsKV]
  type DDataKey = ORMultiMapKey[ServiceKey[_], Entry]

  final val EmptyORMultiMap = ORMultiMap.empty[ServiceKey[_], Entry]

  override val name = "clusterReceptionist"

  // values contain system uid to make it possible to discern actors at the same
  // path in different incarnations of a cluster node
  final case class Entry(ref: ActorRef[_], systemUid: Long)(val createdTimestamp: Long) {
    def uniqueAddress(selfAddress: Address): UniqueAddress =
      if (ref.path.address.hasLocalScope) UniqueAddress(selfAddress, systemUid)
      else UniqueAddress(ref.path.address, systemUid)

    override def toString: String =
      s"${ref.path.toString}#${ref.path.uid} @ $systemUid"
  }

  private sealed trait InternalCommand extends Command
  private final case class LocalServiceActorTerminated[T](ref: ActorRef[T]) extends InternalCommand
  private final case class SubscriberTerminated[T](ref: ActorRef[ReceptionistMessages.Listing[T]])
      extends InternalCommand
  private final case class NodeAdded(addresses: UniqueAddress) extends InternalCommand
  private final case class NodeRemoved(addresses: UniqueAddress) extends InternalCommand
  private final case class NodeUnreachable(addresses: UniqueAddress) extends InternalCommand
  private final case class NodeReachable(addresses: UniqueAddress) extends InternalCommand
  private final case class ChangeFromReplicator(key: DDataKey, value: ORMultiMap[ServiceKey[_], Entry])
      extends InternalCommand
  private case object RemoveTick extends InternalCommand
  private case object PruneTombstonesTick extends InternalCommand

  /**
   * @param registry The last seen state from the replicator - only updated when we get an update from th replicator
   * @param servicesPerActor needed since an actor can implement several services
   * @param tombstones Local actors that were stopped and should not be re-added to the available set of actors
   *                   for a key.
   * @param subscriptions Locally subscriptions, not replicated
   */
  final case class State(
      registry: ShardedServiceRegistry,
      servicesPerActor: Map[ActorRef[_], Set[AbstractServiceKey]],
      tombstones: Map[ActorRef[_], Set[(AbstractServiceKey, Deadline)]],
      subscriptions: SubscriptionRegistry) {

    /** tombstone all services actor is registered for */
    def addTombstone(actor: ActorRef[_], deadline: Deadline): State = {
      servicesPerActor.getOrElse(actor, Set.empty).foldLeft(this) { (state, key) =>
        state.addTombstone(actor.asInstanceOf[ActorRef[key.Protocol]], key.asServiceKey, deadline)
      }
    }

    /** tombstone specific service actor is registered for */
    def addTombstone[T](actor: ActorRef[T], serviceKey: ServiceKey[T], deadline: Deadline): State = {
      val newTombsonesForActor = tombstones.getOrElse(actor, Set.empty) + (serviceKey -> deadline)
      copy(tombstones = tombstones.updated(actor, newTombsonesForActor))
    }

    def hasTombstone[T](serviceKey: ServiceKey[T])(actorRef: ActorRef[T]): Boolean =
      tombstones.nonEmpty && tombstones.getOrElse(actorRef, Set.empty).exists { case (key, _) => key == serviceKey }

    def pruneTombstones(): State = {
      if (tombstones.isEmpty) this
      else {
        val newTombstones: Map[ActorRef[_], Set[(AbstractServiceKey, Deadline)]] =
          tombstones.foldLeft(tombstones) {
            case (acc, (actorRef, entries)) =>
              val entriesToKeep = entries.filter {
                case (_, deadline) => deadline.hasTimeLeft()
              }
              if (entriesToKeep.size == entries.size) acc
              else if (entriesToKeep.isEmpty) acc - actorRef
              else acc.updated(actorRef, entriesToKeep)
          }
        if (newTombstones eq tombstones) this
        else copy(tombstones = newTombstones)
      }
    }

    /**
     * @return (reachable-nodes, all)
     */
    def activeActorRefsFor[T](
        key: ServiceKey[T],
        selfUniqueAddress: UniqueAddress): (Set[ActorRef[T]], Set[ActorRef[T]]) = {
      val ddataKey = registry.ddataKeyFor(key)
      val entries = registry.serviceRegistries(ddataKey).entriesFor(key)
      val selfAddress = selfUniqueAddress.address
      val reachable = Set.newBuilder[ActorRef[T]]
      val all = Set.newBuilder[ActorRef[T]]
      entries.foreach { entry =>
        val entryAddress = entry.uniqueAddress(selfAddress)
        val ref = entry.ref.asInstanceOf[ActorRef[key.Protocol]]
        if (registry.nodes.contains(entryAddress) && !hasTombstone(key)(ref)) {
          all += ref
          if (!registry.unreachable.contains(entryAddress)) {
            reachable += ref
          }
        }
      }
      (reachable.result(), all.result())
    }

    def addLocalService[T](serviceInstance: ActorRef[T], key: ServiceKey[T]): State = {
      val newServicesPerActor =
        servicesPerActor.updated(serviceInstance, servicesPerActor.getOrElse(serviceInstance, Set.empty) + key)
      // if the service was previously registered and unregistered we need to remove it from the tombstones
      val tombstonesForActor = tombstones.getOrElse(serviceInstance, Set.empty)
      val newTombstones =
        if (tombstonesForActor.isEmpty) tombstones
        else tombstones.updated(serviceInstance, tombstonesForActor.filterNot(_._1 == key))
      copy(servicesPerActor = newServicesPerActor, tombstones = newTombstones)
    }

    def removeLocalService[T](serviceInstance: ActorRef[T], key: ServiceKey[T], tombstoneDeadline: Deadline): State = {
      val newServicesForActor = servicesPerActor.get(serviceInstance) match {
        case Some(keys) =>
          val newKeys = keys - key
          if (newKeys.isEmpty)
            servicesPerActor - serviceInstance
          else
            servicesPerActor.updated(serviceInstance, newKeys)
        case None =>
          throw new IllegalArgumentException(
            s"Trying to remove $serviceInstance for $key but that has never been registered")
      }
      addTombstone(serviceInstance, key, tombstoneDeadline).copy(servicesPerActor = newServicesForActor)
    }

    def removeSubscriber[T](subscriber: ActorRef[ReceptionistMessages.Listing[T]]): ClusterReceptionist.State =
      copy(subscriptions = subscriptions.valueRemoved(subscriber))

  }

  // captures setup/dependencies so we can avoid doing it over and over again
  final class Setup(ctx: ActorContext[Command]) {
    val classicSystem = ctx.system.toClassic
    val settings = ClusterReceptionistSettings(ctx.system)
    val selfSystemUid = classicSystem.asInstanceOf[ExtendedActorSystem].uid
    lazy val keepTombstonesFor = cluster.settings.PruneGossipTombstonesAfter match {
      case f: FiniteDuration => f
      case _                 => throw new IllegalStateException("Cannot actually happen")
    }
    val cluster = Cluster(classicSystem)
    // don't use DistributedData.selfUniqueAddress here, because that will initialize extension, which
    // isn't used otherwise by the ClusterReceptionist
    implicit val selfNodeAddress: SelfUniqueAddress = SelfUniqueAddress(cluster.selfUniqueAddress)

    val replicator = ctx.actorOf(Replicator.props(settings.replicatorSettings), "replicator")

    def newTombstoneDeadline() = Deadline(keepTombstonesFor)
    def selfUniqueAddress: UniqueAddress = cluster.selfUniqueAddress
  }

  override def behavior: Behavior[Command] =
    Behaviors.setup { ctx =>
      ctx.setLoggerName(classOf[ClusterReceptionist])
      Behaviors.withTimers { timers =>
        val setup = new Setup(ctx)
        // include selfUniqueAddress so that it can be used locally before joining cluster
        val initialRegistry =
          ShardedServiceRegistry(setup.settings.distributedKeyCount).addNode(setup.selfUniqueAddress)

        // subscribe to changes from other nodes
        val replicatorMessageAdapter: ActorRef[Replicator.ReplicatorMessage] =
          ctx.messageAdapter[Replicator.ReplicatorMessage] {
            case changed: Replicator.Changed[_] @unchecked =>
              ChangeFromReplicator(
                changed.key.asInstanceOf[DDataKey],
                changed.dataValue.asInstanceOf[ORMultiMap[ServiceKey[_], Entry]])
            case _ => throw new IllegalArgumentException() // compiler exhaustiveness check pleaser
          }

        initialRegistry.allDdataKeys.foreach(key =>
          setup.replicator ! Replicator.Subscribe(key, replicatorMessageAdapter.toClassic))

        // keep track of cluster members
        // remove entries when members are removed
        val clusterEventMessageAdapter: ActorRef[ClusterDomainEvent] =
          ctx.messageAdapter[ClusterDomainEvent] {
            case MemberJoined(member)      => NodeAdded(member.uniqueAddress)
            case MemberWeaklyUp(member)    => NodeAdded(member.uniqueAddress)
            case MemberUp(member)          => NodeAdded(member.uniqueAddress)
            case MemberRemoved(member, _)  => NodeRemoved(member.uniqueAddress)
            case UnreachableMember(member) => NodeUnreachable(member.uniqueAddress)
            case ReachableMember(member)   => NodeReachable(member.uniqueAddress)
            case ClusterShuttingDown       => NodeRemoved(setup.cluster.selfUniqueAddress)
            case other =>
              throw new IllegalStateException(s"Unexpected ClusterDomainEvent $other. Please report bug.")
          }
        setup.cluster.subscribe(
          clusterEventMessageAdapter.toClassic,
          ClusterEvent.InitialStateAsEvents,
          classOf[MemberJoined],
          classOf[MemberWeaklyUp],
          classOf[MemberUp],
          classOf[MemberRemoved],
          classOf[ReachabilityEvent],
          ClusterShuttingDown.getClass)

        // also periodic cleanup in case removal from ORMultiMap is skipped due to concurrent update,
        // which is possible for OR CRDTs - done with an adapter to leverage the existing NodesRemoved message
        timers.startTimerWithFixedDelay(RemoveTick, setup.settings.pruningInterval)

        // default tombstone keepalive is 24h (based on prune-gossip-tombstones-after) and keeping the actorrefs
        // around isn't very costly so don't prune often
        timers.startTimerWithFixedDelay(PruneTombstonesTick, setup.keepTombstonesFor / 24)

        val initialState = State(
          registry = initialRegistry,
          servicesPerActor = Map.empty,
          tombstones = Map.empty,
          subscriptions = TypedMultiMap.empty[AbstractServiceKey, SubscriptionsKV])
        behavior(setup, initialState)
      }
    }

  def behavior(setup: Setup, state: State): Behavior[Command] =
    Behaviors.setup { ctx =>
      import setup._

      def isLeader = {
        cluster.state.leader.contains(cluster.selfAddress)
      }

      def nodesRemoved(addresses: Set[UniqueAddress], onlyRemoveOldEntries: Boolean): Unit = {
        // ok to update from several nodes but more efficient to try to do it from one node
        def isOnRemovedNode(entry: Entry): Boolean = addresses(entry.uniqueAddress(setup.selfUniqueAddress.address))

        val now = System.currentTimeMillis()

        // it possible that an entry is added before MemberJoined is visible and such entries should not be removed
        def isOld(entry: Entry): Boolean = (now - entry.createdTimestamp) >= settings.pruneRemovedOlderThan.toMillis

        val removals = {
          state.registry.allServices.foldLeft(Map.empty[AbstractServiceKey, Set[Entry]]) {
            case (acc, (key, entries)) =>
              val removedEntries =
                entries.filter(entry => isOnRemovedNode(entry) && (!onlyRemoveOldEntries || isOld(entry)))

              if (removedEntries.isEmpty) acc // no change
              else acc + (key -> removedEntries)
          }
        }

        if (removals.nonEmpty) {
          if (ctx.log.isDebugEnabled)
            ctx.log.debug(
              "ClusterReceptionist [{}] - Node(s) removed [{}], updating registry removing entries: [{}]",
              cluster.selfAddress,
              addresses.mkString(","),
              removals
                .map {
                  case (key, entries) => key.asServiceKey.id -> entries.mkString("[", ", ", "]")
                }
                .mkString(","))

          // shard changes over the ddata keys they belong to
          val removalsPerDdataKey = state.registry.entriesPerDdataKey(removals)

          removalsPerDdataKey.foreach {
            case (ddataKey, removalForKey) =>
              replicator ! Replicator.Update(ddataKey, EmptyORMultiMap, settings.writeConsistency) { registry =>
                ServiceRegistry(registry).removeAll(removalForKey).toORMultiMap
              }
          }

        }
      }

      def reachabilityChanged(keysForNode: Set[AbstractServiceKey], newState: State): Unit = {
        notifySubscribers(keysForNode, servicesWereAddedOrRemoved = false, newState)
      }

      def notifySubscribers(
          changedKeys: Set[AbstractServiceKey],
          servicesWereAddedOrRemoved: Boolean,
          newState: State): Unit = {
        changedKeys.foreach { changedKey =>
          val serviceKey = changedKey.asServiceKey

          val subscribers = newState.subscriptions.get(changedKey)
          if (subscribers.nonEmpty) {
            val (reachable, all) = newState.activeActorRefsFor(serviceKey, selfUniqueAddress)
            val listing =
              ReceptionistMessages.Listing(serviceKey, reachable, all, servicesWereAddedOrRemoved)
            subscribers.foreach(_ ! listing)
          }
        }
      }

      def onCommand(cmd: Command): Behavior[Command] = cmd match {
        case ReceptionistMessages.Register(key, serviceInstance, maybeReplyTo) =>
          if (serviceInstance.path.address.hasLocalScope) {
            val entry = Entry(serviceInstance, setup.selfSystemUid)(System.currentTimeMillis())
            ctx.log.debug("ClusterReceptionist [{}] - Actor was registered: [{}] [{}]", cluster.selfAddress, key, entry)
            // actor already watched after one service key registration
            if (!state.servicesPerActor.contains(serviceInstance))
              ctx.watchWith(serviceInstance, LocalServiceActorTerminated(serviceInstance))

            maybeReplyTo match {
              case Some(replyTo) => replyTo ! ReceptionistMessages.Registered(key, serviceInstance)
              case None          =>
            }
            val ddataKey = state.registry.ddataKeyFor(key)
            replicator ! Replicator.Update(ddataKey, EmptyORMultiMap, settings.writeConsistency) { registry =>
              ServiceRegistry(registry).addBinding(key, entry).toORMultiMap
            }
            behavior(setup, state.addLocalService(serviceInstance, key))
          } else {
            ctx.log.error("ClusterReceptionist [{}] - Register of non-local [{}] is not supported", serviceInstance)
            Behaviors.same
          }

        case ReceptionistMessages.Deregister(key, serviceInstance, maybeReplyTo) =>
          if (serviceInstance.path.address.hasLocalScope) {
            val entry = Entry(serviceInstance, setup.selfSystemUid)(0L)
            ctx.log.debug(
              "ClusterReceptionist [{}] - Unregister actor: [{}] [{}]",
              cluster.selfAddress,
              key.asServiceKey.id,
              entry)
            val newState = state.removeLocalService(serviceInstance, key, setup.newTombstoneDeadline())
            if (!newState.servicesPerActor.contains(serviceInstance)) {
              // last service for actor unregistered, stop watching
              ctx.unwatch(serviceInstance)
            }
            maybeReplyTo match {
              case Some(replyTo) => replyTo ! ReceptionistMessages.Deregistered(key, serviceInstance)
              case None          =>
            }
            val ddataKey = state.registry.ddataKeyFor(key)
            replicator ! Replicator.Update(ddataKey, EmptyORMultiMap, settings.writeConsistency) { registry =>
              ServiceRegistry(registry).removeBinding(key, entry).toORMultiMap
            }
            // tombstone removals so they are not re-added by merging with other concurrent
            // registrations for the same key
            behavior(setup, newState)
          } else {
            ctx.log.error("ClusterReceptionist [{}] - Unregistering non-local [{}] is not supported", serviceInstance)
            Behaviors.same
          }

        case ReceptionistMessages.Find(key, replyTo) =>
          val (reachable, all) = state.activeActorRefsFor(key, selfUniqueAddress)
          replyTo ! ReceptionistMessages.Listing(key.asServiceKey, reachable, all, servicesWereAddedOrRemoved = true)
          Behaviors.same

        case ReceptionistMessages.Subscribe(key, subscriber) =>
          if (subscriber.path.address.hasLocalScope) {
            ctx.watchWith(subscriber, SubscriberTerminated(subscriber))

            // immediately reply with initial listings to the new subscriber
            val listing = {
              val (reachable, all) = state.activeActorRefsFor(key, selfUniqueAddress)
              ReceptionistMessages.Listing(key.asServiceKey, reachable, all, servicesWereAddedOrRemoved = true)
            }
            subscriber ! listing

            behavior(setup, state.copy(subscriptions = state.subscriptions.inserted(key)(subscriber)))
          } else {
            ctx.log.error("ClusterReceptionist [{}] - Subscriptions from non-local [{}] is not supported", subscriber)
            Behaviors.same
          }

        case _ =>
          throw new IllegalArgumentException() // to please exhaustiveness check, compiler does not know about internal/public command

      }

      def onInternalCommand(cmd: InternalCommand): Behavior[Command] = cmd match {

        case SubscriberTerminated(subscriber) =>
          behavior(setup, state.removeSubscriber(subscriber))

        case LocalServiceActorTerminated(serviceInstance) =>
          val entry = Entry(serviceInstance, setup.selfSystemUid)(0L)

          // could be empty if there was a race between termination and unregistration
          val keys = state.servicesPerActor.getOrElse(serviceInstance, Set.empty)

          ctx.log.debug(
            "ClusterReceptionist [{}] - Registered actor terminated: [{}] [{}]",
            cluster.selfAddress,
            keys.map(_.asServiceKey.id).mkString(", "),
            entry)

          keys.foreach { key =>
            val ddataKey = state.registry.ddataKeyFor(key.asServiceKey)
            replicator ! Replicator.Update(ddataKey, EmptyORMultiMap, settings.writeConsistency) { registry =>
              ServiceRegistry(registry).removeBinding(key.asServiceKey, entry).toORMultiMap
            }
          }
          // tombstone removals so they are not re-added by merging with other concurrent
          // registrations for the same key
          behavior(setup, state.addTombstone(serviceInstance, setup.newTombstoneDeadline()))

        case ChangeFromReplicator(ddataKey, value) =>
          // every change will come back this way - this is where the local notifications happens
          val newRegistry = ServiceRegistry(value)
          val changedKeys = state.registry.collectChangedKeys(ddataKey, newRegistry)
          val newState = state.copy(registry = state.registry.withServiceRegistry(ddataKey, newRegistry))

          if (changedKeys.nonEmpty) {
            if (ctx.log.isDebugEnabled) {
              ctx.log.debug(
                "ClusterReceptionist [{}] - Change from replicator: [{}], changes: [{}], tombstones [{}]",
                cluster.selfAddress,
                newRegistry.entries.entries,
                changedKeys
                  .map(key => key.asServiceKey.id -> newRegistry.entriesFor(key).mkString("[", ", ", "]"))
                  .mkString(", "),
                state.tombstones.mkString(", "))
            }

            notifySubscribers(changedKeys, servicesWereAddedOrRemoved = true, newState)

            changedKeys.foreach { changedKey =>
              val serviceKey = changedKey.asServiceKey

              // because of how ORMultiMap/ORset works, we could have a case where an actor we removed
              // is re-introduced because of a concurrent update, in that case we need to re-remove it
              val tombstonedButReAdded = newRegistry.actorRefsFor(serviceKey).filter(state.hasTombstone(serviceKey))
              if (tombstonedButReAdded.nonEmpty) {
                if (ctx.log.isDebugEnabled)
                  ctx.log.debug(
                    "ClusterReceptionist [{}] - Saw ActorRefs that were tomstoned [{}], re-removing.",
                    cluster.selfAddress,
                    tombstonedButReAdded.mkString(", "))

                replicator ! Replicator.Update(ddataKey, EmptyORMultiMap, settings.writeConsistency) { registry =>
                  tombstonedButReAdded
                    .foldLeft(ServiceRegistry(registry)) { (acc, ref) =>
                      acc.removeBinding(serviceKey, Entry(ref, setup.selfSystemUid)(0L))
                    }
                    .toORMultiMap
                }
              }
            }

            behavior(setup, newState)
          } else {
            Behaviors.same
          }

        case NodeAdded(uniqueAddress) =>
          if (state.registry.nodes.contains(uniqueAddress)) {
            Behaviors.same
          } else {
            val newState = state.copy(registry = state.registry.addNode(uniqueAddress))
            val keysForNode = newState.registry.keysFor(uniqueAddress)
            if (keysForNode.nonEmpty) {
              ctx.log.debug(
                "ClusterReceptionist [{}] - Node with registered services added [{}]",
                cluster.selfAddress,
                uniqueAddress)
              notifySubscribers(keysForNode, servicesWereAddedOrRemoved = true, newState)
            } else {
              ctx.log.debug("ClusterReceptionist [{}] - Node added [{}]", cluster.selfAddress, uniqueAddress)
            }

            behavior(setup, newState)
          }

        case NodeRemoved(uniqueAddress) =>
          if (uniqueAddress == selfUniqueAddress) {
            ctx.log.debug("ClusterReceptionist [{}] - terminated/removed", cluster.selfAddress)
            // If self cluster node is shutting down our own entries should have been removed via
            // watch-Terminated or will be removed by other nodes. This point is anyway too late.
            Behaviors.stopped
          } else if (state.registry.nodes.contains(uniqueAddress)) {

            val keysForNode = state.registry.keysFor(uniqueAddress)
            val newState = state.copy(registry = state.registry.removeNode(uniqueAddress))
            if (keysForNode.nonEmpty) {
              ctx.log.debug(
                "ClusterReceptionist [{}] - Node with registered services removed [{}]",
                cluster.selfAddress,
                uniqueAddress)
              notifySubscribers(keysForNode, servicesWereAddedOrRemoved = true, newState)
            }

            // Ok to update from several nodes but more efficient to try to do it from one node.
            if (isLeader) {
              ctx.log.debug(
                "ClusterReceptionist [{}] - Leader node observed removed node [{}]",
                cluster.selfAddress,
                uniqueAddress)
              nodesRemoved(Set(uniqueAddress), onlyRemoveOldEntries = false)
            }

            behavior(setup, newState)
          } else {
            Behaviors.same
          }

        case NodeUnreachable(uniqueAddress) =>
          val keysForNode = state.registry.keysFor(uniqueAddress)
          val newState = state.copy(registry = state.registry.addUnreachable(uniqueAddress))
          if (keysForNode.nonEmpty) {
            ctx.log.debug(
              "ClusterReceptionist [{}] - Node with registered services unreachable [{}]",
              cluster.selfAddress,
              uniqueAddress)
            reachabilityChanged(keysForNode, newState)
          }
          behavior(setup, newState)

        case NodeReachable(uniqueAddress) =>
          val keysForNode = state.registry.keysFor(uniqueAddress)
          val newState = state.copy(registry = state.registry.removeUnreachable(uniqueAddress))
          if (keysForNode.nonEmpty) {
            ctx.log.debug(
              "ClusterReceptionist [{}] - Node with registered services reachable again [{}]",
              cluster.selfAddress,
              uniqueAddress)
            reachabilityChanged(keysForNode, newState)
          }
          behavior(setup, newState)

        case RemoveTick =>
          // ok to update from several nodes but more efficient to try to do it from one node
          if (isLeader) {
            val allAddressesInState: Set[UniqueAddress] =
              state.registry.allUniqueAddressesInState(setup.selfUniqueAddress)
            val notInCluster = allAddressesInState.diff(state.registry.nodes)

            if (notInCluster.nonEmpty) {
              if (ctx.log.isDebugEnabled)
                ctx.log.debug(
                  "ClusterReceptionist [{}] - Leader node cleanup tick, removed nodes: [{}]",
                  cluster.selfAddress,
                  notInCluster.mkString(","))
              nodesRemoved(notInCluster, onlyRemoveOldEntries = true)
            }
          }
          Behaviors.same

        case PruneTombstonesTick =>
          val prunedState = state.pruneTombstones()
          if (prunedState eq state) Behaviors.same
          else {
            ctx.log.debug("ClusterReceptionist [{}] - Pruning tombstones", cluster.selfAddress)
            behavior(setup, prunedState)
          }
      }

      Behaviors.receive[Command] { (_, msg) =>
        msg match {
          // support two heterogeneous types of messages without union types
          case cmd: InternalCommand => onInternalCommand(cmd)
          case cmd: Command         => onCommand(cmd)
          case null                 => Behaviors.unhandled
        }
      }
    }

}
