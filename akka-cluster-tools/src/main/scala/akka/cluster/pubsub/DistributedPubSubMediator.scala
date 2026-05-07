/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.pubsub

import java.net.URLDecoder
import java.net.URLEncoder
import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.collection.immutable.Set
import scala.collection.immutable.TreeMap
import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.actor._
import akka.annotation.DoNotInherit
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.routing.ActorRefRoutee
import akka.routing.BroadcastRoutingLogic
import akka.routing.ConsistentHashingRoutingLogic
import akka.routing.RandomRoutingLogic
import akka.routing.RoundRobinRoutingLogic
import akka.routing.Routee
import akka.routing.Router
import akka.routing.RouterEnvelope
import akka.routing.RoutingLogic

object DistributedPubSubSettings {

  /**
   * Create settings from the default configuration
   * `akka.cluster.pub-sub`.
   */
  def apply(system: ActorSystem): DistributedPubSubSettings =
    apply(system.settings.config.getConfig("akka.cluster.pub-sub"))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.pub-sub`.
   */
  def apply(config: Config): DistributedPubSubSettings =
    new DistributedPubSubSettings(
      role = roleOption(config.getString("role")),
      routingLogic = config.getString("routing-logic") match {
        case "random"      => RandomRoutingLogic()
        case "round-robin" => RoundRobinRoutingLogic()
        case "consistent-hashing" =>
          throw new IllegalArgumentException(
            s"'consistent-hashing' routing logic can't be used by the pub-sub mediator")
        case "broadcast" => BroadcastRoutingLogic()
        case other       => throw new IllegalArgumentException(s"Unknown 'routing-logic': [$other]")
      },
      gossipInterval = config.getDuration("gossip-interval", MILLISECONDS).millis,
      removedTimeToLive = config.getDuration("removed-time-to-live", MILLISECONDS).millis,
      maxDeltaElements = config.getInt("max-delta-elements"),
      sendToDeadLettersWhenNoSubscribers = config.getBoolean("send-to-dead-letters-when-no-subscribers"))

  /**
   * Java API: Create settings from the default configuration
   * `akka.cluster.pub-sub`.
   */
  def create(system: ActorSystem): DistributedPubSubSettings = apply(system)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.pub-sub`.
   */
  def create(config: Config): DistributedPubSubSettings = apply(config)

  /**
   * INTERNAL API
   */
  private[akka] def roleOption(role: String): Option[String] =
    if (role == "") None else Option(role)
}

/**
 * @param role Start the mediator on members tagged with this role.
 *   All members are used if undefined.
 * @param routingLogic The routing logic to use for `Send`.
 * @param gossipInterval How often the DistributedPubSubMediator should send out gossip information
 * @param removedTimeToLive Removed entries are pruned after this duration
 * @param maxDeltaElements Maximum number of elements to transfer in one message when synchronizing
 *   the registries. Next chunk will be transferred in next round of gossip.
 * @param sendToDeadLettersWhenNoSubscribers When a message is published to a topic with no subscribers send it to the dead letters.
 */
final class DistributedPubSubSettings(
    val role: Option[String],
    val routingLogic: RoutingLogic,
    val gossipInterval: FiniteDuration,
    val removedTimeToLive: FiniteDuration,
    val maxDeltaElements: Int,
    val sendToDeadLettersWhenNoSubscribers: Boolean)
    extends NoSerializationVerificationNeeded {

  @deprecated("Use the other constructor instead.", "2.5.5")
  def this(
      role: Option[String],
      routingLogic: RoutingLogic,
      gossipInterval: FiniteDuration,
      removedTimeToLive: FiniteDuration,
      maxDeltaElements: Int) =
    this(
      role,
      routingLogic,
      gossipInterval,
      removedTimeToLive,
      maxDeltaElements,
      sendToDeadLettersWhenNoSubscribers = true)

  require(
    !routingLogic.isInstanceOf[ConsistentHashingRoutingLogic],
    "'ConsistentHashingRoutingLogic' can't be used by the pub-sub mediator")

  def withRole(role: String): DistributedPubSubSettings = copy(role = DistributedPubSubSettings.roleOption(role))

  def withRole(role: Option[String]): DistributedPubSubSettings = copy(role = role)

  def withRoutingLogic(routingLogic: RoutingLogic): DistributedPubSubSettings =
    copy(routingLogic = routingLogic)

  def withGossipInterval(gossipInterval: FiniteDuration): DistributedPubSubSettings =
    copy(gossipInterval = gossipInterval)

  def withRemovedTimeToLive(removedTimeToLive: FiniteDuration): DistributedPubSubSettings =
    copy(removedTimeToLive = removedTimeToLive)

  def withMaxDeltaElements(maxDeltaElements: Int): DistributedPubSubSettings =
    copy(maxDeltaElements = maxDeltaElements)

  def withSendToDeadLettersWhenNoSubscribers(sendToDeadLetterWhenNoSubscribers: Boolean): DistributedPubSubSettings =
    copy(sendToDeadLettersWhenNoSubscribers = sendToDeadLetterWhenNoSubscribers)

  private def copy(
      role: Option[String] = role,
      routingLogic: RoutingLogic = routingLogic,
      gossipInterval: FiniteDuration = gossipInterval,
      removedTimeToLive: FiniteDuration = removedTimeToLive,
      maxDeltaElements: Int = maxDeltaElements,
      sendToDeadLettersWhenNoSubscribers: Boolean = sendToDeadLettersWhenNoSubscribers): DistributedPubSubSettings =
    new DistributedPubSubSettings(
      role,
      routingLogic,
      gossipInterval,
      removedTimeToLive,
      maxDeltaElements,
      sendToDeadLettersWhenNoSubscribers)
}

object DistributedPubSubMediator {

  /**
   * Scala API: Factory method for `DistributedPubSubMediator` [[akka.actor.Props]].
   */
  def props(settings: DistributedPubSubSettings): Props =
    Props(new DistributedPubSubMediator(settings)).withDeploy(Deploy.local)

  @SerialVersionUID(1L) final case class Put(ref: ActorRef)
  @SerialVersionUID(1L) final case class Remove(path: String)
  @SerialVersionUID(1L) final case class Subscribe(topic: String, group: Option[String], ref: ActorRef) {
    require(topic != null && topic != "", "topic must be defined")

    /**
     * Convenience constructor with `group` None
     */
    def this(topic: String, ref: ActorRef) = this(topic, None, ref)

    /**
     * Java API: constructor with group: String
     */
    def this(topic: String, group: String, ref: ActorRef) = this(topic, Some(group), ref)
  }
  object Subscribe {
    def apply(topic: String, ref: ActorRef) = new Subscribe(topic, ref)
  }
  @SerialVersionUID(1L) final case class Unsubscribe(topic: String, group: Option[String], ref: ActorRef) {
    require(topic != null && topic != "", "topic must be defined")
    def this(topic: String, ref: ActorRef) = this(topic, None, ref)
    def this(topic: String, group: String, ref: ActorRef) = this(topic, Some(group), ref)
  }
  object Unsubscribe {
    def apply(topic: String, ref: ActorRef) = new Unsubscribe(topic, ref)
  }
  @SerialVersionUID(1L) final case class SubscribeAck(subscribe: Subscribe) extends DeadLetterSuppression
  @SerialVersionUID(1L) final case class UnsubscribeAck(unsubscribe: Unsubscribe)
  @SerialVersionUID(1L) final case class Publish(topic: String, msg: Any, sendOneMessageToEachGroup: Boolean)
      extends DistributedPubSubMessage
      with WrappedMessage {
    def this(topic: String, msg: Any) = this(topic, msg, sendOneMessageToEachGroup = false)

    override def message: Any = msg
  }
  object Publish {
    def apply(topic: String, msg: Any) = new Publish(topic, msg)
  }
  @SerialVersionUID(1L) final case class Send(path: String, msg: Any, localAffinity: Boolean)
      extends DistributedPubSubMessage
      with WrappedMessage {
    if (msg == null)
      throw InvalidMessageException("[null] is not an allowed message")

    /**
     * Convenience constructor with `localAffinity` false
     */
    def this(path: String, msg: Any) = this(path, msg, localAffinity = false)

    override def message: Any = msg
  }
  @SerialVersionUID(1L) final case class SendToAll(path: String, msg: Any, allButSelf: Boolean = false)
      extends DistributedPubSubMessage
      with WrappedMessage {
    if (msg == null)
      throw InvalidMessageException("[null] is not an allowed message")

    def this(path: String, msg: Any) = this(path, msg, allButSelf = false)

    override def message: Any = msg
  }

  sealed abstract class GetTopics

  /**
   * Send this message to the mediator and it will reply with
   * [[CurrentTopics]] containing the names of the (currently known)
   * registered topic names.
   */
  @SerialVersionUID(1L)
  case object GetTopics extends GetTopics

  /**
   * Java API: Send this message to the mediator and it will reply with
   * [[DistributedPubSubMediator.CurrentTopics]] containing the names of the (currently known)
   * registered topic names.
   */
  def getTopicsInstance: GetTopics = GetTopics

  /**
   * Reply to `GetTopics`.
   */
  @SerialVersionUID(1L)
  final case class CurrentTopics(topics: Set[String]) {

    /**
     * Java API
     */
    def getTopics(): java.util.Set[String] = {
      import scala.jdk.CollectionConverters._
      topics.asJava
    }
  }

  sealed abstract class Count

  /**
   * Scala API: Send this message to the mediator and it will reply with an `Int` of
   * the number of subscribers.
   * Only for testing purposes, to poll/await replication.
   */
  case object Count extends Count

  /**
   * Java API: Send this message to the mediator and it will reply with an `Integer` of
   * the number of subscribers.
   */
  def getCountInstance: Count = Count

  final case class CountSubscribers(topic: String)

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    case object Prune

    @SerialVersionUID(1L)
    final case class Bucket(owner: Address, version: Long, content: TreeMap[String, ValueHolder])

    @SerialVersionUID(1L)
    final case class ValueHolder(version: Long, ref: Option[ActorRef]) {
      @transient lazy val routee: Option[Routee] = ref.map(ActorRefRoutee(_))
    }

    @SerialVersionUID(1L)
    final case class Status(versions: Map[Address, Long], isReplyToStatus: Boolean)
        extends DistributedPubSubMessage
        with DeadLetterSuppression
    @SerialVersionUID(1L)
    final case class Delta(buckets: immutable.Iterable[Bucket])
        extends DistributedPubSubMessage
        with DeadLetterSuppression

    // Only for testing purposes, to verify replication
    case object DeltaCount

    case object GossipTick

    @SerialVersionUID(1L)
    final case class RegisterTopic(topicRef: ActorRef)
    @SerialVersionUID(1L)
    final case class Subscribed(ack: SubscribeAck, subscriber: ActorRef)
    @SerialVersionUID(1L)
    final case class Unsubscribed(ack: UnsubscribeAck, subscriber: ActorRef)
    @SerialVersionUID(1L)
    final case class SendToOneSubscriber(msg: Any)

    /**
     * Messages used to encode protocol to make sure that we do not send Subscribe/Unsubscribe message to
     * child (mediator -&gt; topic, topic -&gt; group) during a period of transition. Protects from situations like:
     *
     * Sending Subscribe/Unsubscribe message to child actor after child has been terminated
     * but Terminate message did not yet arrive to parent.
     *
     * Sending Subscribe/Unsubscribe message to child actor that has Prune message queued and pruneDeadline set.
     *
     * In both of those situation parent actor still thinks that child actor is alive and forwards messages to it resulting in lost ACKs.
     */
    trait ChildActorTerminationProtocol

    /**
     * Passivate-like message sent from child to parent, used to signal that sender has no subscribers and no child actors.
     */
    case object NoMoreSubscribers extends ChildActorTerminationProtocol

    /**
     * Sent from parent to child actor to signalize that messages are being buffered. When received by child actor
     * if no [[Subscribe]] message has been received after sending [[NoMoreSubscribers]] message child actor will stop itself.
     */
    case object TerminateRequest extends ChildActorTerminationProtocol

    /**
     * Sent from child to parent actor as response to [[TerminateRequest]] in case [[Subscribe]] message arrived
     * after sending [[NoMoreSubscribers]] but before receiving [[TerminateRequest]].
     *
     * When received by the parent buffered messages will be forwarded to child actor for processing.
     */
    case object NewSubscriberArrived extends ChildActorTerminationProtocol

    @SerialVersionUID(1L)
    final case class MediatorRouterEnvelope(msg: Any) extends RouterEnvelope {
      override def message = msg
    }

    def encName(s: String) = URLEncoder.encode(s, "utf-8")

    def mkKey(ref: ActorRef): String = mkKey(ref.path)

    def mkKey(path: ActorPath): String = path.toStringWithoutAddress

    trait TopicLike extends Actor {
      import context.dispatcher
      val pruneInterval: FiniteDuration = emptyTimeToLive / 2
      val pruneTask = context.system.scheduler.scheduleWithFixedDelay(pruneInterval, pruneInterval, self, Prune)
      var pruneDeadline: Option[Deadline] = None

      var subscribers = Set.empty[ActorRef]

      val emptyTimeToLive: FiniteDuration

      override def postStop(): Unit = {
        super.postStop()
        pruneTask.cancel()
      }

      def defaultReceive: Receive = {
        case msg @ Subscribe(_, _, ref) =>
          context.watch(ref)
          subscribers += ref
          pruneDeadline = None
          context.parent ! Subscribed(SubscribeAck(msg), sender())
        case msg @ Unsubscribe(_, _, ref) =>
          context.unwatch(ref)
          remove(ref)
          context.parent ! Unsubscribed(UnsubscribeAck(msg), sender())
        case Terminated(ref) =>
          remove(ref)
        case Prune =>
          pruneDeadline match {
            case Some(deadline) if deadline.isOverdue() =>
              pruneDeadline = None
              context.parent ! NoMoreSubscribers
            case _ =>
          }
        case TerminateRequest =>
          if (subscribers.isEmpty && context.children.isEmpty)
            context.stop(self)
          else
            context.parent ! NewSubscriberArrived
        case Count =>
          sender() ! subscribers.size
        case msg =>
          subscribers.foreach { _.forward(msg) }
      }

      def business: Receive

      def receive = business.orElse[Any, Unit](defaultReceive)

      def remove(ref: ActorRef): Unit = {
        if (subscribers.contains(ref))
          subscribers -= ref
        if (subscribers.isEmpty && context.children.isEmpty)
          pruneDeadline = Some(Deadline.now + emptyTimeToLive)
      }
    }

    class Topic(val emptyTimeToLive: FiniteDuration, routingLogic: RoutingLogic)
        extends TopicLike
        with PerGroupingBuffer {
      def business = {
        case msg @ Subscribe(_, Some(group), _) =>
          val encGroup = encName(group)
          bufferOr(mkKey(self.path / encGroup), msg, sender()) {
            context.child(encGroup) match {
              case Some(g) => g.forward(msg)
              case None    => newGroupActor(encGroup).forward(msg)
            }
          }
          pruneDeadline = None
        case msg @ Unsubscribe(_, Some(group), _) =>
          val encGroup = encName(group)
          bufferOr(mkKey(self.path / encGroup), msg, sender()) {
            context.child(encGroup) match {
              case Some(g) => g.forward(msg)
              case None    => // no such group here
            }
          }
        case msg: Subscribed =>
          context.parent.forward(msg)
        case msg: Unsubscribed =>
          context.parent.forward(msg)
        case NoMoreSubscribers =>
          val key = mkKey(sender())
          initializeGrouping(key)
          sender() ! TerminateRequest
        case NewSubscriberArrived =>
          val key = mkKey(sender())
          forwardMessages(key, sender())
        case Terminated(ref) =>
          val key = mkKey(ref)
          recreateAndForwardMessagesIfNeeded(key, newGroupActor(ref.path.name))
          remove(ref)
      }

      def newGroupActor(encGroup: String): ActorRef = {
        val g = context.actorOf(Props(new Group(emptyTimeToLive, routingLogic)), name = encGroup)
        context.watch(g)
        context.parent ! RegisterTopic(g)
        g
      }
    }

    class Group(val emptyTimeToLive: FiniteDuration, routingLogic: RoutingLogic) extends TopicLike {
      def business = {
        case SendToOneSubscriber(msg) =>
          if (subscribers.nonEmpty)
            Router(routingLogic, subscribers.map(ActorRefRoutee(_)).toVector).route(wrapIfNeeded(msg), sender())
      }
    }

    /**
     * Mediator uses [[akka.routing.Router]] to send messages to multiple destinations, Router in general
     * unwraps messages from [[akka.routing.RouterEnvelope]] and sends the contents to [[akka.routing.Routee]]s.
     *
     * Using mediator services should not have an undesired effect of unwrapping messages
     * out of [[akka.routing.RouterEnvelope]]. For this reason user messages are wrapped in
     * [[MediatorRouterEnvelope]] which will be unwrapped by the [[akka.routing.Router]] leaving original
     * user message.
     */
    def wrapIfNeeded: Any => Any = {
      case msg: RouterEnvelope => MediatorRouterEnvelope(msg)
      case null                => throw InvalidMessageException("Message must not be null")
      case msg: Any            => msg
    }
  }
}

/**
 * Marker trait for remote messages with special serializer.
 */
trait DistributedPubSubMessage extends Serializable

/**
 * This actor manages a registry of actor references and replicates
 * the entries to peer actors among all cluster nodes or a group of nodes
 * tagged with a specific role.
 *
 * The `DistributedPubSubMediator` actor is supposed to be started on all nodes,
 * or all nodes with specified role, in the cluster. The mediator can be
 * started with the [[DistributedPubSub]] extension or as an ordinary actor.
 *
 * Changes are only performed in the own part of the registry and those changes
 * are versioned. Deltas are disseminated in a scalable way to other nodes with
 * a gossip protocol. The registry is eventually consistent, i.e. changes are not
 * immediately visible at other nodes, but typically they will be fully replicated
 * to all other nodes after a few seconds.
 *
 * You can send messages via the mediator on any node to registered actors on
 * any other node. There is three modes of message delivery.
 *
 * 1. [[DistributedPubSubMediator.Send]] -
 * The message will be delivered to one recipient with a matching path, if any such
 * exists in the registry. If several entries match the path the message will be sent
 * via the supplied `routingLogic` (default random) to one destination. The sender of the
 * message can specify that local affinity is preferred, i.e. the message is sent to an actor
 * in the same local actor system as the used mediator actor, if any such exists, otherwise
 * route to any other matching entry. A typical usage of this mode is private chat to one
 * other user in an instant messaging application. It can also be used for distributing
 * tasks to registered workers, like a cluster aware router where the routees dynamically
 * can register themselves.
 *
 * 2. [[DistributedPubSubMediator.SendToAll]] -
 * The message will be delivered to all recipients with a matching path. Actors with
 * the same path, without address information, can be registered on different nodes.
 * On each node there can only be one such actor, since the path is unique within one
 * local actor system. Typical usage of this mode is to broadcast messages to all replicas
 * with the same path, e.g. 3 actors on different nodes that all perform the same actions,
 * for redundancy.
 *
 * 3. [[DistributedPubSubMediator.Publish]] -
 * Actors may be registered to a named topic instead of path. This enables many subscribers
 * on each node. The message will be delivered to all subscribers of the topic. For
 * efficiency the message is sent over the wire only once per node (that has a matching topic),
 * and then delivered to all subscribers of the local topic representation. This is the
 * true pub/sub mode. A typical usage of this mode is a chat room in an instant messaging
 * application.
 *
 * 4. [[DistributedPubSubMediator.Publish]] with sendOneMessageToEachGroup -
 * Actors may be subscribed to a named topic with an optional property `group`.
 * If subscribing with a group name, each message published to a topic with the
 * `sendOneMessageToEachGroup` flag is delivered via the supplied `routingLogic`
 * (default random) to one actor within each subscribing group.
 * If all the subscribed actors have the same group name, then this works just like
 * [[DistributedPubSubMediator.Send]] and all messages are delivered to one subscribe.
 * If all the subscribed actors have different group names, then this works like normal
 * [[DistributedPubSubMediator.Publish]] and all messages are broadcast to all subscribers.
 *
 * You register actors to the local mediator with [[DistributedPubSubMediator.Put]] or
 * [[DistributedPubSubMediator.Subscribe]]. `Put` is used together with `Send` and
 * `SendToAll` message delivery modes. The `ActorRef` in `Put` must belong to the same
 * local actor system as the mediator. `Subscribe` is used together with `Publish`.
 * Actors are automatically removed from the registry when they are terminated, or you
 * can explicitly remove entries with [[DistributedPubSubMediator.Remove]] or
 * [[DistributedPubSubMediator.Unsubscribe]].
 *
 * Successful `Subscribe` and `Unsubscribe` is acknowledged with
 * [[DistributedPubSubMediator.SubscribeAck]] and [[DistributedPubSubMediator.UnsubscribeAck]]
 * replies.
 *
 * Not intended for subclassing by user code.
 */
@DoNotInherit
class DistributedPubSubMediator(settings: DistributedPubSubSettings)
    extends Actor
    with ActorLogging
    with PerGroupingBuffer {

  import DistributedPubSubMediator._
  import DistributedPubSubMediator.Internal._
  import settings._

  require(
    !routingLogic.isInstanceOf[ConsistentHashingRoutingLogic],
    "'consistent-hashing' routing logic can't be used by the pub-sub mediator")

  val cluster = Cluster(context.system)
  import cluster.selfAddress

  require(
    role.forall(cluster.selfRoles.contains),
    s"This cluster member [${selfAddress}] doesn't have the role [$role]")

  val removedTimeToLiveMillis = removedTimeToLive.toMillis

  //Start periodic gossip to random nodes in cluster
  import context.dispatcher
  val gossipTask = context.system.scheduler.scheduleWithFixedDelay(gossipInterval, gossipInterval, self, GossipTick)
  val pruneInterval: FiniteDuration = removedTimeToLive / 2
  val pruneTask = context.system.scheduler.scheduleWithFixedDelay(pruneInterval, pruneInterval, self, Prune)

  var registry: Map[Address, Bucket] = Map.empty.withDefault(a => Bucket(a, 0L, TreeMap.empty))
  var nodes: Set[Address] = Set.empty
  var deltaCount = 0L

  // the version is a timestamp because it is also used when pruning removed entries
  val nextVersion = {
    var version = 0L
    () => {
      val current = System.currentTimeMillis
      version = if (current > version) current else version + 1
      version
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    require(!cluster.isTerminated, "Cluster node must not be terminated")
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    gossipTask.cancel()
    pruneTask.cancel()
  }

  def matchingRole(m: Member): Boolean = role.forall(m.hasRole)

  def receive = {

    case Send(path, msg, localAffinity) =>
      val routees = registry(selfAddress).content.get(path) match {
        case Some(valueHolder) if localAffinity =>
          valueHolder.routee.toList.toIndexedSeq
        case _ =>
          (for {
            (_, bucket) <- registry
            valueHolder <- bucket.content.get(path).toList
            routee <- valueHolder.routee.toList
          } yield routee).toVector
      }

      if (routees.isEmpty) ignoreOrSendToDeadLetters(msg)
      else Router(routingLogic, routees).route(wrapIfNeeded(msg), sender())

    case SendToAll(path, msg, skipSenderNode) =>
      publish(path, msg, skipSenderNode)

    case Publish(topic, msg, sendOneMessageToEachGroup) =>
      if (sendOneMessageToEachGroup)
        publishToEachGroup(mkKey(self.path / encName(topic)), msg)
      else
        publish(mkKey(self.path / encName(topic)), msg)

    case Put(ref: ActorRef) =>
      if (ref.path.address.hasGlobalScope)
        log.warning("Registered actor must be local: [{}]", ref)
      else {
        put(mkKey(ref), Some(ref))
        context.watch(ref)
      }

    case Remove(key) =>
      registry(selfAddress).content.get(key) match {
        case Some(ValueHolder(_, Some(ref))) =>
          context.unwatch(ref)
          put(key, None)
        case _ =>
      }

    case msg @ Subscribe(topic, _, _) =>
      // each topic is managed by a child actor with the same name as the topic

      val encTopic = encName(topic)

      bufferOr(mkKey(self.path / encTopic), msg, sender()) {
        context.child(encTopic) match {
          case Some(t) => t.forward(msg)
          case None    => newTopicActor(encTopic).forward(msg)
        }
      }

    case RegisterTopic(t) =>
      registerTopic(t)

    case NoMoreSubscribers =>
      val key = mkKey(sender())
      initializeGrouping(key)
      sender() ! TerminateRequest

    case NewSubscriberArrived =>
      val key = mkKey(sender())
      forwardMessages(key, sender())

    case GetTopics =>
      sender() ! CurrentTopics(getCurrentTopics())

    case Subscribed(ack, ref) =>
      ref ! ack

    case msg @ Unsubscribe(topic, _, _) =>
      val encTopic = encName(topic)
      bufferOr(mkKey(self.path / encTopic), msg, sender()) {
        context.child(encTopic) match {
          case Some(t) => t.forward(msg)
          case None    => // no such topic here
        }
      }

    case Unsubscribed(ack, ref) =>
      ref ! ack

    case Status(otherVersions, isReplyToStatus) =>
      // only accept status from known nodes, otherwise old cluster with same address may interact
      // also accept from local for testing purposes
      if (nodes(sender().path.address) || sender().path.address.hasLocalScope) {
        // gossip chat starts with a Status message, containing the bucket versions of the other node
        val delta = collectDelta(otherVersions)
        if (delta.nonEmpty)
          sender() ! Delta(delta)
        if (!isReplyToStatus && otherHasNewerVersions(otherVersions))
          sender() ! Status(versions = myVersions, isReplyToStatus = true) // it will reply with Delta
      }

    case Delta(buckets) =>
      deltaCount += 1

      // reply from Status message in the gossip chat
      // the Delta contains potential updates (newer versions) from the other node
      // only accept deltas/buckets from known nodes, otherwise there is a risk of
      // adding back entries when nodes are removed
      if (nodes(sender().path.address)) {
        buckets.foreach { b =>
          if (nodes(b.owner)) {
            val myBucket = registry(b.owner)
            if (b.version > myBucket.version) {
              registry += (b.owner -> myBucket.copy(version = b.version, content = myBucket.content ++ b.content))
            }
          }
        }
      }

    case GossipTick => gossip()

    case Prune => prune()

    case Terminated(a) =>
      val key = mkKey(a)
      registry(selfAddress).content.get(key) match {
        case Some(ValueHolder(_, Some(`a`))) =>
          // remove
          put(key, None)
        case _ =>
      }
      recreateAndForwardMessagesIfNeeded(key, newTopicActor(a.path.name))

    case state: CurrentClusterState =>
      nodes = state.members.collect {
        case m if m.status != MemberStatus.Joining && matchingRole(m) => m.address
      }

    case MemberUp(m) =>
      if (matchingRole(m))
        nodes += m.address

    case MemberWeaklyUp(m) =>
      if (matchingRole(m))
        nodes += m.address

    case MemberLeft(m) =>
      if (matchingRole(m)) {
        nodes -= m.address
        registry -= m.address
      }

    case MemberDowned(m) =>
      if (matchingRole(m)) {
        nodes -= m.address
        registry -= m.address
      }

    case MemberRemoved(m, _) =>
      if (m.address == selfAddress)
        context.stop(self)
      else if (matchingRole(m)) {
        nodes -= m.address
        registry -= m.address
      }

    case _: MemberEvent => // not of interest

    case Count =>
      val count = registry.map {
        case (_, bucket) =>
          bucket.content.count {
            case (_, valueHolder) => valueHolder.ref.isDefined
          }
      }.sum
      sender() ! count

    case DeltaCount =>
      sender() ! deltaCount

    case msg @ CountSubscribers(topic) =>
      val encTopic = encName(topic)
      bufferOr(mkKey(self.path / encTopic), msg, sender()) {
        context.child(encTopic) match {
          case Some(ref) => ref.tell(Count, sender())
          case None      => sender() ! 0
        }
      }
  }

  private def ignoreOrSendToDeadLetters(msg: Any) =
    if (settings.sendToDeadLettersWhenNoSubscribers)
      context.system.deadLetters ! DeadLetter(msg, sender(), context.self)

  def publish(path: String, msg: Any, allButSelf: Boolean = false): Unit = {
    val refs = for {
      (address, bucket) <- registry
      if !(allButSelf && address == selfAddress) // if we should skip sender() node and current address == self address => skip
      valueHolder <- bucket.content.get(path).toList
      ref <- valueHolder.ref.toList
    } yield ref
    if (refs.isEmpty) ignoreOrSendToDeadLetters(msg)
    else refs.foreach(_.forward(msg))
  }

  def publishToEachGroup(path: String, msg: Any): Unit = {
    val prefix = path + '/'
    val lastKey = path + '0' // '0' is the next char of '/'
    val groups = (for {
      (_, bucket) <- registry.toSeq
      key <- bucket.content.range(prefix, lastKey).keys
      valueHolder <- bucket.content.get(key)
      ref <- valueHolder.routee
    } yield (key, ref)).groupBy(_._1).values

    if (groups.isEmpty) {
      ignoreOrSendToDeadLetters(msg)
    } else {
      val wrappedMsg = SendToOneSubscriber(msg)
      groups.foreach { group =>
        val routees = group.map(_._2).toVector
        if (routees.nonEmpty)
          Router(routingLogic, routees).route(wrappedMsg, sender())
      }
    }
  }

  def put(key: String, valueOption: Option[ActorRef]): Unit = {
    val bucket = registry(selfAddress)
    val v = nextVersion()
    registry += (selfAddress -> bucket.copy(
      version = v,
      content = bucket.content + (key -> ValueHolder(v, valueOption))))
  }

  def getCurrentTopics(): Set[String] = {
    val topicPrefix = self.path.toStringWithoutAddress
    (for {
      (_, bucket) <- registry
      (key, _) <- bucket.content.toSeq
      if key.startsWith(topicPrefix)
      topic = key.substring(topicPrefix.length + 1)
      if !topic.contains('/') // exclude group topics
    } yield URLDecoder.decode(topic, "utf-8")).toSet
  }

  def registerTopic(ref: ActorRef): Unit = {
    put(mkKey(ref), Some(ref))
    context.watch(ref)
  }

  def mkKey(ref: ActorRef): String = Internal.mkKey(ref)

  def mkKey(path: ActorPath): String = Internal.mkKey(path)

  def myVersions: Map[Address, Long] = registry.map { case (owner, bucket) => (owner -> bucket.version) }

  def collectDelta(otherVersions: Map[Address, Long]): immutable.Iterable[Bucket] = {
    // missing entries are represented by version 0
    val filledOtherVersions = myVersions.map { case (k, _) => k -> 0L } ++ otherVersions
    var count = 0
    filledOtherVersions.collect {
      case (owner, v) if registry(owner).version > v && count < maxDeltaElements =>
        val bucket = registry(owner)
        val deltaContent = bucket.content.filter {
          case (_, value) => value.version > v
        }
        count += deltaContent.size
        if (count <= maxDeltaElements)
          bucket.copy(content = deltaContent)
        else {
          // exceeded the maxDeltaElements, pick the elements with lowest versions
          val sortedContent = deltaContent.toVector.sortBy(_._2.version)
          val chunk = sortedContent.take(maxDeltaElements - (count - sortedContent.size))
          bucket.copy(content = TreeMap.empty[String, ValueHolder] ++ chunk, version = chunk.last._2.version)
        }
    }
  }

  def otherHasNewerVersions(otherVersions: Map[Address, Long]): Boolean =
    otherVersions.exists {
      case (owner, v) => v > registry(owner).version
    }

  /**
   * Gossip to peer nodes.
   */
  def gossip(): Unit = selectRandomNode((nodes - selfAddress).toVector).foreach(gossipTo)

  def gossipTo(address: Address): Unit = {
    val sel = context.actorSelection(self.path.toStringWithAddress(address))
    sel ! Status(versions = myVersions, isReplyToStatus = false)
  }

  def selectRandomNode(addresses: immutable.IndexedSeq[Address]): Option[Address] =
    if (addresses.isEmpty) None else Some(addresses(ThreadLocalRandom.current.nextInt(addresses.size)))

  def prune(): Unit = {
    registry.foreach {
      case (owner, bucket) =>
        val oldRemoved = bucket.content.collect {
          case (key, ValueHolder(version, None)) if (bucket.version - version > removedTimeToLiveMillis) => key
        }
        if (oldRemoved.nonEmpty)
          registry += owner -> bucket.copy(content = bucket.content -- oldRemoved)
    }
  }

  def newTopicActor(encTopic: String): ActorRef = {
    val t = context.actorOf(Props(new Topic(removedTimeToLive, routingLogic)), name = encTopic)
    registerTopic(t)
    t
  }
}

object DistributedPubSub extends ExtensionId[DistributedPubSub] with ExtensionIdProvider {
  override def get(system: ActorSystem): DistributedPubSub = super.get(system)

  override def get(system: ClassicActorSystemProvider): DistributedPubSub = super.get(system)

  override def lookup = DistributedPubSub

  override def createExtension(system: ExtendedActorSystem): DistributedPubSub =
    new DistributedPubSub(system)
}

/**
 * Extension that starts a [[DistributedPubSubMediator]] actor
 * with settings defined in config section `akka.cluster.pub-sub`.
 */
class DistributedPubSub(system: ExtendedActorSystem) extends Extension {

  private val settings = DistributedPubSubSettings(system)

  /**
   * Returns true if this member is not tagged with the role configured for the
   * mediator.
   */
  def isTerminated: Boolean =
    Cluster(system).isTerminated || !settings.role.forall(Cluster(system).selfRoles.contains)

  /**
   * The [[DistributedPubSubMediator]]
   */
  val mediator: ActorRef = {
    if (isTerminated)
      system.deadLetters
    else {
      val name = system.settings.config.getString("akka.cluster.pub-sub.name")
      val dispatcher = system.settings.config.getString("akka.cluster.pub-sub.use-dispatcher")
      system.systemActorOf(DistributedPubSubMediator.props(settings).withDispatcher(dispatcher), name)
    }
  }
}
