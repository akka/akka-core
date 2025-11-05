/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster.client

import java.net.URLEncoder

import scala.collection.immutable
import scala.collection.immutable.{ HashMap, HashSet }
import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorLogging
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Cancellable
import akka.actor.ClassicActorSystemProvider
import akka.actor.DeadLetterSuppression
import akka.actor.Deploy
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Identify
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.pubsub._
import akka.japi.Util.immutableSeq
import akka.remote.DeadlineFailureDetector
import akka.routing.ConsistentHash
import akka.routing.MurmurHash
import akka.util.MessageBuffer
import akka.util.ccompat._

@ccompatUsedUntil213
@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
object ClusterClientSettings {

  /**
   * Create settings from the default configuration
   * `akka.cluster.client`.
   */
  def apply(system: ActorSystem): ClusterClientSettings =
    apply(system.settings.config.getConfig("akka.cluster.client"))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.client`.
   */
  def apply(config: Config): ClusterClientSettings = {
    val initialContacts = immutableSeq(config.getStringList("initial-contacts")).map(ActorPath.fromString).toSet
    new ClusterClientSettings(
      initialContacts,
      establishingGetContactsInterval = config.getDuration("establishing-get-contacts-interval", MILLISECONDS).millis,
      refreshContactsInterval = config.getDuration("refresh-contacts-interval", MILLISECONDS).millis,
      heartbeatInterval = config.getDuration("heartbeat-interval", MILLISECONDS).millis,
      acceptableHeartbeatPause = config.getDuration("acceptable-heartbeat-pause", MILLISECONDS).millis,
      bufferSize = config.getInt("buffer-size"),
      reconnectTimeout = config.getString("reconnect-timeout") match {
        case "off" => None
        case _     => Some(config.getDuration("reconnect-timeout", MILLISECONDS).millis)
      })
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.cluster.client`.
   */
  def create(system: ActorSystem): ClusterClientSettings = apply(system)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.client`.
   */
  def create(config: Config): ClusterClientSettings = apply(config)

}

/**
 * @param initialContacts Actor paths of the `ClusterReceptionist` actors on
 *   the servers (cluster nodes) that the client will try to contact initially.
 *   It is mandatory to specify at least one initial contact. The path of the
 *   default receptionist is
 *   "akka://system@hostname:port/system/receptionist"
 * @param establishingGetContactsInterval Interval at which the client retries
 *   to establish contact with one of ClusterReceptionist on the servers (cluster nodes)
 * @param refreshContactsInterval Interval at which the client will ask the
 *   `ClusterReceptionist` for new contact points to be used for next reconnect.
 * @param heartbeatInterval How often failure detection heartbeat messages for detection
 *   of failed connections should be sent.
 * @param acceptableHeartbeatPause Number of potentially lost/delayed heartbeats that will
 *   be accepted before considering it to be an anomaly. The ClusterClient is using the
 *   [[akka.remote.DeadlineFailureDetector]], which will trigger if there are no heartbeats
 *   within the duration `heartbeatInterval + acceptableHeartbeatPause`.
 * @param bufferSize If connection to the receptionist is not established the client
 *   will buffer this number of messages and deliver them the connection is established.
 *   When the buffer is full old messages will be dropped when new messages are sent via the
 *   client. Use 0 to disable buffering, i.e. messages will be dropped immediately if the
 *   location of the receptionist is unavailable.
 * @param reconnectTimeout If the connection to the receptionist is lost and cannot
 *   be re-established within this duration the cluster client will be stopped. This makes it possible
 *   to watch it from another actor and possibly acquire a new list of initialContacts from some
 *   external service registry
 */
@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
final class ClusterClientSettings(
    val initialContacts: Set[ActorPath],
    val establishingGetContactsInterval: FiniteDuration,
    val refreshContactsInterval: FiniteDuration,
    val heartbeatInterval: FiniteDuration,
    val acceptableHeartbeatPause: FiniteDuration,
    val bufferSize: Int,
    val reconnectTimeout: Option[FiniteDuration])
    extends NoSerializationVerificationNeeded {

  require(bufferSize >= 0 && bufferSize <= 10000, "bufferSize must be >= 0 and <= 10000")

  /**
   * For binary/source compatibility
   */
  def this(
      initialContacts: Set[ActorPath],
      establishingGetContactsInterval: FiniteDuration,
      refreshContactsInterval: FiniteDuration,
      heartbeatInterval: FiniteDuration,
      acceptableHeartbeatPause: FiniteDuration,
      bufferSize: Int) =
    this(
      initialContacts,
      establishingGetContactsInterval,
      refreshContactsInterval,
      heartbeatInterval,
      acceptableHeartbeatPause,
      bufferSize,
      None)

  /**
   * Scala API
   */
  def withInitialContacts(initialContacts: Set[ActorPath]): ClusterClientSettings = {
    require(initialContacts.nonEmpty, "initialContacts must be defined")
    copy(initialContacts = initialContacts)
  }

  /**
   * Java API
   */
  def withInitialContacts(initialContacts: java.util.Set[ActorPath]): ClusterClientSettings = {
    import akka.util.ccompat.JavaConverters._
    withInitialContacts(initialContacts.asScala.toSet)
  }

  def withEstablishingGetContactsInterval(establishingGetContactsInterval: FiniteDuration): ClusterClientSettings =
    copy(establishingGetContactsInterval = establishingGetContactsInterval)

  def withRefreshContactsInterval(refreshContactsInterval: FiniteDuration): ClusterClientSettings =
    copy(refreshContactsInterval = refreshContactsInterval)

  def withHeartbeat(
      heartbeatInterval: FiniteDuration,
      acceptableHeartbeatPause: FiniteDuration): ClusterClientSettings =
    copy(heartbeatInterval = heartbeatInterval, acceptableHeartbeatPause = acceptableHeartbeatPause)

  def withBufferSize(bufferSize: Int): ClusterClientSettings =
    copy(bufferSize = bufferSize)

  def withReconnectTimeout(reconnectTimeout: Option[FiniteDuration]): ClusterClientSettings =
    copy(reconnectTimeout = reconnectTimeout)

  private def copy(
      initialContacts: Set[ActorPath] = initialContacts,
      establishingGetContactsInterval: FiniteDuration = establishingGetContactsInterval,
      refreshContactsInterval: FiniteDuration = refreshContactsInterval,
      heartbeatInterval: FiniteDuration = heartbeatInterval,
      acceptableHeartbeatPause: FiniteDuration = acceptableHeartbeatPause,
      bufferSize: Int = bufferSize,
      reconnectTimeout: Option[FiniteDuration] = reconnectTimeout): ClusterClientSettings =
    new ClusterClientSettings(
      initialContacts,
      establishingGetContactsInterval,
      refreshContactsInterval,
      heartbeatInterval,
      acceptableHeartbeatPause,
      bufferSize,
      reconnectTimeout)
}

/**
 * Declares a super type for all events emitted by the `ClusterClient`
 * in relation to contact points being added or removed.
 */
sealed trait ContactPointChange {
  val contactPoint: ActorPath
}

/**
 * Emitted to a subscriber when contact points have been
 * received by the ClusterClient and a new one has been added.
 */
final case class ContactPointAdded(override val contactPoint: ActorPath) extends ContactPointChange

/**
 * Emitted to a subscriber when contact points have been
 * received by the ClusterClient and a new one has been added.
 */
final case class ContactPointRemoved(override val contactPoint: ActorPath) extends ContactPointChange

sealed abstract class SubscribeContactPoints

/**
 * Subscribe to a cluster client's contact point changes where
 * it is guaranteed that a sender receives the initial state
 * of contact points prior to any events in relation to them
 * changing.
 * The sender will automatically become unsubscribed when it
 * terminates.
 */
case object SubscribeContactPoints extends SubscribeContactPoints {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

sealed abstract class UnsubscribeContactPoints

/**
 * Explicitly unsubscribe from contact point change events.
 */
case object UnsubscribeContactPoints extends UnsubscribeContactPoints {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

sealed abstract class GetContactPoints

/**
 * Get the contact points known to this client. A ``ContactPoints`` message
 * will be replied.
 */
case object GetContactPoints extends GetContactPoints {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * The reply to ``GetContactPoints``.
 *
 * @param contactPoints The presently known list of contact points.
 */
final case class ContactPoints(contactPoints: Set[ActorPath]) {
  import akka.util.ccompat.JavaConverters._

  /**
   * Java API
   */
  def getContactPoints: java.util.Set[ActorPath] =
    contactPoints.asJava
}

@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
object ClusterClient {

  /**
   * Scala API: Factory method for `ClusterClient` [[akka.actor.Props]].
   */
  @deprecated(
    "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
    since = "2.6.0")
  def props(settings: ClusterClientSettings): Props =
    Props(new ClusterClient(settings)).withDeploy(Deploy.local)

  @SerialVersionUID(1L)
  @deprecated(
    "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
    since = "2.6.0")
  final case class Send(path: String, msg: Any, localAffinity: Boolean) {

    /**
     * Convenience constructor with `localAffinity` false
     */
    def this(path: String, msg: Any) = this(path, msg, localAffinity = false)
  }
  @SerialVersionUID(1L)
  @deprecated(
    "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
    since = "2.6.0")
  final case class SendToAll(path: String, msg: Any)

  @SerialVersionUID(1L)
  @deprecated(
    "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
    since = "2.6.0")
  final case class Publish(topic: String, msg: Any)

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    case object RefreshContactsTick
    case object HeartbeatTick
    case object ReconnectTimeout
  }
}

/**
 * This actor is intended to be used on an external node that is not member
 * of the cluster. It acts like a gateway for sending messages to actors
 * somewhere in the cluster. From the initial contact points it will establish
 * a connection to a [[ClusterReceptionist]] somewhere in the cluster. It will
 * monitor the connection to the receptionist and establish a new connection if
 * the link goes down. When looking for a new receptionist it uses fresh contact
 * points retrieved from previous establishment, or periodically refreshed
 * contacts, i.e. not necessarily the initial contact points.
 *
 * You can send messages via the `ClusterClient` to any actor in the cluster
 * that is registered in the [[ClusterReceptionist]].
 * Messages are wrapped in [[ClusterClient.Send]], [[ClusterClient.SendToAll]]
 * or [[ClusterClient.Publish]].
 *
 * 1. [[ClusterClient.Send]] -
 * The message will be delivered to one recipient with a matching path, if any such
 * exists. If several entries match the path the message will be delivered
 * to one random destination. The sender of the message can specify that local
 * affinity is preferred, i.e. the message is sent to an actor in the same local actor
 * system as the used receptionist actor, if any such exists, otherwise random to any other
 * matching entry.
 *
 * 2. [[ClusterClient.SendToAll]] -
 * The message will be delivered to all recipients with a matching path.
 *
 * 3. [[ClusterClient.Publish]] -
 * The message will be delivered to all recipients Actors that have been registered as subscribers to
 * to the named topic.
 *
 *  Use the factory method [[ClusterClient#props]]) to create the
 * [[akka.actor.Props]] for the actor.
 *
 * If the receptionist is not currently available, the client will buffer the messages
 * and then deliver them when the connection to the receptionist has been established.
 * The size of the buffer is configurable and it can be disabled by using a buffer size
 * of 0. When the buffer is full old messages will be dropped when new messages are sent
 * via the client.
 *
 * Note that this is a best effort implementation: messages can always be lost due to the distributed
 * nature of the actors involved.
 */
@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
final class ClusterClient(settings: ClusterClientSettings) extends Actor with ActorLogging {

  import ClusterClient._
  import ClusterClient.Internal._
  import ClusterReceptionist.Internal._
  import settings._

  require(initialContacts.nonEmpty, "initialContacts must be defined")

  val failureDetector = new DeadlineFailureDetector(acceptableHeartbeatPause, heartbeatInterval)

  var contactPaths: HashSet[ActorPath] =
    initialContacts.to(HashSet)
  val initialContactsSel =
    contactPaths.map(context.actorSelection)
  var contacts = initialContactsSel
  sendGetContacts()

  var contactPathsPublished = contactPaths

  var subscribers = Vector.empty[ActorRef]

  import context.dispatcher
  val heartbeatTask =
    context.system.scheduler.scheduleWithFixedDelay(heartbeatInterval, heartbeatInterval, self, HeartbeatTick)
  var refreshContactsTask: Option[Cancellable] = None
  scheduleRefreshContactsTick(establishingGetContactsInterval)
  self ! RefreshContactsTick

  var buffer = MessageBuffer.empty

  def scheduleRefreshContactsTick(interval: FiniteDuration): Unit = {
    refreshContactsTask.foreach { _.cancel() }
    refreshContactsTask = Some(
      context.system.scheduler.scheduleWithFixedDelay(interval, interval, self, RefreshContactsTick))
  }

  override def postStop(): Unit = {
    super.postStop()
    heartbeatTask.cancel()
    refreshContactsTask.foreach { _.cancel() }
  }

  def receive = establishing.orElse(contactPointMessages)

  def establishing: Actor.Receive = {
    val connectTimerCancelable = settings.reconnectTimeout.map { timeout =>
      context.system.scheduler.scheduleOnce(timeout, self, ReconnectTimeout)
    }

    {
      case Contacts(contactPoints) =>
        if (contactPoints.nonEmpty) {
          contactPaths = contactPoints.map(ActorPath.fromString).to(HashSet)
          contacts = contactPaths.map(context.actorSelection)
          contacts.foreach { _ ! Identify(Array.emptyByteArray) }
        }
        publishContactPoints()
      case ActorIdentity(_, Some(receptionist)) =>
        log.info("Connected to [{}]", receptionist.path)
        scheduleRefreshContactsTick(refreshContactsInterval)
        sendBuffered(receptionist)
        context.become(active(receptionist).orElse(contactPointMessages))
        connectTimerCancelable.foreach(_.cancel())
        failureDetector.heartbeat()
        self ! HeartbeatTick // will register us as active client of the selected receptionist
      case ActorIdentity(_, None) => // ok, use another instead
      case HeartbeatTick =>
        failureDetector.heartbeat()
      case RefreshContactsTick => sendGetContacts()
      case Send(path, msg, localAffinity) =>
        buffer(DistributedPubSubMediator.Send(path, msg, localAffinity))
      case SendToAll(path, msg) =>
        buffer(DistributedPubSubMediator.SendToAll(path, msg))
      case Publish(topic, msg) =>
        buffer(DistributedPubSubMediator.Publish(topic, msg))
      case ReconnectTimeout =>
        log.warning(
          "Receptionist reconnect not successful within {} stopping cluster client",
          settings.reconnectTimeout)
        context.stop(self)
      case ReceptionistShutdown => // ok, haven't chosen a receptionist yet
    }
  }

  def active(receptionist: ActorRef): Actor.Receive = {
    case Send(path, msg, localAffinity) =>
      receptionist.forward(DistributedPubSubMediator.Send(path, msg, localAffinity))
    case SendToAll(path, msg) =>
      receptionist.forward(DistributedPubSubMediator.SendToAll(path, msg))
    case Publish(topic, msg) =>
      receptionist.forward(DistributedPubSubMediator.Publish(topic, msg))
    case HeartbeatTick =>
      if (!failureDetector.isAvailable) {
        log.info("Lost contact with [{}], reestablishing connection", receptionist)
        reestablish()
      } else
        receptionist ! Heartbeat
    case HeartbeatRsp =>
      failureDetector.heartbeat()
    case RefreshContactsTick =>
      receptionist ! GetContacts
    case Contacts(contactPoints) =>
      // refresh of contacts
      if (contactPoints.nonEmpty) {
        contactPaths = contactPoints.map(ActorPath.fromString).to(HashSet)
        contacts = contactPaths.map(context.actorSelection)
      }
      publishContactPoints()
    case _: ActorIdentity => // ok, from previous establish, already handled
    case ReceptionistShutdown =>
      if (receptionist == sender()) {
        log.info("Receptionist [{}] is shutting down, reestablishing connection", receptionist)
        reestablish()
      }
  }

  def contactPointMessages: Actor.Receive = {
    case SubscribeContactPoints =>
      val subscriber = sender()
      subscriber ! ContactPoints(contactPaths)
      subscribers :+= subscriber
      context.watch(subscriber)
    case UnsubscribeContactPoints =>
      val subscriber = sender()
      subscribers = subscribers.filterNot(_ == subscriber)
    case Terminated(subscriber) =>
      self.tell(UnsubscribeContactPoints, subscriber)
    case GetContactPoints =>
      sender() ! ContactPoints(contactPaths)
  }

  def sendGetContacts(): Unit = {
    val sendTo =
      if (contacts.isEmpty) initialContactsSel
      else if (contacts.size == 1) initialContactsSel.union(contacts)
      else contacts
    if (log.isDebugEnabled)
      log.debug(s"""Sending GetContacts to [${sendTo.mkString(",")}]""")
    sendTo.foreach { _ ! GetContacts }
  }

  def buffer(msg: Any): Unit =
    if (settings.bufferSize == 0)
      log.debug("Receptionist not available and buffering is disabled, dropping message [{}]", msg.getClass.getName)
    else if (buffer.size == settings.bufferSize) {
      val (m, _) = buffer.head()
      buffer.dropHead()
      log.debug("Receptionist not available, buffer is full, dropping first message [{}]", m.getClass.getName)
      buffer.append(msg, sender())
    } else {
      log.debug("Receptionist not available, buffering message type [{}]", msg.getClass.getName)
      buffer.append(msg, sender())
    }

  def sendBuffered(receptionist: ActorRef): Unit = {
    log.debug("Sending buffered messages to receptionist")
    buffer.foreach((msg, snd) => receptionist.tell(msg, snd))
    buffer = MessageBuffer.empty
  }

  def publishContactPoints(): Unit = {
    for (cp <- contactPaths if !contactPathsPublished.contains(cp)) {
      val contactPointAdded = ContactPointAdded(cp)
      subscribers.foreach(_ ! contactPointAdded)
    }
    for (cp <- contactPathsPublished if !contactPaths.contains(cp)) {
      val contactPointRemoved = ContactPointRemoved(cp)
      subscribers.foreach(_ ! contactPointRemoved)
    }
    contactPathsPublished = contactPaths
  }

  def reestablish(): Unit = {
    sendGetContacts()
    scheduleRefreshContactsTick(establishingGetContactsInterval)
    context.become(establishing.orElse(contactPointMessages))
    failureDetector.heartbeat()
  }
}

@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
object ClusterClientReceptionist extends ExtensionId[ClusterClientReceptionist] with ExtensionIdProvider {
  override def get(system: ActorSystem): ClusterClientReceptionist = super.get(system)
  override def get(system: ClassicActorSystemProvider): ClusterClientReceptionist = super.get(system)

  override def lookup = ClusterClientReceptionist

  override def createExtension(system: ExtendedActorSystem): ClusterClientReceptionist =
    new ClusterClientReceptionist(system)
}

/**
 * Extension that starts [[ClusterReceptionist]] and accompanying [[akka.cluster.pubsub.DistributedPubSubMediator]]
 * with settings defined in config section `akka.cluster.client.receptionist`.
 * The [[akka.cluster.pubsub.DistributedPubSubMediator]] is started by the [[akka.cluster.pubsub.DistributedPubSub]] extension.
 */
@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
final class ClusterClientReceptionist(system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config.getConfig("akka.cluster.client.receptionist")
  private val role: Option[String] = config.getString("role") match {
    case "" => None
    case r  => Some(r)
  }

  /**
   * Returns true if this member is not tagged with the role configured for the
   * receptionist.
   */
  def isTerminated: Boolean = Cluster(system).isTerminated || !role.forall(Cluster(system).selfRoles.contains)

  /**
   * Register the actors that should be reachable for the clients in this [[DistributedPubSubMediator]].
   */
  private def pubSubMediator: ActorRef = DistributedPubSub(system).mediator

  /**
   * Register an actor that should be reachable for the clients.
   * The clients can send messages to this actor with `Send` or `SendToAll` using
   * the path elements of the `ActorRef`, e.g. `"/user/myservice"`.
   */
  def registerService(actor: ActorRef): Unit =
    pubSubMediator ! DistributedPubSubMediator.Put(actor)

  /**
   * A registered actor will be automatically unregistered when terminated,
   * but it can also be explicitly unregistered before termination.
   */
  def unregisterService(actor: ActorRef): Unit =
    pubSubMediator ! DistributedPubSubMediator.Remove(actor.path.toStringWithoutAddress)

  /**
   * Register an actor that should be reachable for the clients to a named topic.
   * Several actors can be registered to the same topic name, and all will receive
   * published messages.
   * The client can publish messages to this topic with `Publish`.
   */
  def registerSubscriber(topic: String, actor: ActorRef): Unit =
    pubSubMediator ! DistributedPubSubMediator.Subscribe(topic, actor)

  /**
   * A registered subscriber will be automatically unregistered when terminated,
   * but it can also be explicitly unregistered before termination.
   */
  def unregisterSubscriber(topic: String, actor: ActorRef): Unit =
    pubSubMediator ! DistributedPubSubMediator.Unsubscribe(topic, actor)

  /**
   * The [[ClusterReceptionist]] actor
   */
  private val receptionist: ActorRef = {
    if (isTerminated)
      system.deadLetters
    else {
      val name = config.getString("name")
      val dispatcher = config.getString("use-dispatcher")
      // important to use val mediator here to activate it outside of ClusterReceptionist constructor
      val mediator = pubSubMediator
      system.systemActorOf(
        ClusterReceptionist.props(mediator, ClusterReceptionistSettings(config)).withDispatcher(dispatcher),
        name)
    }
  }

  /**
   * Returns the underlying receptionist actor, particularly so that its
   * events can be observed via subscribe/unsubscribe.
   */
  def underlying: ActorRef =
    receptionist
}

@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
object ClusterReceptionistSettings {

  /**
   * Create settings from the default configuration
   * `akka.cluster.client.receptionist`.
   */
  def apply(system: ActorSystem): ClusterReceptionistSettings =
    apply(system.settings.config.getConfig("akka.cluster.client.receptionist"))

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.client.receptionist`.
   */
  def apply(config: Config): ClusterReceptionistSettings =
    new ClusterReceptionistSettings(
      role = roleOption(config.getString("role")),
      numberOfContacts = config.getInt("number-of-contacts"),
      responseTunnelReceiveTimeout = config.getDuration("response-tunnel-receive-timeout", MILLISECONDS).millis,
      heartbeatInterval = config.getDuration("heartbeat-interval", MILLISECONDS).millis,
      acceptableHeartbeatPause = config.getDuration("acceptable-heartbeat-pause", MILLISECONDS).millis,
      failureDetectionInterval = config.getDuration("failure-detection-interval", MILLISECONDS).millis)

  /**
   * Java API: Create settings from the default configuration
   * `akka.cluster.client.receptionist`.
   */
  def create(system: ActorSystem): ClusterReceptionistSettings = apply(system)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.client.receptionist`.
   */
  def create(config: Config): ClusterReceptionistSettings = apply(config)

  /**
   * INTERNAL API
   */
  private[akka] def roleOption(role: String): Option[String] =
    if (role == "") None else Option(role)

}

/**
 * @param role Start the receptionist on members tagged with this role.
 *   All members are used if undefined.
 * @param numberOfContacts The receptionist will send this number of contact points to the client
 * @param responseTunnelReceiveTimeout The actor that tunnel response messages to the
 *   client will be stopped after this time of inactivity.
 */
@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
final class ClusterReceptionistSettings(
    val role: Option[String],
    val numberOfContacts: Int,
    val responseTunnelReceiveTimeout: FiniteDuration)
    extends NoSerializationVerificationNeeded {

  def withRole(role: String): ClusterReceptionistSettings = copy(role = ClusterReceptionistSettings.roleOption(role))

  def withRole(role: Option[String]): ClusterReceptionistSettings = copy(role = role)

  def withNumberOfContacts(numberOfContacts: Int): ClusterReceptionistSettings =
    copy(numberOfContacts = numberOfContacts)

  def withResponseTunnelReceiveTimeout(responseTunnelReceiveTimeout: FiniteDuration): ClusterReceptionistSettings =
    copy(responseTunnelReceiveTimeout = responseTunnelReceiveTimeout)

  def withHeartbeat(
      heartbeatInterval: FiniteDuration,
      acceptableHeartbeatPause: FiniteDuration,
      failureDetectionInterval: FiniteDuration): ClusterReceptionistSettings =
    copy(
      heartbeatInterval = heartbeatInterval,
      acceptableHeartbeatPause = acceptableHeartbeatPause,
      failureDetectionInterval = failureDetectionInterval)

  // BEGIN BINARY COMPATIBILITY
  // The following is required in order to maintain binary
  // compatibility with 2.4. Post 2.4, the following 3 properties should
  // be moved to the class's constructor, and the following section of code
  // should be removed entirely.
  // TODO: ADDRESS FOR v.2.5

  def heartbeatInterval: FiniteDuration =
    _heartbeatInterval
  def acceptableHeartbeatPause: FiniteDuration =
    _acceptableHeartbeatPause
  def failureDetectionInterval: FiniteDuration =
    _failureDetectionInterval

  private var _heartbeatInterval: FiniteDuration = 2.seconds
  private var _acceptableHeartbeatPause: FiniteDuration = 13.seconds
  private var _failureDetectionInterval: FiniteDuration = 2.second

  def this(
      role: Option[String],
      numberOfContacts: Int,
      responseTunnelReceiveTimeout: FiniteDuration,
      heartbeatInterval: FiniteDuration,
      acceptableHeartbeatPause: FiniteDuration,
      failureDetectionInterval: FiniteDuration) = {
    this(role, numberOfContacts, responseTunnelReceiveTimeout)
    this._heartbeatInterval = heartbeatInterval
    this._acceptableHeartbeatPause = acceptableHeartbeatPause
    this._failureDetectionInterval = failureDetectionInterval
  }

  // END BINARY COMPATIBILITY

  private def copy(
      role: Option[String] = role,
      numberOfContacts: Int = numberOfContacts,
      responseTunnelReceiveTimeout: FiniteDuration = responseTunnelReceiveTimeout,
      heartbeatInterval: FiniteDuration = heartbeatInterval,
      acceptableHeartbeatPause: FiniteDuration = acceptableHeartbeatPause,
      failureDetectionInterval: FiniteDuration = failureDetectionInterval): ClusterReceptionistSettings =
    new ClusterReceptionistSettings(
      role,
      numberOfContacts,
      responseTunnelReceiveTimeout,
      heartbeatInterval,
      acceptableHeartbeatPause,
      failureDetectionInterval)
}

/**
 * Marker trait for remote messages with special serializer.
 */
sealed trait ClusterClientMessage extends Serializable

/**
 * Declares a super type for all events emitted by the `ClusterReceptionist`.
 * in relation to cluster clients being interacted with.
 */
sealed trait ClusterClientInteraction {
  val clusterClient: ActorRef
}

/**
 * Emitted to the Akka event stream when a cluster client has interacted with
 * a receptionist.
 */
final case class ClusterClientUp(override val clusterClient: ActorRef) extends ClusterClientInteraction

/**
 * Emitted to the Akka event stream when a cluster client was previously connected
 * but then not seen for some time.
 */
final case class ClusterClientUnreachable(override val clusterClient: ActorRef) extends ClusterClientInteraction

sealed abstract class SubscribeClusterClients

/**
 * Subscribe to a cluster receptionist's client interactions where
 * it is guaranteed that a sender receives the initial state
 * of contact points prior to any events in relation to them
 * changing.
 * The sender will automatically become unsubscribed when it
 * terminates.
 */
case object SubscribeClusterClients extends SubscribeClusterClients {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

sealed abstract class UnsubscribeClusterClients

/**
 * Explicitly unsubscribe from client interaction events.
 */
case object UnsubscribeClusterClients extends UnsubscribeClusterClients {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

sealed abstract class GetClusterClients

/**
 * Get the cluster clients known to this receptionist. A ``ClusterClients`` message
 * will be replied.
 */
case object GetClusterClients extends GetClusterClients {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * The reply to ``GetClusterClients``.
 *
 * @param clusterClients The presently known list of cluster clients.
 */
final case class ClusterClients(clusterClients: Set[ActorRef]) {
  import akka.util.ccompat.JavaConverters._

  /**
   * Java API
   */
  def getClusterClients: java.util.Set[ActorRef] =
    clusterClients.asJava
}

@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
object ClusterReceptionist {

  /**
   * Scala API: Factory method for `ClusterReceptionist` [[akka.actor.Props]].
   */
  def props(pubSubMediator: ActorRef, settings: ClusterReceptionistSettings): Props =
    Props(new ClusterReceptionist(pubSubMediator, settings)).withDeploy(Deploy.local)

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    @SerialVersionUID(1L)
    case object GetContacts extends ClusterClientMessage with DeadLetterSuppression
    @SerialVersionUID(1L)
    final case class Contacts(contactPoints: immutable.IndexedSeq[String]) extends ClusterClientMessage
    @SerialVersionUID(1L)
    case object Heartbeat extends ClusterClientMessage with DeadLetterSuppression
    @SerialVersionUID(1L)
    case object HeartbeatRsp extends ClusterClientMessage with DeadLetterSuppression
    @SerialVersionUID(1L)
    case object ReceptionistShutdown extends ClusterClientMessage with DeadLetterSuppression
    @SerialVersionUID(1L)
    case object Ping extends DeadLetterSuppression
    case object CheckDeadlines

    /**
     * Replies are tunneled via this actor, child of the receptionist, to avoid
     * inbound connections from other cluster nodes to the client.
     */
    class ClientResponseTunnel(client: ActorRef, timeout: FiniteDuration) extends Actor with ActorLogging {
      context.setReceiveTimeout(timeout)

      private val isAsk = {
        val pathElements = client.path.elements
        pathElements.size == 2 && pathElements.head == "temp" && pathElements.tail.head.startsWith("$")
      }

      def receive = {
        case Ping => // keep alive from client
        case ReceiveTimeout =>
          log.debug("ClientResponseTunnel for client [{}] stopped due to inactivity", client.path)
          context.stop(self)
        case msg =>
          client.tell(msg, Actor.noSender)
          if (isAsk)
            context.stop(self)
      }
    }
  }

}

/**
 * [[ClusterClient]] connects to this actor to retrieve. The `ClusterReceptionist` is
 * supposed to be started on all nodes, or all nodes with specified role, in the cluster.
 * The receptionist can be started with the [[ClusterClientReceptionist]] or as an
 * ordinary actor (use the factory method [[ClusterReceptionist#props]]).
 *
 * The receptionist forwards messages from the client to the associated [[akka.cluster.pubsub.DistributedPubSubMediator]],
 * i.e. the client can send messages to any actor in the cluster that is registered in the
 * `DistributedPubSubMediator`. Messages from the client are wrapped in
 * [[akka.cluster.pubsub.DistributedPubSubMediator.Send]], [[akka.cluster.pubsub.DistributedPubSubMediator.SendToAll]]
 * or [[akka.cluster.pubsub.DistributedPubSubMediator.Publish]] with the semantics described in
 * [[akka.cluster.pubsub.DistributedPubSubMediator]].
 *
 * Response messages from the destination actor are tunneled via the receptionist
 * to avoid inbound connections from other cluster nodes to the client, i.e.
 * the `sender()`, as seen by the destination actor, is not the client itself.
 * The `sender()` of the response messages, as seen by the client, is `deadLetters`
 * since the client should normally send subsequent messages via the `ClusterClient`.
 * It is possible to pass the original sender inside the reply messages if
 * the client is supposed to communicate directly to the actor in the cluster.
 *
 */
@deprecated(
  "Use Akka gRPC instead, see https://doc.akka.io/docs/akka/2.6/cluster-client.html#migration-to-akka-grpc",
  since = "2.6.0")
final class ClusterReceptionist(pubSubMediator: ActorRef, settings: ClusterReceptionistSettings)
    extends Actor
    with ActorLogging {

  import ClusterReceptionist.Internal._
  import DistributedPubSubMediator.{ Publish, Send, SendToAll }
  import settings._

  val cluster = Cluster(context.system)
  val verboseHeartbeat = cluster.settings.Debug.VerboseHeartbeatLogging
  import cluster.selfAddress

  require(role.forall(cluster.selfRoles.contains), s"This cluster member [$selfAddress] doesn't have the role [$role]")

  var nodes: immutable.SortedSet[Address] = {
    def hashFor(node: Address): Int = node match {
      // cluster node identifier is the host and port of the address; protocol and system is assumed to be the same
      case Address(_, _, Some(host), Some(port)) => MurmurHash.stringHash(s"$host:$port")
      case _ =>
        throw new IllegalStateException(s"Unexpected address without host/port: [$node]")
    }
    implicit val ringOrdering: Ordering[Address] = Ordering.fromLessThan[Address] { (a, b) =>
      val ha = hashFor(a)
      val hb = hashFor(b)
      ha < hb || (ha == hb && Member.addressOrdering.compare(a, b) < 0)
    }
    immutable.SortedSet()
  }
  val virtualNodesFactor = 10
  var consistentHash: ConsistentHash[Address] = ConsistentHash(nodes, virtualNodesFactor)

  var clientInteractions = HashMap.empty[ActorRef, DeadlineFailureDetector]
  var clientsPublished = HashSet.empty[ActorRef]

  var subscribers = Vector.empty[ActorRef]

  val checkDeadlinesTask =
    context.system.scheduler.scheduleWithFixedDelay(
      failureDetectionInterval,
      failureDetectionInterval,
      self,
      CheckDeadlines)(context.dispatcher)

  override def preStart(): Unit = {
    super.preStart()
    require(!cluster.isTerminated, "Cluster node must not be terminated")
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    checkDeadlinesTask.cancel()
    clientInteractions.keySet.foreach(_ ! ReceptionistShutdown)
  }

  def matchingRole(m: Member): Boolean = role.forall(m.hasRole)

  def responseTunnel(client: ActorRef): ActorRef = {
    val encName = URLEncoder.encode(client.path.toSerializationFormat, "utf-8")
    context.child(encName) match {
      case Some(tunnel) => tunnel
      case None =>
        context.actorOf(Props(classOf[ClientResponseTunnel], client, responseTunnelReceiveTimeout), encName)
    }
  }

  def receive = {
    case msg @ (_: Send | _: SendToAll | _: Publish) =>
      val tunnel = responseTunnel(sender())
      tunnel ! Ping // keep alive
      pubSubMediator.tell(msg, tunnel)

    case Heartbeat =>
      if (verboseHeartbeat) log.debug("Heartbeat from client [{}]", sender().path)
      sender() ! HeartbeatRsp
      updateClientInteractions(sender())

    case GetContacts =>
      // Consistent hashing is used to ensure that the reply to GetContacts
      // is the same from all nodes (most of the time) and it also
      // load balances the client connections among the nodes in the cluster.
      if (numberOfContacts >= nodes.size) {
        val contacts = Contacts(nodes.iterator.map(a => self.path.toStringWithAddress(a)).to(immutable.IndexedSeq))
        if (log.isDebugEnabled)
          log.debug(
            "Client [{}] gets contactPoints [{}] (all nodes)",
            sender().path,
            contacts.contactPoints.mkString(","))
        sender() ! contacts
      } else {
        // using toStringWithAddress in case the client is local, normally it is not, and
        // toStringWithAddress will use the remote address of the client
        val a = consistentHash.nodeFor(sender().path.toStringWithAddress(cluster.selfAddress))
        val slice = {
          val first = nodes.rangeFrom(a).tail.take(numberOfContacts)
          if (first.size == numberOfContacts) first
          else first.union(nodes.take(numberOfContacts - first.size))
        }
        val contacts = Contacts(slice.iterator.map(a => self.path.toStringWithAddress(a)).to(immutable.IndexedSeq))
        if (log.isDebugEnabled)
          log.debug("Client [{}] gets contactPoints [{}]", sender().path, contacts.contactPoints.mkString(","))
        sender() ! contacts
      }

    case state: CurrentClusterState =>
      nodes = nodes.empty.union(state.members.collect {
        case m if m.status != MemberStatus.Joining && matchingRole(m) => m.address
      })
      consistentHash = ConsistentHash(nodes, virtualNodesFactor)

    case MemberUp(m) =>
      if (matchingRole(m)) {
        nodes += m.address
        consistentHash = ConsistentHash(nodes, virtualNodesFactor)
      }

    case MemberRemoved(m, _) =>
      if (m.address == selfAddress)
        context.stop(self)
      else if (matchingRole(m)) {
        nodes -= m.address
        consistentHash = ConsistentHash(nodes, virtualNodesFactor)
      }

    case _: MemberEvent => // not of interest

    case SubscribeClusterClients =>
      val subscriber = sender()
      subscriber ! ClusterClients(clientInteractions.keySet.to(HashSet))
      subscribers :+= subscriber
      context.watch(subscriber)

    case UnsubscribeClusterClients =>
      val subscriber = sender()
      subscribers = subscribers.filterNot(_ == subscriber)

    case Terminated(subscriber) =>
      self.tell(UnsubscribeClusterClients, subscriber)

    case GetClusterClients =>
      sender() ! ClusterClients(clientInteractions.keySet.to(HashSet))

    case CheckDeadlines =>
      clientInteractions = clientInteractions.filter {
        case (_, failureDetector) =>
          failureDetector.isAvailable
      }
      publishClientsUnreachable()
  }

  def updateClientInteractions(client: ActorRef): Unit =
    clientInteractions.get(client) match {
      case Some(failureDetector) =>
        failureDetector.heartbeat()
      case None =>
        val failureDetector = new DeadlineFailureDetector(acceptableHeartbeatPause, heartbeatInterval)
        failureDetector.heartbeat()
        clientInteractions = clientInteractions + (client -> failureDetector)
        log.debug("Received new contact from [{}]", client.path)
        val clusterClientUp = ClusterClientUp(client)
        subscribers.foreach(_ ! clusterClientUp)
        clientsPublished = clientInteractions.keySet.to(HashSet)
    }

  def publishClientsUnreachable(): Unit = {
    val publishableClients = clientInteractions.keySet.to(HashSet)
    for (c <- clientsPublished if !publishableClients.contains(c)) {
      log.debug("Lost contact with [{}]", c.path)
      val clusterClientUnreachable = ClusterClientUnreachable(c)
      subscribers.foreach(_ ! clusterClientUnreachable)
    }
    clientsPublished = publishableClients
  }
}
