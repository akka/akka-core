/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import java.net.URISyntaxException
import java.util.concurrent.TimeoutException

import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.actor._
import akka.persistence.{
  AtomicWrite,
  DeleteMessagesFailure,
  DeleteSnapshotFailure,
  DeleteSnapshotsFailure,
  JournalProtocol,
  NonPersistentRepr,
  Persistence,
  SaveSnapshotFailure,
  SnapshotProtocol
}
import akka.util.Helpers.Requiring

object PersistencePluginProxy {
  final case class TargetLocation(address: Address)
  private case object InitTimeout

  def setTargetLocation(system: ActorSystem, address: Address): Unit = {
    Persistence(system).journalFor(null) ! TargetLocation(address)
    if (system.settings.config.getString("akka.persistence.snapshot-store.plugin") != "")
      Persistence(system).snapshotStoreFor(null) ! TargetLocation(address)
  }

  def start(system: ActorSystem): Unit = {
    Persistence(system).journalFor(null)
    if (system.settings.config.getString("akka.persistence.snapshot-store.plugin") != "")
      Persistence(system).snapshotStoreFor(null)

  }

  private sealed trait PluginType {
    def qualifier: String
  }
  private case object Journal extends PluginType {
    override def qualifier: String = "journal"
  }
  private case object SnapshotStore extends PluginType {
    override def qualifier: String = "snapshot-store"
  }
}

/**
 * PersistencePluginProxyExtensionImpl is an `Extension` that enables initialization of the `PersistencePluginProxy`
 * via configuration, without requiring any code changes or the creation of any actors.
 * @param system The actor system to initialize the extension for
 */
class PersistencePluginProxyExtensionImpl(system: ActorSystem) extends Extension {
  PersistencePluginProxy.start(system)
}

object PersistencePluginProxyExtension
    extends ExtensionId[PersistencePluginProxyExtensionImpl]
    with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): PersistencePluginProxyExtensionImpl =
    new PersistencePluginProxyExtensionImpl(system)
  override def lookup: ExtensionId[_ <: Extension] = PersistencePluginProxyExtension
  override def get(system: ActorSystem): PersistencePluginProxyExtensionImpl = super.get(system)
  override def get(system: ClassicActorSystemProvider): PersistencePluginProxyExtensionImpl = super.get(system)
}

final class PersistencePluginProxy(config: Config) extends Actor with Stash with ActorLogging {
  import JournalProtocol._
  import PersistencePluginProxy._
  import SnapshotProtocol._

  private val pluginId = self.path.name
  private val pluginType: PluginType = pluginId match {
    case "akka.persistence.journal.proxy"        => Journal
    case "akka.persistence.snapshot-store.proxy" => SnapshotStore
    case other =>
      throw new IllegalArgumentException("Unknown plugin type: " + other)
  }

  private val initTimeout: FiniteDuration = config.getDuration("init-timeout", MILLISECONDS).millis
  private val targetPluginId: String = {
    val key = s"target-${pluginType.qualifier}-plugin"
    config.getString(key).requiring(_ != "", s"$pluginId.$key must be defined")
  }
  private val startTarget: Boolean = config.getBoolean(s"start-target-${pluginType.qualifier}")

  override def preStart(): Unit = {
    if (startTarget) {
      val target = pluginType match {
        case Journal =>
          log.info("Starting target journal [{}]", targetPluginId)
          Persistence(context.system).journalFor(targetPluginId)
        case SnapshotStore =>
          log.info("Starting target snapshot-store [{}]", targetPluginId)
          Persistence(context.system).snapshotStoreFor(targetPluginId)
      }
      context.become(active(target, targetAtThisNode = true))
    } else {
      val targetAddressKey = s"target-${pluginType.qualifier}-address"
      val targetAddress = config.getString(targetAddressKey)
      if (targetAddress != "") {
        try {
          log.info("Setting target {} address to {}", pluginType.qualifier, targetAddress)
          PersistencePluginProxy.setTargetLocation(context.system, AddressFromURIString(targetAddress))
        } catch {
          case _: URISyntaxException =>
            log.warning("Invalid URL provided for target {} address: {}", pluginType.qualifier, targetAddress)
        }
      }

      context.system.scheduler.scheduleOnce(initTimeout, self, InitTimeout)(context.dispatcher)
    }
  }

  private val selfAddress: Address =
    context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

  private def timeoutException() =
    new TimeoutException(
      s"Target ${pluginType.qualifier} not initialized. " +
      s"Use `PersistencePluginProxy.setTargetLocation` or set `target-${pluginType.qualifier}-address`")

  def receive = init

  def init: Receive = {
    case TargetLocation(address) =>
      context.setReceiveTimeout(1.second) // for retries
      context.become(identifying(address))
    case InitTimeout =>
      log.info(
        "Initialization timed-out (after {}), Use `PersistencePluginProxy.setTargetLocation` or set `target-{}-address`",
        initTimeout,
        pluginType.qualifier)
      context.become(initTimedOut)
      unstashAll() // will trigger appropriate failures
    case Terminated(_) =>
    case _ =>
      stash()
  }

  def becomeIdentifying(address: Address): Unit = {
    sendIdentify(address)
    context.setReceiveTimeout(1.second) // for retries
    context.become(identifying(address))
  }

  def sendIdentify(address: Address): Unit = {
    val sel = context.actorSelection(RootActorPath(address) / "system" / targetPluginId)
    log.info("Trying to identify target {} at {}", pluginType.qualifier, sel)
    sel ! Identify(targetPluginId)
  }

  def identifying(address: Address): Receive =
    ({
      case ActorIdentity(`targetPluginId`, Some(target)) =>
        log.info("Found target {} at [{}]", pluginType.qualifier, address)
        context.setReceiveTimeout(Duration.Undefined)
        context.watch(target)
        unstashAll()
        context.become(active(target, address == selfAddress))
      case _: ActorIdentity => // will retry after ReceiveTimeout
      case Terminated(_)    =>
      case ReceiveTimeout =>
        sendIdentify(address)
    }: Receive).orElse(init)

  def active(targetJournal: ActorRef, targetAtThisNode: Boolean): Receive = {
    case TargetLocation(address) =>
      if (targetAtThisNode && address != selfAddress)
        becomeIdentifying(address)
    case Terminated(`targetJournal`) =>
      context.unwatch(targetJournal)
      context.become(initTimedOut)
    case Terminated(_) =>
    case InitTimeout   =>
    case msg =>
      targetJournal.forward(msg)
  }

  def initTimedOut: Receive = {

    case req: JournalProtocol.Request =>
      req match { // exhaustive match
        case WriteMessages(messages, persistentActor, actorInstanceId, _) =>
          val atomicWriteCount = messages.count(_.isInstanceOf[AtomicWrite])
          persistentActor ! WriteMessagesFailed(timeoutException(), atomicWriteCount)
          messages.foreach {
            case a: AtomicWrite =>
              a.payload.foreach { p =>
                persistentActor ! WriteMessageFailure(p, timeoutException(), actorInstanceId)
              }
            case r: NonPersistentRepr =>
              persistentActor ! LoopMessageSuccess(r.payload, actorInstanceId)
          }
        case ReplayMessages(_, _, _, _, persistentActor) =>
          persistentActor ! ReplayMessagesFailure(timeoutException())
        case DeleteMessagesTo(_, toSequenceNr, persistentActor) =>
          persistentActor ! DeleteMessagesFailure(timeoutException(), toSequenceNr)
      }

    case req: SnapshotProtocol.Request =>
      req match { // exhaustive match
        case _: LoadSnapshot =>
          sender() ! LoadSnapshotFailed(timeoutException())
        case SaveSnapshot(metadata, _) =>
          sender() ! SaveSnapshotFailure(metadata, timeoutException())
        case DeleteSnapshot(metadata) =>
          sender() ! DeleteSnapshotFailure(metadata, timeoutException())
        case DeleteSnapshots(_, criteria) =>
          sender() ! DeleteSnapshotsFailure(criteria, timeoutException())
      }

    case TargetLocation(address) =>
      becomeIdentifying(address)

    case Terminated(_) =>
    case _ =>
      val e = timeoutException()
      log.error(e, "Failed PersistencePluginProxy request: {}", e.getMessage)
  }

}
