/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import java.util.concurrent.ConcurrentHashMap

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.control.NonFatal

import akka.actor.{
  Actor,
  ActorPath,
  ActorPathExtractor,
  ActorRef,
  ActorSystemImpl,
  AddressTerminated,
  Deploy,
  InternalActorRef,
  Nobody,
  Props,
  VirtualPathContainer
}
import akka.actor.ActorIdentity
import akka.actor.ActorRefScope
import akka.actor.ActorRefWithCell
import akka.actor.ActorSelectionMessage
import akka.actor.EmptyLocalActorRef
import akka.actor.Identify
import akka.actor.SelectChildName
import akka.actor.SelectChildPattern
import akka.actor.SelectParent
import akka.dispatch.sysmsg.{ DeathWatchNotification, SystemMessage, Watch }
import akka.dispatch.sysmsg.Unwatch
import akka.event.{ AddressTerminatedTopic, LogMarker, MarkerLoggingAdapter }
import akka.util.Switch

/**
 * INTERNAL API
 */
private[akka] sealed trait DaemonMsg

/**
 * INTERNAL API
 */
@SerialVersionUID(1L)
private[akka] final case class DaemonMsgCreate(props: Props, deploy: Deploy, path: String, supervisor: ActorRef)
    extends DaemonMsg

/**
 * INTERNAL API
 *
 * Internal system "daemon" actor for remote internal communication.
 *
 * It acts as the brain of the remote that responds to system remote events (messages) and undertakes action.
 */
private[akka] class RemoteSystemDaemon(
    system: ActorSystemImpl,
    _path: ActorPath,
    _parent: InternalActorRef,
    terminator: ActorRef,
    _log: MarkerLoggingAdapter,
    val untrustedMode: Boolean)
    extends VirtualPathContainer(system.provider, _path, _parent, _log) {

  import akka.actor.SystemGuardian._

  private val terminating = new Switch(startAsOn = false)

  AddressTerminatedTopic(system).subscribe(this)

  private val parent2children = new ConcurrentHashMap[ActorRef, Set[ActorRef]]

  private val allowListEnabled = system.settings.config.getBoolean("akka.remote.deployment.enable-allow-list")
  private val remoteDeploymentAllowList: immutable.Set[String] = {
    import scala.jdk.CollectionConverters._
    if (allowListEnabled)
      system.settings.config.getStringList("akka.remote.deployment.allowed-actor-classes").asScala.toSet
    else Set.empty
  }

  @tailrec private def addChildParentNeedsWatch(parent: ActorRef, child: ActorRef): Boolean =
    parent2children.get(parent) match {
      case null =>
        if (parent2children.putIfAbsent(parent, Set(child)) == null) true
        else addChildParentNeedsWatch(parent, child)
      case children =>
        if (parent2children.replace(parent, children, children + child)) false
        else addChildParentNeedsWatch(parent, child)
    }

  @tailrec private def removeChildParentNeedsUnwatch(parent: ActorRef, child: ActorRef): Boolean = {
    parent2children.get(parent) match {
      case null => false // no-op
      case children =>
        val next = children - child
        if (next.isEmpty) {
          if (!parent2children.remove(parent, children)) removeChildParentNeedsUnwatch(parent, child)
          else true
        } else {
          if (!parent2children.replace(parent, children, next)) removeChildParentNeedsUnwatch(parent, child)
          else false
        }
    }
  }

  /**
   * Find the longest matching path which we know about and return that ref
   * (or ask that ref to continue searching if elements are left).
   */
  override def getChild(names: Iterator[String]): InternalActorRef = {

    @tailrec
    def rec(s: String, n: Int): (InternalActorRef, Int) = {
      import akka.actor.ActorCell._
      val (childName, uid) = splitNameAndUid(s)
      getChild(childName) match {
        case null =>
          val last = s.lastIndexOf('/')
          if (last == -1) (Nobody, n)
          else rec(s.substring(0, last), n + 1)
        case ref if uid != undefinedUid && uid != ref.path.uid => (Nobody, n)
        case ref                                               => (ref, n)
      }
    }

    val full = Vector() ++ names
    rec(full.mkString("/"), 0) match {
      case (Nobody, _) => Nobody
      case (ref, 0)    => ref
      case (ref, n)    => ref.getChild(full.takeRight(n).iterator)
    }
  }

  override def sendSystemMessage(message: SystemMessage): Unit = message match {
    case DeathWatchNotification(child: ActorRefWithCell with ActorRefScope, _, _) if child.isLocal =>
      terminating.locked {
        removeChild(child.path.elements.drop(1).mkString("/"), child)
        val parent = child.getParent
        if (removeChildParentNeedsUnwatch(parent, child)) parent.sendSystemMessage(Unwatch(parent, this))
        terminationHookDoneWhenNoChildren()
      }
    case DeathWatchNotification(parent: ActorRef with ActorRefScope, _, _) if !parent.isLocal =>
      terminating.locked {
        parent2children.remove(parent) match {
          case null =>
          case children =>
            for (c <- children) {
              system.stop(c)
              removeChild(c.path.elements.drop(1).mkString("/"), c)
            }
            terminationHookDoneWhenNoChildren()
        }
      }
    case _ => super.sendSystemMessage(message)
  }

  override def !(msg: Any)(implicit sender: ActorRef = Actor.noSender): Unit =
    try msg match {
      case message: DaemonMsg =>
        log.debug("Received command [{}] to RemoteSystemDaemon on [{}]", message, path.address)
        message match {
          case DaemonMsgCreate(_, _, path, _) if untrustedMode =>
            log.debug("does not accept deployments (untrusted) for [{}]", path) // TODO add security marker?

          case DaemonMsgCreate(props, deploy, path, supervisor) if allowListEnabled =>
            val name = props.clazz.getCanonicalName
            if (remoteDeploymentAllowList.contains(name))
              doCreateActor(message, props, deploy, path, supervisor)
            else {
              val ex =
                new NotAllowedClassRemoteDeploymentAttemptException(props.actorClass(), remoteDeploymentAllowList)
              log.error(
                LogMarker.Security,
                ex,
                "Received command to create remote Actor, but class [{}] is not allow-listed! " +
                "Target path: [{}]",
                props.actorClass(),
                path)
            }
          case DaemonMsgCreate(props, deploy, path, supervisor) =>
            doCreateActor(message, props, deploy, path, supervisor)
        }

      case sel: ActorSelectionMessage =>
        val (concatenatedChildNames, m) = {
          val iter = sel.elements.iterator
          // find child elements, and the message to send, which is a remaining ActorSelectionMessage
          // in case of SelectChildPattern, otherwise the actual message of the selection
          @tailrec def rec(acc: List[String]): (List[String], Any) =
            if (iter.isEmpty)
              (acc.reverse, sel.msg)
            else {
              iter.next() match {
                case SelectChildName(name)       => rec(name :: acc)
                case SelectParent if acc.isEmpty => rec(acc)
                case SelectParent                => rec(acc.tail)
                case pat: SelectChildPattern     => (acc.reverse, sel.copy(elements = pat +: iter.toVector))
              }
            }
          rec(Nil)
        }
        getChild(concatenatedChildNames.iterator) match {
          case Nobody =>
            val emptyRef =
              new EmptyLocalActorRef(system.provider, path / sel.elements.map(_.toString), system.eventStream)
            emptyRef.tell(sel, sender)
          case child =>
            child.tell(m, sender)
        }

      case Identify(messageId) => sender ! ActorIdentity(messageId, Some(this))

      case TerminationHook =>
        terminating.switchOn {
          terminationHookDoneWhenNoChildren()
          foreachChild { system.stop }
        }

      case AddressTerminated(address) =>
        foreachChild {
          case a: InternalActorRef if a.getParent.path.address == address => system.stop(a)
          case _                                                          => // skip, this child doesn't belong to the terminated address
        }

      case unknown => log.warning(LogMarker.Security, "Unknown message [{}] received by [{}]", unknown, this)

    } catch {
      case NonFatal(e) => log.error(e, "exception while processing remote command [{}] from [{}]", msg, sender)
    }

  private def doCreateActor(message: DaemonMsg, props: Props, deploy: Deploy, path: String, supervisor: ActorRef) = {
    path match {
      case ActorPathExtractor(_, elems) if elems.nonEmpty && elems.head == "remote" =>
        // TODO RK currently the extracted “address” is just ignored, is that okay?
        // TODO RK canonicalize path so as not to duplicate it always #1446
        val subpath = elems.drop(1)
        val p = this.path / subpath
        val childName = {
          val s = subpath.mkString("/")
          val i = s.indexOf('#')
          if (i < 0) s
          else s.substring(0, i)
        }
        val isTerminating = !terminating.whileOff {
          val parent = supervisor.asInstanceOf[InternalActorRef]
          val actor = system.provider.actorOf(
            system,
            props,
            parent,
            p,
            systemService = false,
            Some(deploy),
            lookupDeploy = true,
            async = false)
          addChild(childName, actor)
          actor.sendSystemMessage(Watch(actor, this))
          actor.start()
          if (addChildParentNeedsWatch(parent, actor)) parent.sendSystemMessage(Watch(parent, this))
        }
        if (isTerminating)
          log.error("Skipping [{}] to RemoteSystemDaemon on [{}] while terminating", message, p.address)
      case _ =>
        log.debug("remote path does not match path from message [{}]", message)
    }
  }

  def terminationHookDoneWhenNoChildren(): Unit = terminating.whileOn {
    if (!hasChildren) terminator.tell(TerminationHookDone, this)
  }

}

/** INTERNAL API */
final class NotAllowedClassRemoteDeploymentAttemptException(illegal: Class[_], allowedClassNames: immutable.Set[String])
    extends RuntimeException(
      s"Attempted to deploy Actor class: " +
      s"[$illegal], " +
      s"which is not allowed, allowed classes: [${allowedClassNames.mkString(", ")}]")
