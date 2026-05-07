/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import akka.annotation.InternalApi
import java.util.concurrent.CompletionStage
import java.util.regex.Pattern

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.FutureOps
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.language.implicitConversions
import scala.util.Success

import akka.pattern.ask
import akka.routing.MurmurHash
import akka.util.{ Helpers, Timeout }

/**
 * An ActorSelection is a logical view of a section of an ActorSystem's tree of Actors,
 * allowing for broadcasting of messages to that section.
 */
@SerialVersionUID(1L)
abstract class ActorSelection extends Serializable {
  this: ScalaActorSelection =>

  protected[akka] val anchor: ActorRef

  protected val path: immutable.IndexedSeq[SelectionPathElement]

  /**
   * Sends the specified message to this ActorSelection, i.e. fire-and-forget
   * semantics, including the sender reference if possible.
   *
   * Pass [[ActorRef#noSender]] or `null` as sender if there is nobody to reply to
   */
  def tell(msg: Any, sender: ActorRef): Unit =
    ActorSelection.deliverSelection(
      anchor.asInstanceOf[InternalActorRef],
      sender,
      ActorSelectionMessage(msg, path, wildcardFanOut = false))

  /**
   * Forwards the message and passes the original sender actor as the sender.
   *
   * Works, no matter whether originally sent with tell/'!' or ask/'?'.
   */
  def forward(message: Any)(implicit context: ActorContext): Unit = tell(message, context.sender())

  /**
   * Resolve the [[ActorRef]] matching this selection.
   * The result is returned as a Future that is completed with the [[ActorRef]]
   * if such an actor exists. It is completed with failure [[ActorNotFound]] if
   * no such actor exists or the identification didn't complete within the
   * supplied `timeout`.
   *
   * Under the hood it talks to the actor to verify its existence and acquire its
   * [[ActorRef]].
   */
  def resolveOne()(implicit timeout: Timeout): Future[ActorRef] = {
    implicit val ec = ExecutionContext.parasitic
    val p = Promise[ActorRef]()
    this.ask(Identify(None)).onComplete {
      case Success(ActorIdentity(_, Some(ref))) => p.success(ref)
      case _                                    => p.failure(ActorNotFound(this))
    }
    p.future
  }

  /**
   * Resolve the [[ActorRef]] matching this selection.
   * The result is returned as a Future that is completed with the [[ActorRef]]
   * if such an actor exists. It is completed with failure [[ActorNotFound]] if
   * no such actor exists or the identification didn't complete within the
   * supplied `timeout`.
   *
   * Under the hood it talks to the actor to verify its existence and acquire its
   * [[ActorRef]].
   */
  def resolveOne(timeout: FiniteDuration): Future[ActorRef] = resolveOne()(timeout)

  /**
   * Java API for [[#resolveOne]]
   *
   * Resolve the [[ActorRef]] matching this selection.
   * The result is returned as a CompletionStage that is completed with the [[ActorRef]]
   * if such an actor exists. It is completed with failure [[ActorNotFound]] if
   * no such actor exists or the identification didn't complete within the
   * supplied `timeout`.
   *
   */
  def resolveOne(timeout: java.time.Duration): CompletionStage[ActorRef] = {
    resolveOne(timeout.toScala).asJava
  }

  override def toString: String = {
    val builder = new java.lang.StringBuilder()
    builder.append("ActorSelection[Anchor(").append(anchor.path)
    if (anchor.path.uid != ActorCell.undefinedUid)
      builder.append("#").append(anchor.path.uid)

    builder.append("), Path(").append(path.mkString("/", "/", "")).append(")]")
    builder.toString
  }

  /**
   * The [[akka.actor.ActorPath]] of the anchor actor.
   */
  def anchorPath: ActorPath = anchor.path

  /**
   * String representation of the path elements, starting with "/" and separated with "/".
   */
  def pathString: String = path.mkString("/", "/", "")

  /**
   * String representation of the actor selection suitable for storage and recreation.
   * The output is similar to the URI fragment returned by [[akka.actor.ActorPath#toSerializationFormat]].
   * @return URI fragment
   */
  def toSerializationFormat: String = {
    val anchorPath = anchor match {
      case a: ActorRefWithCell => anchor.path.toStringWithAddress(a.provider.getDefaultAddress)
      case _                   => anchor.path.toString
    }

    val builder = new java.lang.StringBuilder()
    builder.append(anchorPath)
    val lastChar = builder.charAt(builder.length - 1)
    if (path.nonEmpty && lastChar != '/')
      builder.append(path.mkString("/", "/", ""))
    else if (path.nonEmpty)
      builder.append(path.mkString("/"))
    builder.toString
  }

  override def equals(obj: Any): Boolean = obj match {
    case s: ActorSelection => this.anchor == s.anchor && this.path == s.path
    case _                 => false
  }

  override lazy val hashCode: Int = {
    import MurmurHash._
    var h = startHash(anchor.##)
    h = extendHash(h, path.##, startMagicA, startMagicB)
    finalizeHash(h)
  }
}

/**
 * An ActorSelection is a logical view of a section of an ActorSystem's tree of Actors,
 * allowing for broadcasting of messages to that section.
 */
object ActorSelection {
  //This cast is safe because the self-type of ActorSelection requires that it mixes in ScalaActorSelection
  implicit def toScala(sel: ActorSelection): ScalaActorSelection = sel.asInstanceOf[ScalaActorSelection]

  /**
   * Construct an ActorSelection from the given string representing a path
   * relative to the given target. This operation has to create all the
   * matching magic, so it is preferable to cache its result if the
   * intention is to send messages frequently.
   */
  def apply(anchorRef: ActorRef, path: String): ActorSelection = apply(anchorRef, path.split("/+"))

  /**
   * Construct an ActorSelection from the given string representing a path
   * relative to the given target. This operation has to create all the
   * matching magic, so it is preferable to cache its result if the
   * intention is to send messages frequently.
   */
  def apply(anchorRef: ActorRef, elements: Iterable[String]): ActorSelection = {
    val compiled: immutable.IndexedSeq[SelectionPathElement] = elements.iterator
      .collect {
        case x if x.nonEmpty =>
          if ((x.indexOf('?') != -1) || (x.indexOf('*') != -1)) SelectChildPattern(x)
          else if (x == "..") SelectParent
          else SelectChildName(x)
      }
      .to(immutable.IndexedSeq)
    new ActorSelection with ScalaActorSelection {
      override val anchor = anchorRef
      override val path = compiled
    }
  }

  /**
   * INTERNAL API
   * The receive logic for ActorSelectionMessage. The idea is to recursively descend as far as possible
   * with local refs and hand over to that “foreign” child when we encounter it.
   */
  private[akka] def deliverSelection(anchor: InternalActorRef, sender: ActorRef, sel: ActorSelectionMessage): Unit =
    if (sel.elements.isEmpty)
      anchor.tell(sel.msg, sender)
    else {

      val iter = sel.elements.iterator

      @tailrec def rec(ref: InternalActorRef): Unit = {
        ref match {
          case refWithCell: ActorRefWithCell =>
            def emptyRef =
              new EmptyLocalActorRef(
                refWithCell.provider,
                anchor.path / sel.elements.map(_.toString),
                refWithCell.underlying.system.eventStream)

            iter.next() match {
              case SelectParent =>
                val parent = ref.getParent
                if (iter.isEmpty)
                  parent.tell(sel.msg, sender)
                else
                  rec(parent)
              case SelectChildName(name) =>
                val child = refWithCell.getSingleChild(name)
                if (child == Nobody) {
                  // don't send to emptyRef after wildcard fan-out
                  if (!sel.wildcardFanOut) emptyRef.tell(sel, sender)
                } else if (iter.isEmpty)
                  child.tell(sel.msg, sender)
                else
                  rec(child)
              case p: SelectChildPattern =>
                // fan-out when there is a wildcard
                val chldr = refWithCell.children
                if (iter.isEmpty) {
                  // leaf
                  val matchingChildren = chldr.filter(c => p.pattern.matcher(c.path.name).matches)
                  if (matchingChildren.isEmpty && !sel.wildcardFanOut)
                    emptyRef.tell(sel, sender)
                  else
                    matchingChildren.foreach(_.tell(sel.msg, sender))
                } else {
                  val matchingChildren = chldr.filter(c => p.pattern.matcher(c.path.name).matches)
                  // don't send to emptyRef after wildcard fan-out
                  if (matchingChildren.isEmpty && !sel.wildcardFanOut)
                    emptyRef.tell(sel, sender)
                  else {
                    val m = sel.copy(
                      elements = iter.toVector,
                      wildcardFanOut = sel.wildcardFanOut || matchingChildren.size > 1)
                    matchingChildren.foreach(c => deliverSelection(c.asInstanceOf[InternalActorRef], sender, m))
                  }
                }
            }

          case _ =>
            // foreign ref, continue by sending ActorSelectionMessage to it with remaining elements
            ref.tell(sel.copy(elements = iter.toVector), sender)
        }
      }

      rec(anchor)
    }
}

/**
 * Contains the Scala API (!-method) for ActorSelections) which provides automatic tracking of the sender,
 * as per the usual implicit ActorRef pattern.
 */
trait ScalaActorSelection {
  this: ActorSelection =>

  def !(msg: Any)(implicit sender: ActorRef = Actor.noSender): Unit = tell(msg, sender)
}

/**
 * INTERNAL API
 * ActorRefFactory.actorSelection returns a ActorSelection which sends these
 * nested path descriptions whenever using ! on them, the idea being that the
 * message is delivered by traversing the various actor paths involved.
 */
@SerialVersionUID(2L) // it has protobuf serialization in akka-remote
private[akka] final case class ActorSelectionMessage(
    msg: Any,
    elements: immutable.Iterable[SelectionPathElement],
    wildcardFanOut: Boolean)
    extends AutoReceivedMessage
    with PossiblyHarmful
    with WrappedMessage {

  def identifyRequest: Option[Identify] = msg match {
    case x: Identify => Some(x)
    case _           => None
  }

  override def message: Any = msg
}

/**
 * INTERNAL API
 */
@nowarn("msg=@SerialVersionUID has no effect on traits")
@SerialVersionUID(1L)
private[akka] sealed trait SelectionPathElement

/**
 * INTERNAL API
 */
@InternalApi
@SerialVersionUID(2L)
private[akka] final case class SelectChildName(name: String) extends SelectionPathElement {
  override def toString: String = name
}

/**
 * INTERNAL API
 */
@InternalApi
@SerialVersionUID(2L)
private[akka] final case class SelectChildPattern(patternStr: String) extends SelectionPathElement {
  val pattern: Pattern = Helpers.makePattern(patternStr)
  override def toString: String = patternStr
}

/**
 * INTERNAL API
 */
@InternalApi
@SerialVersionUID(2L)
private[akka] case object SelectParent extends SelectionPathElement {
  override def toString: String = ".."
}

/**
 * When [[ActorSelection#resolveOne]] can't identify the actor the
 * `Future` is completed with this failure.
 */
@SerialVersionUID(1L)
final case class ActorNotFound(selection: ActorSelection) extends RuntimeException("Actor not found for: " + selection)
