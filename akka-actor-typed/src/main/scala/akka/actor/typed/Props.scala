/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.annotation.tailrec
import scala.annotation.varargs
import scala.reflect.ClassTag

import akka.actor.typed.internal.PropsImpl._
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import scala.jdk.CollectionConverters._

object Props {

  /**
   * Empty props instance, should seldom be needed in user code but can be useful as a default props where
   * custom configuration of an actor is possible.
   */
  val empty: Props = EmptyProps
}

/**
 * Data structure for describing an actor’s props details like which
 * executor to run it on. For each type of setting (e.g. [[DispatcherSelector]])
 * the FIRST occurrence is used when creating the
 * actor; this means that adding configuration using the "with" methods
 * overrides what was configured previously.
 *
 * Deliberately not sealed in order to emphasize future extensibility by the
 * framework—this is not intended to be extended by user code.
 *
 * Not for user extension.
 */
@DoNotInherit
abstract class Props private[akka] () extends Product with Serializable {

  /**
   * Reference to the tail of this Props list.
   *
   * The `next` reference is here so that it can form an
   * internally linked list. Traversal of this list stops when encountering the
   * [[EmptyProps]] object.
   *
   * INTERNAL API
   */
  @InternalApi
  private[akka] def next: Props

  /**
   * Create a copy of this Props node with its `next` reference
   * replaced by the given object. <b>This does NOT append the given list
   * of configuration nodes to the current list!</b>
   *
   * INTERNAL API
   */
  @InternalApi
  private[akka] def withNext(next: Props): Props

  /**
   * Prepend a selection of the [[ActorSystem]] default executor to this Props.
   */
  def withDispatcherDefault: Props = DispatcherDefault(this)

  /**
   * Prepend a selection of the executor found at the given Config path to this Props.
   * The path is relative to the configuration root of the [[ActorSystem]] that looks up the
   * executor.
   */
  def withDispatcherFromConfig(path: String): Props = DispatcherFromConfig(path, this)

  /**
   * Prepend a selection of the same executor as the parent actor to this Props.
   */
  def withDispatcherSameAsParent: Props = DispatcherSameAsParent(this)

  /**
   * Prepend a selection of the mailbox found at the given Config path to this Props.
   * The path is relative to the configuration root of the [[ActorSystem]] that looks up the
   * mailbox.
   */
  def withMailboxFromConfig(path: String): Props = MailboxFromConfigSelector(path, this)

  /**
   * Find the first occurrence of a configuration node of the given type, falling
   * back to the provided default if none is found.
   *
   * INTERNAL API
   */
  @InternalApi
  private[akka] def firstOrElse[T <: Props: ClassTag](default: T): T = {
    @tailrec def rec(d: Props): T = {
      d match {
        case EmptyProps => default
        case t: T       => t
        case _          => rec(d.next)
      }
    }
    rec(this)
  }

  /**
   * Retrieve all configuration nodes of a given type in the order that they
   * are present in this Props. The `next` reference for all returned
   * nodes will be the [[EmptyProps]].
   *
   * INTERNAL API
   */
  @InternalApi
  private[akka] def allOf[T <: Props: ClassTag]: List[Props] = {
    @tailrec def select(d: Props, acc: List[Props]): List[Props] =
      d match {
        case EmptyProps => acc.reverse
        case _: T       => select(d.next, (d.withNext(EmptyProps)) :: acc)
        case _          => select(d.next, acc)
      }
    select(this, Nil)
  }

  /**
   * Remove all configuration nodes of a given type and return the resulting
   * Props.
   */
  @InternalApi
  private[akka] def filterNot[T <: Props: ClassTag]: Props = {
    @tailrec def select(d: Props, acc: List[Props]): List[Props] =
      d match {
        case EmptyProps => acc
        case _: T       => select(d.next, acc)
        case _          => select(d.next, d :: acc)
      }
    @tailrec def link(l: List[Props], acc: Props): Props =
      l match {
        case d :: ds => link(ds, d.withNext(acc))
        case Nil     => acc
      }
    link(select(this, Nil), EmptyProps)
  }
}

/**
 * Not for user extension.
 */
@DoNotInherit
abstract class DispatcherSelector extends Props

/**
 * Factories for [[DispatcherSelector]]s which describe which thread pool shall be used to run
 * the actor to which this configuration is applied. See the individual factory methods for details
 * on the options.
 *
 * The default configuration if none of these options are present is to run
 * the actor on the default [[ActorSystem]] executor.
 */
object DispatcherSelector {

  /**
   * Scala API:
   * Run the actor on the default [[ActorSystem]] executor.
   */
  def default(): DispatcherSelector = DispatcherDefault.empty

  /**
   * Java API:
   * Run the actor on the default [[ActorSystem]] executor.
   */
  def defaultDispatcher(): DispatcherSelector = default()

  /**
   *  Run the actor on the default blocking dispatcher that is
   *  configured under default-blocking-io-dispatcher
   */
  def blocking(): DispatcherSelector = fromConfig("akka.actor.default-blocking-io-dispatcher")

  /**
   * Look up an executor definition in the [[ActorSystem]] configuration.
   * ExecutorServices created in this fashion will be shut down when the
   * ActorSystem terminates.
   */
  def fromConfig(path: String): DispatcherSelector = DispatcherFromConfig(path)

  /**
   * Run the actor on the same executor as the parent actor.
   * @return
   */
  def sameAsParent(): DispatcherSelector = DispatcherSameAsParent.empty
}

/**
 * Not for user extension.
 */
@DoNotInherit
abstract class MailboxSelector extends Props

object MailboxSelector {

  /**
   * Scala API: The default mailbox is SingleConsumerOnlyUnboundedMailbox
   */
  def default(): MailboxSelector = fromConfig("akka.actor.typed.default-mailbox")

  /**
   * Java API: The default mailbox is SingleConsumerOnlyUnboundedMailbox
   */
  def defaultMailbox(): MailboxSelector = default()

  /**
   * A mailbox with a max capacity after which new messages are dropped (passed to deadletters).
   * @param capacity The maximum number of messages in the mailbox before new messages are dropped
   */
  def bounded(capacity: Int): MailboxSelector = BoundedMailboxSelector(capacity)

  /**
   * Select a mailbox from the config file using an absolute config path.
   *
   * This is a power user settings default or bounded should be preferred unless you know what you are doing.
   */
  def fromConfig(path: String): MailboxSelector = MailboxFromConfigSelector(path)
}

/**
 * Actor tags are used to logically group actors. The tags are included in logging as markers
 * Especially useful for logging from functional style actors and since those may not have a clear logger class.
 *
 * Not for user extension.
 */
@DoNotInherit
abstract class ActorTags extends Props {

  /**
   * Scala API: one or more tags defined for the actor
   * @return
   */
  def tags: Set[String]

  /**
   * Java API: one or more tags defined for the actor
   */
  def getTags(): java.util.Set[String] = tags.asJava
}

object ActorTags {

  /**
   * Java API: create a tag props with one or more tags
   */
  @varargs
  def create(tags: String*): ActorTags = apply(tags.toSet)

  /**
   * Java API: create a multi-tag props
   *
   * Set must not be empty.
   */
  def create(tags: java.util.Set[String]): ActorTags = ActorTagsImpl(tags.asScala.toSet)

  /**
   * Scala API: create a tag props with one or more tags
   */
  def apply(tag: String, additionalTags: String*): ActorTags = {
    val tags =
      if (additionalTags.isEmpty) Set(tag)
      else Set(tag) ++ additionalTags
    ActorTagsImpl(tags)
  }

  /**
   * Scala API: create a multi-tag props.
   *
   * Set must not be empty.
   */
  def apply(tags: Set[String]): ActorTags = ActorTagsImpl(tags)
}
