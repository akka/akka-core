/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dungeon

import scala.collection.immutable
import akka.actor.{ ActorRef, ChildNameReserved, ChildRestartStats, ChildStats, InvalidActorNameException }
import akka.annotation.InternalApi
import akka.util.Collections.{ EmptyImmutableSeq, PartialImmutableValuesIterable }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait ChildrenContainer {

  def add(name: String, stats: ChildRestartStats): ChildrenContainer
  def remove(child: ActorRef): ChildrenContainer

  def getByName(name: String): Option[ChildStats]
  def getByRef(actor: ActorRef): Option[ChildRestartStats]

  def children: immutable.Iterable[ActorRef]
  def stats: immutable.Iterable[ChildRestartStats]

  def shallDie(actor: ActorRef): ChildrenContainer

  // reserve that name or throw an exception
  def reserve(name: String): ChildrenContainer
  // cancel a reservation
  def unreserve(name: String): ChildrenContainer

  def isTerminating: Boolean = false
  def isNormal: Boolean = true
}

/**
 * INTERNAL API
 *
 * This object holds the classes performing the logic of managing the children
 * of an actor, hence they are intimately tied to ActorCell.
 */
@InternalApi
private[akka] object ChildrenContainer {

  sealed trait SuspendReason
  case object UserRequest extends SuspendReason
  // careful with those system messages, all handling to be taking place in ActorCell.scala!
  final case class Recreation(cause: Throwable) extends SuspendReason with WaitingForChildren
  final case class Creation() extends SuspendReason with WaitingForChildren
  case object Termination extends SuspendReason

  class ChildRestartsIterable(stats: immutable.Map[_, ChildStats])
      extends PartialImmutableValuesIterable[ChildStats, ChildRestartStats] {
    override final def apply(c: ChildStats) = c.asInstanceOf[ChildRestartStats]
    override final def isDefinedAt(c: ChildStats) = c.isInstanceOf[ChildRestartStats]
    override final def valuesIterator = stats.valuesIterator
  }

  class ChildrenIterable(stats: immutable.Map[_, ChildStats])
      extends PartialImmutableValuesIterable[ChildStats, ActorRef] {
    override final def apply(c: ChildStats) = c.asInstanceOf[ChildRestartStats].child
    override final def isDefinedAt(c: ChildStats) = c.isInstanceOf[ChildRestartStats]
    override final def valuesIterator = stats.valuesIterator
  }

  trait WaitingForChildren

  trait EmptyChildrenContainer extends ChildrenContainer {
    val emptyStats = immutable.TreeMap.empty[String, ChildStats]
    override def add(name: String, stats: ChildRestartStats): ChildrenContainer =
      new NormalChildrenContainer(emptyStats.updated(name, stats))
    override def remove(child: ActorRef): ChildrenContainer = this
    override def getByName(name: String): Option[ChildRestartStats] = None
    override def getByRef(actor: ActorRef): Option[ChildRestartStats] = None
    override def children: immutable.Iterable[ActorRef] = EmptyImmutableSeq
    override def stats: immutable.Iterable[ChildRestartStats] = EmptyImmutableSeq
    override def shallDie(actor: ActorRef): ChildrenContainer = this
    override def reserve(name: String): ChildrenContainer =
      new NormalChildrenContainer(emptyStats.updated(name, ChildNameReserved))
    override def unreserve(name: String): ChildrenContainer = this
  }

  /**
   * This is the empty container, shared among all leaf actors.
   */
  object EmptyChildrenContainer extends EmptyChildrenContainer {
    override def toString = "no children"
  }

  /**
   * This is the empty container which is installed after the last child has
   * terminated while stopping; it is necessary to distinguish from the normal
   * empty state while calling handleChildTerminated() for the last time.
   */
  object TerminatedChildrenContainer extends EmptyChildrenContainer {
    override def add(name: String, stats: ChildRestartStats): ChildrenContainer = this
    override def reserve(name: String): ChildrenContainer =
      throw new IllegalStateException("cannot reserve actor name '" + name + "': already terminated")
    override def isTerminating: Boolean = true
    override def isNormal: Boolean = false
    override def toString = "terminated"
  }

  /**
   * Normal children container: we do have at least one child, but none of our
   * children are currently terminating (which is the time period between
   * calling context.stop(child) and processing the ChildTerminated() system
   * message).
   */
  class NormalChildrenContainer(val c: immutable.TreeMap[String, ChildStats]) extends ChildrenContainer {

    override def add(name: String, stats: ChildRestartStats): ChildrenContainer =
      new NormalChildrenContainer(c.updated(name, stats))

    override def remove(child: ActorRef): ChildrenContainer = NormalChildrenContainer(c - child.path.name)

    override def getByName(name: String): Option[ChildStats] = c.get(name)

    override def getByRef(actor: ActorRef): Option[ChildRestartStats] = c.get(actor.path.name) match {
      case c @ Some(crs: ChildRestartStats) if crs.child == actor => c.asInstanceOf[Option[ChildRestartStats]]
      case _                                                      => None
    }

    override def children: immutable.Iterable[ActorRef] =
      if (c.isEmpty) EmptyImmutableSeq else new ChildrenIterable(c)

    override def stats: immutable.Iterable[ChildRestartStats] =
      if (c.isEmpty) EmptyImmutableSeq else new ChildRestartsIterable(c)

    override def shallDie(actor: ActorRef): ChildrenContainer = TerminatingChildrenContainer(c, Set(actor), UserRequest)

    override def reserve(name: String): ChildrenContainer =
      if (c contains name)
        throw InvalidActorNameException(s"actor name [$name] is not unique!")
      else new NormalChildrenContainer(c.updated(name, ChildNameReserved))

    override def unreserve(name: String): ChildrenContainer = c.get(name) match {
      case Some(ChildNameReserved) => NormalChildrenContainer(c - name)
      case _                       => this
    }

    override def toString =
      if (c.size > 20) c.size.toString + " children"
      else c.mkString("children:\n    ", "\n    ", "")
  }

  object NormalChildrenContainer {
    def apply(c: immutable.TreeMap[String, ChildStats]): ChildrenContainer =
      if (c.isEmpty) EmptyChildrenContainer
      else new NormalChildrenContainer(c)
  }

  /**
   * Waiting state: there are outstanding termination requests (i.e. context.stop(child)
   * was called but the corresponding ChildTerminated() system message has not yet been
   * processed). There could be no specific reason (UserRequested), we could be Restarting
   * or Terminating.
   *
   * Removing the last child which was supposed to be terminating will return a different
   * type of container, depending on whether or not children are left and whether or not
   * the reason was “Terminating”.
   */
  final case class TerminatingChildrenContainer(
      c: immutable.TreeMap[String, ChildStats],
      toDie: Set[ActorRef],
      reason: SuspendReason)
      extends ChildrenContainer {

    override def add(name: String, stats: ChildRestartStats): ChildrenContainer = copy(c.updated(name, stats))

    override def remove(child: ActorRef): ChildrenContainer = {
      val t = toDie - child
      if (t.isEmpty) reason match {
        case Termination => TerminatedChildrenContainer
        case _           => NormalChildrenContainer(c - child.path.name)
      } else copy(c - child.path.name, t)
    }

    override def getByName(name: String): Option[ChildStats] = c.get(name)

    override def getByRef(actor: ActorRef): Option[ChildRestartStats] = c.get(actor.path.name) match {
      case c @ Some(crs: ChildRestartStats) if crs.child == actor => c.asInstanceOf[Option[ChildRestartStats]]
      case _                                                      => None
    }

    override def children: immutable.Iterable[ActorRef] =
      if (c.isEmpty) EmptyImmutableSeq else new ChildrenIterable(c)

    override def stats: immutable.Iterable[ChildRestartStats] =
      if (c.isEmpty) EmptyImmutableSeq else new ChildRestartsIterable(c)

    override def shallDie(actor: ActorRef): ChildrenContainer = copy(toDie = toDie + actor)

    override def reserve(name: String): ChildrenContainer = reason match {
      case Termination => throw new IllegalStateException("cannot reserve actor name '" + name + "': terminating")
      case _ =>
        if (c contains name)
          throw InvalidActorNameException(s"actor name [$name] is not unique!")
        else copy(c = c.updated(name, ChildNameReserved))
    }

    override def unreserve(name: String): ChildrenContainer = c.get(name) match {
      case Some(ChildNameReserved) => copy(c = c - name)
      case _                       => this
    }

    override def isTerminating: Boolean = reason == Termination
    override def isNormal: Boolean = reason == UserRequest

    override def toString =
      if (c.size > 20) c.size.toString + " children"
      else c.mkString("children (" + toDie.size + " terminating):\n    ", "\n    ", "\n") + toDie
  }

}
