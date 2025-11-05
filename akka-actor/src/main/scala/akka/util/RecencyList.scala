/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object RecencyList {
  def emptyWithNanoClock[A]: RecencyList[A] =
    RecencyList[A](new NanoClock)

  def apply[A](clock: Clock): RecencyList[A] =
    new RecencyList[A](clock)

  private final class Node[A](val value: A) {
    var lessRecent, moreRecent: OptionVal[Node[A]] = OptionVal.None
    var timestamp: Long = 0L
  }

}

/**
 * INTERNAL API
 *
 * Mutable non-thread-safe recency list.
 * Used for tracking recency of elements for implementing least/most recently used eviction policies.
 * Implemented using a doubly-linked list plus hash map for lookup, so that all operations are constant time.
 */
@InternalApi
private[akka] final class RecencyList[A] private (clock: Clock) {
  import RecencyList.Node

  private val recency = new DoubleLinkedList[Node[A]](
    getPrevious = _.lessRecent,
    getNext = _.moreRecent,
    setPrevious = (node, previous) => node.lessRecent = previous,
    setNext = (node, next) => node.moreRecent = next)

  private val lookupNode = mutable.Map.empty[A, Node[A]]

  def size: Int = lookupNode.size

  def update(value: A): RecencyList[A] = {
    if (lookupNode.contains(value)) {
      val node = lookupNode(value)
      node.timestamp = clock.currentTime()
      recency.moveToBack(node)
    } else {
      val node = new Node(value)
      node.timestamp = clock.currentTime()
      recency.append(node)
      lookupNode += value -> node
    }
    this
  }

  def remove(value: A): RecencyList[A] = {
    if (lookupNode.contains(value)) {
      removeNode(lookupNode(value))
    }
    this
  }

  def contains(value: A): Boolean = lookupNode.contains(value)

  def leastRecent: OptionVal[A] = recency.getFirst match {
    case OptionVal.Some(first) => OptionVal.Some(first.value)
    case _                     => OptionVal.none
  }

  def mostRecent: OptionVal[A] = recency.getLast match {
    case OptionVal.Some(last) => OptionVal.Some(last.value)
    case _                    => OptionVal.none
  }

  def leastToMostRecent: Iterator[A] = recency.forwardIterator.map(_.value)

  def mostToLeastRecent: Iterator[A] = recency.backwardIterator.map(_.value)

  def removeLeastRecent(n: Int): immutable.Seq[A] =
    if (n == 1) removeLeastRecent() // optimised removal of just 1 node
    else recency.forwardIterator.take(n).map(removeNode).toList

  def removeLeastRecent(n: Int, skip: Int): immutable.Seq[A] =
    recency.forwardIterator.slice(skip, skip + n).map(removeNode).toList

  def removeLeastRecent(): immutable.Seq[A] = recency.getFirst match {
    case OptionVal.Some(first) => List(removeNode(first))
    case _                     => Nil
  }

  def removeMostRecent(n: Int): immutable.Seq[A] =
    if (n == 1) removeMostRecent() // optimised removal of just 1 node
    else recency.backwardIterator.take(n).map(removeNode).toList

  def removeMostRecent(n: Int, skip: Int): immutable.Seq[A] =
    recency.backwardIterator.slice(skip, skip + n).map(removeNode).toList

  def removeMostRecent(): immutable.Seq[A] = recency.getLast match {
    case OptionVal.Some(last) => List(removeNode(last))
    case _                    => Nil
  }

  def removeLeastRecentOutside(duration: FiniteDuration): immutable.Seq[A] = {
    val min = clock.earlierTime(duration)
    recency.forwardIterator.takeWhile(_.timestamp < min).map(removeNode).toList
  }

  def removeMostRecentWithin(duration: FiniteDuration): immutable.Seq[A] = {
    val max = clock.earlierTime(duration)
    recency.backwardIterator.takeWhile(_.timestamp > max).map(removeNode).toList
  }

  private def removeNode(node: Node[A]): A = {
    val value = node.value
    recency.remove(node)
    lookupNode -= value
    value
  }
}
