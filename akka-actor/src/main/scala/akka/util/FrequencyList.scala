/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.{ immutable, mutable }
import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object FrequencyList {
  def empty[A](dynamicAging: Boolean = false): FrequencyList[A] =
    new FrequencyList[A](dynamicAging, clock = OptionVal.None)

  object withOverallRecency {
    def empty[A](clock: Clock, dynamicAging: Boolean = false): FrequencyList[A] =
      new FrequencyList[A](dynamicAging, OptionVal.Some(clock))
  }

  private final class FrequencyNode[A](val priority: Long) {
    var lessFrequent, moreFrequent: OptionVal[FrequencyNode[A]] = OptionVal.None
    val nodes = new DoubleLinkedList[Node[A]](
      getPrevious = _.lessRecent,
      getNext = _.moreRecent,
      setPrevious = (node, previous) => node.lessRecent = previous,
      setNext = (node, next) => node.moreRecent = next)
  }

  private final class Node[A](val value: A, initialFrequency: FrequencyNode[A]) {
    var accesses: Long = 1L
    var frequency: FrequencyNode[A] = initialFrequency
    var lessRecent, moreRecent: OptionVal[Node[A]] = OptionVal.None
    var overallLessRecent, overallMoreRecent: OptionVal[Node[A]] = OptionVal.None
    var timestamp: Long = 0L
  }
}

/**
 * INTERNAL API
 *
 * Mutable non-thread-safe frequency list.
 * Used for tracking frequency of elements for implementing least/most frequently used eviction policies.
 * Implemented using a doubly-linked list of doubly-linked lists, with lookup, so that all operations are constant time.
 * Elements with the same frequency are stored in order of update (recency within the same frequency count).
 * Overall recency can also be enabled, to support time-based eviction policies, without using a secondary recency list.
 * Dynamic aging can be enabled for least frequently used policies, to automatically 'age' the whole cache on evictions.
 */
@InternalApi
private[akka] final class FrequencyList[A](dynamicAging: Boolean, clock: OptionVal[Clock]) {
  import FrequencyList.{ FrequencyNode, Node }

  private val frequency = new DoubleLinkedList[FrequencyNode[A]](
    getPrevious = _.lessFrequent,
    getNext = _.moreFrequent,
    setPrevious = (node, previous) => node.lessFrequent = previous,
    setNext = (node, next) => node.moreFrequent = next)

  private val overallRecency = new DoubleLinkedList[Node[A]](
    getPrevious = _.overallLessRecent,
    getNext = _.overallMoreRecent,
    setPrevious = (node, previous) => node.overallLessRecent = previous,
    setNext = (node, next) => node.overallMoreRecent = next)

  private val lookupNode = mutable.Map.empty[A, Node[A]]

  private var age = 0L

  def size: Int = lookupNode.size

  def update(value: A): FrequencyList[A] = {
    if (lookupNode.contains(value)) {
      val node = lookupNode(value)
      increaseFrequency(node)
      if (clock.isDefined) {
        node.timestamp = clock.get.currentTime()
        overallRecency.moveToBack(node)
      }
    } else {
      val node = addInitialFrequency(value)
      lookupNode += value -> node
      if (clock.isDefined) {
        node.timestamp = clock.get.currentTime()
        overallRecency.append(node)
      }
    }
    this
  }

  def remove(value: A): FrequencyList[A] = {
    if (lookupNode.contains(value)) {
      removeNode(lookupNode(value))
    }
    this
  }

  def contains(value: A): Boolean = lookupNode.contains(value)

  def leastFrequent: OptionVal[A] = frequency.getFirst match {
    case OptionVal.Some(least) =>
      least.nodes.getFirst match {
        case OptionVal.Some(first) => OptionVal.Some(first.value)
        case _                     => OptionVal.none
      }
    case _ => OptionVal.none
  }

  def mostFrequent: OptionVal[A] = frequency.getLast match {
    case OptionVal.Some(most) =>
      most.nodes.getLast match {
        case OptionVal.Some(last) => OptionVal.Some(last.value)
        case _                    => OptionVal.none
      }
    case _ => OptionVal.none
  }

  def leastToMostFrequent: Iterator[A] = forwardIterator.map(_.value)

  def mostToLeastFrequent: Iterator[A] = backwardIterator.map(_.value)

  def removeLeastFrequent(n: Int): immutable.Seq[A] =
    if (n == 1) removeLeastFrequent() // optimised removal of just 1 node
    else forwardIterator.take(n).map(removeLeastFrequentNode).toList

  def removeLeastFrequent(n: Int = 1, skip: OptionVal[A]): immutable.Seq[A] =
    forwardIterator.filterNot(node => skip.contains(node.value)).take(n).map(removeLeastFrequentNode).toList

  def removeLeastFrequent(): immutable.Seq[A] = frequency.getFirst match {
    case OptionVal.Some(least) =>
      least.nodes.getFirst match {
        case OptionVal.Some(first) => List(removeLeastFrequentNode(first))
        case _                     => Nil
      }
    case _ => Nil
  }

  def removeMostFrequent(n: Int): immutable.Seq[A] =
    if (n == 1) removeMostFrequent() // optimised removal of just 1 node
    else backwardIterator.take(n).map(removeNode).toList

  def removeMostFrequent(n: Int = 1, skip: OptionVal[A]): immutable.Seq[A] =
    backwardIterator.filterNot(node => skip.contains(node.value)).take(n).map(removeNode).toList

  def removeMostFrequent(): immutable.Seq[A] = frequency.getLast match {
    case OptionVal.Some(most) =>
      most.nodes.getLast match {
        case OptionVal.Some(last) => List(removeNode(last))
        case _                    => Nil
      }
    case _ => Nil
  }

  def overallLeastToMostRecent: Iterator[A] = overallRecency.forwardIterator.map(_.value)

  def overallMostToLeastRecent: Iterator[A] = overallRecency.backwardIterator.map(_.value)

  def removeOverallLeastRecent(n: Int = 1): immutable.Seq[A] = {
    if (clock.isEmpty) throw new UnsupportedOperationException("Overall recency is not enabled for this FrequencyList")
    overallRecency.forwardIterator.take(n).map(removeNode).toList
  }

  def removeOverallMostRecent(n: Int = 1): immutable.Seq[A] = {
    if (clock.isEmpty) throw new UnsupportedOperationException("Overall recency is not enabled for this FrequencyList")
    overallRecency.backwardIterator.take(n).map(removeNode).toList
  }

  def removeOverallLeastRecentOutside(duration: FiniteDuration): immutable.Seq[A] = {
    if (clock.isEmpty) throw new UnsupportedOperationException("Overall recency is not enabled for this FrequencyList")
    val min = clock.get.earlierTime(duration)
    overallRecency.forwardIterator.takeWhile(_.timestamp < min).map(removeNode).toList
  }

  def removeOverallMostRecentWithin(duration: FiniteDuration): immutable.Seq[A] = {
    if (clock.isEmpty) throw new UnsupportedOperationException("Overall recency is not enabled for this FrequencyList")
    val max = clock.get.earlierTime(duration)
    overallRecency.backwardIterator.takeWhile(_.timestamp > max).map(removeNode).toList
  }

  private def addInitialFrequency(value: A): Node[A] =
    if (dynamicAging) addInitialAdjustedFrequency(value) else addAsLeastFrequent(value)

  private def addAsLeastFrequent(value: A): Node[A] = {
    val one = frequency.getFirstOrElsePrepend(_.priority == 1, new FrequencyNode[A](priority = 1))
    val node = new Node(value, one)
    addToFrequency(node, one)
    node
  }

  private def addInitialAdjustedFrequency(value: A): Node[A] = {
    val priority = 1 + age
    val frequencyNode = frequency.getFirst match {
      case OptionVal.Some(first) if first.priority < priority =>
        frequency.findNextOrElseInsert(
          first,
          _.priority < priority,
          _.priority == priority,
          new FrequencyNode[A](priority))
      case _ =>
        frequency.getFirstOrElsePrepend(_.priority == priority, new FrequencyNode[A](priority))
    }
    val node = new Node(value, frequencyNode)
    addToFrequency(node, frequencyNode)
    node
  }

  private def increaseFrequency(node: Node[A]): Unit =
    if (dynamicAging) increaseAdjustedFrequency(node) else increaseToNextFrequency(node)

  private def increaseAdjustedFrequency(node: Node[A]): Unit = {
    node.accesses += 1
    val priority = node.accesses + age
    val frequencyNode = frequency.findNextOrElseInsert(
      node.frequency,
      _.priority < priority,
      _.priority == priority,
      new FrequencyNode[A](priority))
    removeFromFrequency(node)
    addToFrequency(node, frequencyNode)
  }

  private def increaseToNextFrequency(node: Node[A]): Unit = {
    node.accesses += 1
    val priority = node.accesses
    val frequencyNode =
      frequency.getNextOrElseInsert(node.frequency, _.priority == priority, new FrequencyNode[A](priority))
    removeFromFrequency(node)
    addToFrequency(node, frequencyNode)
  }

  private def addToFrequency(node: Node[A], frequencyNode: FrequencyNode[A]): Unit = {
    node.frequency = frequencyNode
    frequencyNode.nodes.append(node)
  }

  private def removeFromFrequency(node: Node[A]): Unit = {
    val frequencyNode = node.frequency
    frequencyNode.nodes.remove(node)
    if (frequencyNode.nodes.isEmpty) frequency.remove(frequencyNode)
  }

  private def removeNode(node: Node[A]): A = {
    val value = node.value
    removeFromFrequency(node)
    if (clock.isDefined) overallRecency.remove(node)
    lookupNode -= value
    value
  }

  private def removeLeastFrequentNode(node: Node[A]): A = {
    if (dynamicAging) age = node.frequency.priority
    removeNode(node)
  }

  private def forwardIterator: Iterator[Node[A]] = frequency.forwardIterator.flatMap(_.nodes.forwardIterator)

  private def backwardIterator: Iterator[Node[A]] = frequency.backwardIterator.flatMap(_.nodes.backwardIterator)
}
