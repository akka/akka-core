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
private[akka] object SegmentedRecencyList {
  def empty[A](limits: immutable.Seq[Int]): SegmentedRecencyList[A] =
    new SegmentedRecencyList[A](limits, OptionVal.None)

  object withOverallRecency {
    def empty[A](clock: Clock, limits: immutable.Seq[Int]): SegmentedRecencyList[A] =
      new SegmentedRecencyList[A](limits, OptionVal.Some(clock))
  }

  private final class Node[A](val value: A) {
    var level: Int = 0
    var timestamp: Long = 0L
    var lessRecent, moreRecent: OptionVal[Node[A]] = OptionVal.None
    var overallLessRecent, overallMoreRecent: OptionVal[Node[A]] = OptionVal.None
  }
}

/**
 * INTERNAL API
 *
 * Mutable non-thread-safe segmented recency list.
 * Variation of RecencyList for implementing segmented least recently used eviction policies.
 * Implemented using doubly-linked lists plus hash map for lookup, so that all operations are constant time.
 */
@InternalApi
private[akka] final class SegmentedRecencyList[A](initialLimits: immutable.Seq[Int], clock: OptionVal[Clock]) {
  import SegmentedRecencyList.Node

  private var limits: immutable.IndexedSeq[Int] = initialLimits.toIndexedSeq
  private var totalLimit: Int = limits.sum

  private val levels = limits.size
  private val lowest = 0
  private val highest = levels - 1

  private val segments = IndexedSeq.fill(levels)(
    new DoubleLinkedList[Node[A]](
      getPrevious = _.lessRecent,
      getNext = _.moreRecent,
      setPrevious = (node, previous) => node.lessRecent = previous,
      setNext = (node, next) => node.moreRecent = next))

  private val sizes = mutable.IndexedSeq.fill(levels)(0)

  private val overallRecency = new DoubleLinkedList[Node[A]](
    getPrevious = _.overallLessRecent,
    getNext = _.overallMoreRecent,
    setPrevious = (node, previous) => node.overallLessRecent = previous,
    setNext = (node, next) => node.overallMoreRecent = next)

  private val lookupNode = mutable.Map.empty[A, Node[A]]

  def size: Int = lookupNode.size

  def sizeOf(level: Int): Int = sizes(level)

  def update(value: A): SegmentedRecencyList[A] = {
    if (lookupNode.contains(value)) {
      promote(lookupNode(value))
    } else {
      insert(new Node(value))
    }
    this
  }

  def remove(value: A): SegmentedRecencyList[A] = {
    if (lookupNode.contains(value)) {
      removeNode(lookupNode(value))
    }
    this
  }

  def contains(value: A): Boolean = lookupNode.contains(value)

  def leastRecent: OptionVal[A] = segments(lowest).getFirst match {
    case OptionVal.Some(first) => OptionVal.Some(first.value)
    case _                     => OptionVal.none
  }

  def leastToMostRecentOf(level: Int): Iterator[A] = segments(level).forwardIterator.map(_.value)

  def removeLeastRecentOverLimit(): immutable.Seq[A] = {
    if (size > totalLimit) {
      adjustProtectedLevels()
      val excess = size - totalLimit
      if (excess == 1) removeLeastRecent() // optimised removal of just 1 node
      else segments(lowest).forwardIterator.take(excess).map(removeNode).toList
    } else Nil
  }

  def removeLeastRecent(): immutable.Seq[A] = segments(lowest).getFirst match {
    case OptionVal.Some(first) => List(removeNode(first))
    case _                     => Nil
  }

  def removeOverallLeastRecentOutside(duration: FiniteDuration): immutable.Seq[A] = {
    if (clock.isEmpty) throw new UnsupportedOperationException("Overall recency is not enabled")
    val min = clock.get.earlierTime(duration)
    overallRecency.forwardIterator.takeWhile(_.timestamp < min).map(removeNode).toList
  }

  def updateLimits(newLimits: immutable.Seq[Int]): Unit = {
    limits = newLimits.toIndexedSeq
    totalLimit = limits.sum
  }

  private def adjustProtectedLevels(): Unit =
    for (level <- highest until lowest by -1) adjust(level)

  private def adjust(level: Int): Unit = {
    val excess = sizes(level) - limits(level)
    if (excess > 0) segments(level).forwardIterator.take(excess).foreach(demote)
  }

  private def insert(node: Node[A]): Unit = {
    appendTo(lowest, node)
    lookupNode += node.value -> node
    if (clock.isDefined) {
      node.timestamp = clock.get.currentTime()
      overallRecency.append(node)
    }
  }

  private def promote(node: Node[A]): Unit = {
    if (node.level == highest) {
      segments(node.level).moveToBack(node)
    } else {
      val newLevel = node.level + 1
      removeFromCurrentLevel(node)
      appendTo(newLevel, node)
      adjust(newLevel)
    }
    if (clock.isDefined) {
      node.timestamp = clock.get.currentTime()
      overallRecency.moveToBack(node)
    }
  }

  private def demote(node: Node[A]): Unit = {
    // assume we only demote from the higher protected levels
    removeFromCurrentLevel(node)
    appendTo(node.level - 1, node)
  }

  private def appendTo(level: Int, node: Node[A]): Unit = {
    node.level = level
    segments(level).append(node)
    sizes(level) += 1
  }

  private def removeFromCurrentLevel(node: Node[A]): Unit = {
    segments(node.level).remove(node)
    sizes(node.level) -= 1
  }

  private def removeNode(node: Node[A]): A = {
    val value = node.value
    removeFromCurrentLevel(node)
    if (clock.isDefined) overallRecency.remove(node)
    lookupNode -= value
    value
  }
}
