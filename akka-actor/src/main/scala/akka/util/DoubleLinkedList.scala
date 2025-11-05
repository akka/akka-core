/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.AbstractIterator

import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * Mutable non-thread-safe double-linked list abstraction, with a flexible node type.
 */
@InternalApi
private[akka] final class DoubleLinkedList[Node](
    getPrevious: Node => OptionVal[Node],
    getNext: Node => OptionVal[Node],
    setPrevious: (Node, OptionVal[Node]) => Unit,
    setNext: (Node, OptionVal[Node]) => Unit) {

  private[this] var first: OptionVal[Node] = OptionVal.none
  private[this] var last: OptionVal[Node] = OptionVal.none

  def isEmpty: Boolean = first.isEmpty

  def getFirst: OptionVal[Node] = first

  def getLast: OptionVal[Node] = last

  def prepend(node: Node): Node = insertBefore(node, first)

  def append(node: Node): Node = insertAfter(last, node)

  def remove(node: Node): Unit = {
    unlink(node)
    setNext(node, OptionVal.none)
    setPrevious(node, OptionVal.none)
  }

  def moveToFront(node: Node): Node = {
    if (first.contains(node)) {
      node
    } else {
      unlink(node)
      prepend(node)
    }
  }

  def moveToBack(node: Node): Node = {
    if (last.contains(node)) {
      node
    } else {
      unlink(node)
      append(node)
    }
  }

  def getFirstOrElsePrepend(check: Node => Boolean, newNode: => Node): Node = first match {
    case OptionVal.Some(first) if check(first) => first
    case _                                     => prepend(newNode)
  }

  def getLastOrElseAppend(check: Node => Boolean, newNode: => Node): Node = last match {
    case OptionVal.Some(last) if check(last) => last
    case _                                   => append(newNode)
  }

  def getNextOrElseInsert(node: Node, check: Node => Boolean, newNode: => Node): Node = getNext(node) match {
    case OptionVal.Some(next) if check(next) => next
    case _                                   => insertAfter(OptionVal.Some(node), newNode)
  }

  def getPreviousOrElseInsert(node: Node, check: Node => Boolean, newNode: => Node): Node = getPrevious(node) match {
    case OptionVal.Some(previous) if check(previous) => previous
    case _                                           => insertBefore(newNode, OptionVal.Some(node))
  }

  def findNextOrElseInsert(node: Node, isBefore: Node => Boolean, check: Node => Boolean, newNode: => Node): Node =
    getNextOrElseInsert(shiftWhile(node, getNext, isBefore), check, newNode)

  def findPreviousOrElseInsert(node: Node, isAfter: Node => Boolean, check: Node => Boolean, newNode: => Node): Node =
    getPreviousOrElseInsert(shiftWhile(node, getPrevious, isAfter), check, newNode)

  def forwardIterator: Iterator[Node] = iteratorFrom(first, getNext)

  def backwardIterator: Iterator[Node] = iteratorFrom(last, getPrevious)

  private def insertBefore(node: Node, next: OptionVal[Node]): Node = {
    val previous = if (next.isDefined) getPrevious(next.get) else OptionVal.none
    link(previous, node, next)
    node
  }

  private def insertAfter(previous: OptionVal[Node], node: Node): Node = {
    val next = if (previous.isDefined) getNext(previous.get) else OptionVal.none
    link(previous, node, next)
    node
  }

  private def link(previous: OptionVal[Node], node: Node, next: OptionVal[Node]): Unit = {
    setPrevious(node, previous)
    setNext(node, next)
    if (previous.isEmpty) first = OptionVal.Some(node)
    else setNext(previous.get, OptionVal.Some(node))
    if (next.isEmpty) last = OptionVal.Some(node)
    else setPrevious(next.get, OptionVal.Some(node))
  }

  private def unlink(node: Node): Unit = {
    val previous = getPrevious(node)
    val next = getNext(node)
    if (previous.isEmpty) first = next
    else setNext(previous.get, next)
    if (next.isEmpty) last = previous
    else setPrevious(next.get, previous)
  }

  private def shiftWhile(start: Node, shift: Node => OptionVal[Node], check: Node => Boolean): Node = {
    var current = start
    var peek = shift(current)
    while (peek.isDefined && check(peek.get)) {
      current = peek.get
      peek = shift(current)
    }
    current
  }

  private def iteratorFrom(start: OptionVal[Node], shift: Node => OptionVal[Node]): Iterator[Node] =
    new AbstractIterator[Node] {
      private[this] var cursor: OptionVal[Node] = start
      override def hasNext: Boolean = cursor.isDefined
      override def next(): Node = {
        val node = cursor
        cursor = shift(cursor.get)
        node.get
      }
    }
}
