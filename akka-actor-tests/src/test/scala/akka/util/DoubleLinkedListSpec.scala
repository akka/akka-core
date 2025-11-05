/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object DoubleLinkedListSpec {
  private case class Node(value: String) {
    var less, more: OptionVal[Node] = OptionVal.None
  }
}

class DoubleLinkedListSpec extends AnyWordSpec with Matchers {
  import DoubleLinkedListSpec.Node

  private def create() = new DoubleLinkedList[Node](_.less, _.more, _.less = _, _.more = _)

  private def check(list: DoubleLinkedList[Node], expected: List[String]): Unit = {
    list.isEmpty shouldBe expected.isEmpty
    list.getFirst.toOption.map(_.value) shouldBe expected.headOption
    list.getLast.toOption.map(_.value) shouldBe expected.lastOption
    list.forwardIterator.map(_.value).toList shouldBe expected
    list.backwardIterator.map(_.value).toList shouldBe expected.reverse
  }

  "DoubleLinkedList" must {

    "start empty" in {
      val list = create()
      check(list, Nil)
    }

    "prepend elements to front" in {
      val list = create()
      list.prepend(Node("c"))
      list.prepend(Node("b"))
      list.prepend(Node("a"))
      check(list, List("a", "b", "c"))
    }

    "append elements to back" in {
      val list = create()
      list.append(Node("a"))
      list.append(Node("b"))
      list.append(Node("c"))
      check(list, List("a", "b", "c"))
    }

    "remove elements" in {
      val list = create()
      val a = list.prepend(Node("a"))
      val b = list.append(Node("b"))
      val c = list.append(Node("c"))
      check(list, List("a", "b", "c"))
      list.remove(b)
      check(list, List("a", "c"))
      list.remove(c)
      check(list, List("a"))
      list.remove(a)
      check(list, Nil)
    }

    "move elements to front" in {
      val list = create()
      val a = list.prepend(Node("a"))
      val b = list.prepend(Node("b"))
      val c = list.prepend(Node("c"))
      check(list, List("c", "b", "a"))
      list.moveToFront(c)
      check(list, List("c", "b", "a"))
      list.moveToFront(b)
      check(list, List("b", "c", "a"))
      list.moveToFront(a)
      check(list, List("a", "b", "c"))
    }

    "move elements to back" in {
      val list = create()
      val c = list.append(Node("c"))
      val b = list.append(Node("b"))
      val a = list.append(Node("a"))
      check(list, List("c", "b", "a"))
      list.moveToBack(a)
      check(list, List("c", "b", "a"))
      list.moveToBack(b)
      check(list, List("c", "a", "b"))
      list.moveToBack(c)
      check(list, List("a", "b", "c"))
    }

    "get first element or else prepend new element" in {
      val list = create()
      val a = Node("a")
      val b = Node("b")
      val c = Node("c")
      list.prepend(c)
      list.prepend(b)
      check(list, List("b", "c"))
      list.getFirstOrElsePrepend(_.value == "b", a) shouldBe b
      check(list, List("b", "c"))
      list.getFirstOrElsePrepend(_.value == "a", a) shouldBe a
      check(list, List("a", "b", "c"))
    }

    "get last element or else append new element" in {
      val list = create()
      val a = Node("a")
      val b = Node("b")
      val c = Node("c")
      list.append(a)
      list.append(b)
      check(list, List("a", "b"))
      list.getLastOrElseAppend(_.value == "b", c) shouldBe b
      check(list, List("a", "b"))
      list.getLastOrElseAppend(_.value == "c", c) shouldBe c
      check(list, List("a", "b", "c"))
    }

    "get next element or else insert new element" in {
      val list = create()
      val a = Node("a")
      val b = Node("b")
      val c = Node("c")
      list.prepend(c)
      list.prepend(a)
      check(list, List("a", "c"))
      list.getNextOrElseInsert(a, _.value == "c", b) shouldBe c
      check(list, List("a", "c"))
      list.getNextOrElseInsert(a, _.value == "b", b) shouldBe b
      check(list, List("a", "b", "c"))
    }

    "get previous element or else insert new element" in {
      val list = create()
      val a = Node("a")
      val b = Node("b")
      val c = Node("c")
      list.append(a)
      list.append(c)
      check(list, List("a", "c"))
      list.getPreviousOrElseInsert(c, _.value == "a", b) shouldBe a
      check(list, List("a", "c"))
      list.getPreviousOrElseInsert(c, _.value == "b", b) shouldBe b
      check(list, List("a", "b", "c"))
    }

    "find next element or else insert new element" in {
      val list = create()
      val a1 = Node("a1")
      val a2 = Node("a2")
      val b1 = Node("b1")
      val c1 = Node("c1")
      val c2 = Node("c2")
      val d1 = Node("d1")
      list.prepend(c2)
      list.prepend(c1)
      list.prepend(a2)
      list.prepend(a1)
      check(list, List("a1", "a2", "c1", "c2"))
      list.findNextOrElseInsert(a1, _.value.startsWith("a"), _.value.startsWith("c"), b1) shouldBe c1
      check(list, List("a1", "a2", "c1", "c2"))
      list.findNextOrElseInsert(a1, _.value.startsWith("a"), _.value.startsWith("b"), b1) shouldBe b1
      check(list, List("a1", "a2", "b1", "c1", "c2"))
      list.findNextOrElseInsert(c1, _.value.startsWith("c"), _.value.startsWith("d"), d1) shouldBe d1
      check(list, List("a1", "a2", "b1", "c1", "c2", "d1"))
    }

    "find previous element or else insert new element" in {
      val list = create()
      val a1 = Node("a1")
      val b1 = Node("b1")
      val b2 = Node("b2")
      val c1 = Node("c1")
      val d1 = Node("d1")
      val d2 = Node("d2")
      list.append(b1)
      list.append(b2)
      list.append(d1)
      list.append(d2)
      check(list, List("b1", "b2", "d1", "d2"))
      list.findPreviousOrElseInsert(d2, _.value.startsWith("d"), _.value.startsWith("b"), c1) shouldBe b2
      check(list, List("b1", "b2", "d1", "d2"))
      list.findPreviousOrElseInsert(d2, _.value.startsWith("d"), _.value.startsWith("c"), c1) shouldBe c1
      check(list, List("b1", "b2", "c1", "d1", "d2"))
      list.findPreviousOrElseInsert(b2, _.value.startsWith("b"), _.value.startsWith("a"), a1) shouldBe a1
      check(list, List("a1", "b1", "b2", "c1", "d1", "d2"))
    }
  }
}
