/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class WildcardIndexSpec extends AnyWordSpec with Matchers {

  "wildcard index" must {
    "allow to insert elements using Arrays of strings" in {
      emptyIndex.insert(Array("a", "b"), 1) shouldBe a[WildcardIndex[_]]
      emptyIndex.insert(Array("a"), 1) shouldBe a[WildcardIndex[_]]
      emptyIndex.insert(Array.empty[String], 1) shouldBe a[WildcardIndex[_]]
    }

    "allow to find inserted elements" in {
      val tree = emptyIndex.insert(Array("a"), 1).insert(Array("a", "b"), 2).insert(Array("a", "c"), 3)
      tree.find(Array("a", "b")).get shouldBe 2
      tree.find(Array("a")).get shouldBe 1
      tree.find(Array("x")) shouldBe None
      tree.find(Array.empty[String]) shouldBe None
    }

    "match all elements in the subArray when it contains a wildcard" in {
      val tree1 = emptyIndex.insert(Array("a"), 1).insert(Array("a", "*"), 1)
      tree1.find(Array("z")) shouldBe None
      tree1.find(Array("a")).get shouldBe 1
      tree1.find(Array("a", "b")).get shouldBe 1
      tree1.find(Array("a", "x")).get shouldBe 1

      val tree2 = emptyIndex.insert(Array("a", "*"), 1).insert(Array("a", "*", "c"), 2)
      tree2.find(Array("z")) shouldBe None
      tree2.find(Array("a", "b")).get shouldBe 1
      tree2.find(Array("a", "x")).get shouldBe 1
      tree2.find(Array("a", "x", "c")).get shouldBe 2
      tree2.find(Array("a", "x", "y")) shouldBe None
    }

    "match all elements in the subArray when it contains a wildcard suffix" in {
      val tree1 = emptyIndex.insert(Array("a*"), 1).insert(Array("b", "c*"), 2)
      tree1.find(Array("z")) shouldBe None
      tree1.find(Array("a")).get shouldBe 1
      tree1.find(Array("aa")).get shouldBe 1
      tree1.find(Array("aa", "b")) shouldBe None
      tree1.find(Array("b", "c")).get shouldBe 2
      tree1.find(Array("b", "cc")).get shouldBe 2
      tree1.find(Array("b", "x")) shouldBe None

      val tree2 = emptyIndex.insert(Array("a", "b*"), 1).insert(Array("b", "c*", "d"), 2)
      tree2.find(Array("z")) shouldBe None
      tree2.find(Array("a", "b")).get shouldBe 1
      tree2.find(Array("a", "bb")).get shouldBe 1
      tree2.find(Array("a", "c")) shouldBe None
      tree2.find(Array("b", "c", "d")).get shouldBe 2
      tree2.find(Array("b", "cc", "d")).get shouldBe 2
      tree2.find(Array("b", "c", "x")) shouldBe None
      tree2.find(Array("b", "cc", "x")) shouldBe None
    }

    "fail when a double wildcard is used as a suffix" in {
      an[IllegalArgumentException] should be thrownBy emptyIndex.insert(Array("a**"), 1)
    }

    "never find anything when emptyIndex" in {
      emptyIndex.find(Array("a")) shouldBe None
      emptyIndex.find(Array("a", "b")) shouldBe None
      emptyIndex.find(Array.empty[String]) shouldBe None
    }

    "match all remaining elements when it contains a terminal double wildcard" in {
      val tree1 = emptyIndex.insert(Array("a", "**"), 1)
      tree1.find(Array("z")) shouldBe None
      tree1.find(Array("a", "b")).get shouldBe 1
      tree1.find(Array("a", "x")).get shouldBe 1
      tree1.find(Array("a", "x", "y")).get shouldBe 1

      val tree2 = emptyIndex.insert(Array("**"), 1)
      tree2.find(Array("anything", "I", "want")).get shouldBe 1
      tree2.find(Array("anything")).get shouldBe 1
    }

    "ignore non-terminal double wildcards" in {
      val tree = emptyIndex.insert(Array("a", "**", "c"), 1)
      tree.find(Array("a", "x", "y", "c")) shouldBe None
      tree.find(Array("a", "x", "y")) shouldBe None
    }
  }

  private val emptyIndex = WildcardIndex[Int]()
}
