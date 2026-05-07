/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.immutable

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object TopologicalSortSpec {
  final case class Node(name: String, dependsOn: Set[String])
}

class TopologicalSortSpec extends AnyWordSpec with Matchers {
  import TopologicalSortSpec._

  // some convenience to make the test readable
  def node(name: String, dependsOn: String*): Node = Node(name, dependsOn.toSet)

  private def checkTopologicalSort(nodes: Node*): immutable.Seq[String] = {
    val lookup = nodes.map(n => n.name -> n).toMap
    val result = TopologicalSort.topologicalSort[String](nodes.map(_.name), lookup(_).dependsOn)
    result.zipWithIndex.foreach {
      case (name, i) =>
        lookup.get(name) match {
          case Some(Node(name, dependsOn)) =>
            dependsOn.foreach { deps =>
              withClue(s"node [$name] depends on [$deps] but was ordered before it in topological sort result $result") {
                i should be > result.indexOf(deps)
              }
            }
          case None => // ok
        }
    }
    result
  }

  "TopologicalSort" must {

    "sort nodes in topological order" in {
      checkTopologicalSort() should ===(Nil)

      // empty node
      checkTopologicalSort(node("a")) should ===(List("a"))

      checkTopologicalSort(node("a"), node("b", "a")) should ===(List("a", "b"))

      val result1 = checkTopologicalSort(node("a"), node("c", "a"), node("b", "a"))
      result1.head should ===("a")
      // b, c can be in any order
      result1.toSet should ===(Set("a", "b", "c"))

      checkTopologicalSort(node("a"), node("b", "a"), node("c", "b")) should ===(List("a", "b", "c"))

      checkTopologicalSort(node("a"), node("b", "a"), node("c", "a", "b")) should ===(List("a", "b", "c"))

      val result2 = checkTopologicalSort(node("a"), node("b"), node("c", "a", "b"))
      result2.last should ===("c")
      // a, b can be in any order
      result2.toSet should ===(Set("a", "b", "c"))

      checkTopologicalSort(node("a"), node("b", "a"), node("c", "b"), node("d", "b", "c"), node("e", "d")) should ===(
        List("a", "b", "c", "d", "e"))

      val result3 =
        checkTopologicalSort(
          node("a1"),
          node("a2", "a1"),
          node("a3", "a2"),
          node("b1"),
          node("b2", "b1"),
          node("b3", "b2"))
      val (a, b) = result3.partition(_.charAt(0) == 'a')
      a should ===(List("a1", "a2", "a3"))
      b should ===(List("b1", "b2", "b3"))
    }

    "retain current order when there are no edges" in {
      checkTopologicalSort(node("c"), node("d"), node("e"), node("b"), node("a")) should ===(
        List("c", "d", "e", "b", "a"))
    }

    "remove duplicates automatically" in {
      checkTopologicalSort(node("c"), node("d"), node("b", "a"), node("c", "b"), node("a"), node("b", "a")) should ===(
        List("a", "b", "c", "d"))
    }

    "detect cycles in phases (non-DAG)" in {
      intercept[IllegalArgumentException] {
        TopologicalSort.topologicalSort[String](List("a"), _ => Set("a"))
      }

      intercept[IllegalArgumentException] {
        TopologicalSort.topologicalSort[String](List("a", "b"), x => if (x == "a") Set("b") else Set("a"))
      }

      intercept[IllegalArgumentException] {
        TopologicalSort.topologicalSort[String](List("a", "b", "c"), {
          case "a" => Set("b")
          case "b" => Set("c")
          case "c" => Set("a")
        })
      }

      intercept[IllegalArgumentException] {
        TopologicalSort.topologicalSort[String](List("a", "b", "c", "d"), {
          case "a" => Set()
          case "b" => Set("d")
          case "c" => Set("b")
          case "d" => Set("a", "c")
        })
      }

    }

  }
}
