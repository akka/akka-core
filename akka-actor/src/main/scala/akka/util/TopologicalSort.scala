/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.immutable
import scala.collection.mutable

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TopologicalSort {

  /**
   * INTERNAL API: https://en.wikipedia.org/wiki/Topological_sorting
   */
  private[akka] def topologicalSort[A](nodes: immutable.Seq[A], edges: A => Set[A]): immutable.Seq[A] = {
    if (nodes.isEmpty)
      Nil
    else if (nodes.size == 1 && edges(nodes.head).isEmpty)
      nodes.toList // no need to sort (or check cycle)
    else {
      var result = List.empty[A]
      val unmarked = mutable.LinkedHashSet.from(nodes) // LinkedHashSet to retain original order
      var tempMark = Set.empty[A] // for detecting cycles

      while (unmarked.nonEmpty) {
        depthFirstSearch(unmarked.head)
      }

      def depthFirstSearch(u: A): Unit = {
        if (tempMark(u)) {
          val allDependencies = nodes.map(n => n -> edges(n)).toMap
          throw new IllegalArgumentException(
            "Cycle detected in graph. It must be a DAG. " +
            s"edge [$u] depends transitively on itself. All dependencies: $allDependencies")
        }
        if (unmarked(u)) {
          tempMark += u
          val dependsOn = edges(u)
          if (dependsOn.nonEmpty)
            dependsOn.foreach(depthFirstSearch)
          unmarked -= u // permanent mark
          tempMark -= u
          result = u :: result
        }
      }

      result.reverse
    }
  }

}
