/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{ Scope => JmhScope }
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup

import akka.actor.Address
import akka.cluster.UniqueAddress

@Fork(2)
@State(JmhScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 4)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class ORSetMergeBenchmark {

  @Param(Array("1", "10", "20", "100"))
  var set1Size = 0

  val nodeA = UniqueAddress(Address("akka", "Sys", "aaaa", 2552), 1L)
  val nodeB = UniqueAddress(nodeA.address.copy(host = Some("bbbb")), 2L)
  val nodeC = UniqueAddress(nodeA.address.copy(host = Some("cccc")), 3L)
  val nodeD = UniqueAddress(nodeA.address.copy(host = Some("dddd")), 4L)
  val nodeE = UniqueAddress(nodeA.address.copy(host = Some("eeee")), 5L)
  val nodes = Vector(nodeA, nodeB, nodeC, nodeD, nodeE)
  val nodesIndex = Iterator.from(0)
  def nextNode(): UniqueAddress = nodes(nodesIndex.next() % nodes.size)

  var set1: ORSet[String] = _
  var addFromSameNode: ORSet[String] = _
  var addFromOtherNode: ORSet[String] = _
  var complex1: ORSet[String] = _
  var complex2: ORSet[String] = _
  var elem1: String = _
  var elem2: String = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    set1 = (1 to set1Size).foldLeft(ORSet.empty[String])((s, n) => s.add(nextNode(), "elem" + n))
    addFromSameNode = set1.add(nodeA, "elem" + set1Size + 1).merge(set1)
    addFromOtherNode = set1.add(nodeB, "elem" + set1Size + 1).merge(set1)
    complex1 = set1.add(nodeB, "a").add(nodeC, "b").remove(nodeD, "elem" + set1Size).merge(set1)
    complex2 = set1.add(nodeA, "a").add(nodeA, "c").add(nodeB, "d").merge(set1)
    elem1 = "elem" + (set1Size + 1)
    elem2 = "elem" + (set1Size + 2)
  }

  @Benchmark
  def mergeAddFromSameNode: ORSet[String] = {
    // this is the scenario when updating and then merging with local value
    // set2 produced by modify function
    val set2 = set1.add(nodeA, elem1).add(nodeA, elem2)
    // replicator merges with local value
    set1.merge(set2)
  }

  @Benchmark
  def mergeAddFromOtherNode: ORSet[String] = set1.merge(addFromOtherNode)

  @Benchmark
  def mergeAddFromBothNodes: ORSet[String] = addFromSameNode.merge(addFromOtherNode)

  @Benchmark
  def mergeComplex: ORSet[String] = complex1.merge(complex2)

}
