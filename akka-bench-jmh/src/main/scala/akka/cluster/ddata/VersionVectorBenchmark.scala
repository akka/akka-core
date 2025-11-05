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
class VersionVectorBenchmark {

  @Param(Array("1", "2", "5"))
  var size = 0

  val nodeA = UniqueAddress(Address("akka", "Sys", "aaaa", 2552), 1L)
  val nodeB = UniqueAddress(nodeA.address.copy(host = Some("bbbb")), 2L)
  val nodeC = UniqueAddress(nodeA.address.copy(host = Some("cccc")), 3L)
  val nodeD = UniqueAddress(nodeA.address.copy(host = Some("dddd")), 4L)
  val nodeE = UniqueAddress(nodeA.address.copy(host = Some("eeee")), 5L)
  val nodes = Vector(nodeA, nodeB, nodeC, nodeD, nodeE)
  val nodesIndex = Iterator.from(0)
  def nextNode(): UniqueAddress = nodes(nodesIndex.next() % nodes.size)

  var vv1: VersionVector = _
  var vv2: VersionVector = _
  var vv3: VersionVector = _
  var dot1: VersionVector = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    vv1 = (1 to size).foldLeft(VersionVector.empty)((vv, _) => vv + nextNode())
    vv2 = vv1 + nextNode()
    vv3 = vv1 + nextNode()
    dot1 = VersionVector(nodeA, vv1.versionAt(nodeA))
  }

  @Benchmark
  def increment: VersionVector = (vv1 + nodeA)

  @Benchmark
  def compareSame1: Boolean = (vv1 == dot1)

  @Benchmark
  def compareSame2: Boolean = (vv2 == dot1)

  @Benchmark
  def compareGreaterThan1: Boolean = (vv1 > dot1)

  @Benchmark
  def compareGreaterThan2: Boolean = (vv2 > dot1)

  @Benchmark
  def merge: VersionVector = vv1.merge(vv2)

  @Benchmark
  def mergeConflicting: VersionVector = vv2.merge(vv3)

}
