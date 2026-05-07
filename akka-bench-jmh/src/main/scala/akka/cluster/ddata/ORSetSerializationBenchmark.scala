/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations.{ Scope => JmhScope }
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.serialization.SerializationExtension
import akka.serialization.Serializers

@Fork(2)
@State(JmhScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 4)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
class ORSetSerializationBenchmark {

  private val config = ConfigFactory.parseString("""
    akka.actor.provider=cluster
    akka.remote.artery.canonical.port = 0
    """)

  private val system1 = ActorSystem("ORSetSerializationBenchmark", config)
  private val system2 = ActorSystem("ORSetSerializationBenchmark", config)

  private val ref1 = (1 to 10).map(n => system1.actorOf(Props.empty, s"ref1-$n"))
  private val ref2 = (1 to 10).map(n => system2.actorOf(Props.empty, s"ref2-$n"))

  private val orSet = {
    val selfUniqueAddress1 = SelfUniqueAddress(Cluster(system1).selfUniqueAddress)
    val selfUniqueAddress2 = SelfUniqueAddress(Cluster(system2).selfUniqueAddress)
    val set1 = ref1.foldLeft(ORSet.empty[ActorRef]) { case (acc, r) => acc.add(selfUniqueAddress1, r) }
    val set2 = ref2.foldLeft(ORSet.empty[ActorRef]) { case (acc, r) => acc.add(selfUniqueAddress2, r) }
    set1.merge(set2)
  }

  private val serialization = SerializationExtension(system1)
  private val serializerId = serialization.findSerializerFor(orSet).identifier
  private val manifest = Serializers.manifestFor(serialization.findSerializerFor(orSet), orSet)

  @TearDown
  def shutdown(): Unit = {
    Await.result(system1.terminate(), 5.seconds)
    Await.result(system2.terminate(), 5.seconds)
  }

  @Benchmark
  def serializeRoundtrip: ORSet[ActorRef] = {
    val bytes = serialization.serialize(orSet).get
    serialization.deserialize(bytes, serializerId, manifest).get.asInstanceOf[ORSet[ActorRef]]
  }

}
