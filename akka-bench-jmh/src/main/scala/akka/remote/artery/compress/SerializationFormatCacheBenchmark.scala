/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import scala.annotation.nowarn
import scala.concurrent.Promise

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.OperationsPerInvocation
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.pattern.PromiseActorRef
import akka.remote.artery.SerializationFormatCache
import akka.serialization.Serialization

/**
 * Actually more like specific benchmarks for the few concrete LRU cache usages
 */
@Fork(1)
@State(Scope.Benchmark)
@nowarn
class SerializationFormatCacheBenchmark {

  // note 1 means only top level no temporary at all
  @Param(Array("1", "2", "5", "10"))
  private var everyNToToplevel = 0
  // a few "normal" top level actors communicating
  @Param(Array("100"))
  private var uniqueTopLevelRefs = 0
  // we want to simulate one per request-response, but create upfront, so very high number
  @Param(Array("100000"))
  private var uniqueTemporaryRefs = 0

  private var system: ActorSystem = _
  // hardocoded capacity of 1024
  // note that this is not quite realistic, with a single cache,
  // in practice there are N caches, one in each outgoing artery lane
  private var cache: SerializationFormatCache = _
  private var temporaryActorRefs: Array[ActorRef] = _
  private var topLevelActorRefs: Array[ActorRef] = _

  object Parent {
    def props(childCount: Int, childProps: Props) = Props(new Parent(childCount, childProps))
  }
  class Parent(childCount: Int, childProps: Props) extends Actor {
    val children = (0 to childCount).map(_ => context.actorOf(childProps))
    def receive = PartialFunction.empty
  }

  @Setup
  def init(): Unit = {
    system = ActorSystem("SerializationFormatCacheBenchmark")
    temporaryActorRefs = Array.tabulate(uniqueTemporaryRefs)(
      n =>
        new PromiseActorRef(
          system.asInstanceOf[ExtendedActorSystem].provider,
          Promise(),
          "Any",
          // request path is encoded in this string
          s"_user_region_shard${n % 100}_entitypretendid${n}"))

    topLevelActorRefs = Array.tabulate(uniqueTopLevelRefs)(n => system.actorOf(Props.empty, s"actor_$n"))
  }

  // new empty cache instance each iteration to have more control over cached contents
  @Setup(Level.Iteration)
  def perRunSetup(): Unit = {
    cache = new SerializationFormatCache
  }

  @TearDown
  def shutdown(): Unit = {
    system.terminate()
  }

  @Benchmark
  @OperationsPerInvocation(2000)
  def useCache(blackhole: Blackhole): Unit = {
    // serialization requires this
    Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
      var i: Int = 0
      while (i < 2000) {
        val actorRef =
          if (i % everyNToToplevel == 0) topLevelActorRefs(i % uniqueTopLevelRefs)
          else temporaryActorRefs(i % uniqueTemporaryRefs)
        blackhole.consume(cache.getOrCompute(actorRef))
        i += 1
      }
    }
  }

}
