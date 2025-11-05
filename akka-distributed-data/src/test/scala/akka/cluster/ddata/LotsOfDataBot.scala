/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props

/**
 * This "sample" simulates lots of data entries, and can be used for
 * optimizing replication (e.g. catch-up when adding more nodes).
 */
object LotsOfDataBot {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("2551", "2552", "0"))
    else
      startup(args.toIndexedSeq)
  }

  def startup(ports: Seq[String]): Unit = {
    ports.foreach { port =>
      // Override the configuration of the port
      val config = ConfigFactory
        .parseString("akka.remote.artery.canonical.port=" + port)
        .withFallback(
          ConfigFactory.load(ConfigFactory.parseString("""
            passive = off
            max-entries = 100000
            akka.actor.provider = "cluster"
            akka.remote {
              artery.canonical {
                hostname = "127.0.0.1"
                port = 0
              }
            }

            akka.cluster {
              seed-nodes = [
                "akka://ClusterSystem@127.0.0.1:2551",
                "akka://ClusterSystem@127.0.0.1:2552"]

              downing-provider-class = akka.cluster.testkit.AutoDowning
              testkit.auto-down-unreachable-after = 10s
            }
            """)))

      // Create an Akka system
      val system = ActorSystem("ClusterSystem", config)
      // Create an actor that handles cluster domain events
      system.actorOf(Props[LotsOfDataBot](), name = "dataBot")
    }
  }

  private case object Tick

}

class LotsOfDataBot extends Actor with ActorLogging {
  import LotsOfDataBot._
  import Replicator._

  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
  val replicator = DistributedData(context.system).replicator

  import context.dispatcher
  val isPassive = context.system.settings.config.getBoolean("passive")
  var tickTask =
    if (isPassive)
      context.system.scheduler.scheduleWithFixedDelay(1.seconds, 1.seconds, self, Tick)
    else
      context.system.scheduler.scheduleWithFixedDelay(20.millis, 20.millis, self, Tick)

  val startTime = System.nanoTime()
  var count = 1L
  val maxEntries = context.system.settings.config.getInt("max-entries")

  def receive = if (isPassive) passive else active

  def active: Receive = {
    case Tick =>
      val loop = if (count >= maxEntries) 1 else 100
      for (_ <- 1 to loop) {
        count += 1
        if (count % 10000 == 0)
          log.info("Reached {} entries", count)
        if (count == maxEntries) {
          log.info("Reached {} entries", count)
          tickTask.cancel()
          tickTask = context.system.scheduler.scheduleWithFixedDelay(1.seconds, 1.seconds, self, Tick)
        }
        val key = ORSetKey[String]((count % maxEntries).toString)
        if (count <= 100)
          replicator ! Subscribe(key, self)
        val s = ThreadLocalRandom.current().nextInt(97, 123).toChar.toString
        if (count <= maxEntries || ThreadLocalRandom.current().nextBoolean()) {
          // add
          replicator ! Update(key, ORSet(), WriteLocal)(_ :+ s)
        } else {
          // remove
          replicator ! Update(key, ORSet(), WriteLocal)(_.remove(s))
        }
      }

    case _: UpdateResponse[_] => // ignore

    case c @ Changed(ORSetKey(id)) =>
      val elements = c.dataValue match {
        case ORSet(e) => e
        case _        => throw new RuntimeException()
      }
      log.info("Current elements: {} -> {}", id, elements)
  }

  def passive: Receive = {
    case Tick =>
      if (!tickTask.isCancelled)
        replicator ! GetKeyIds
    case GetKeyIdsResult(keys) =>
      if (keys.size >= maxEntries) {
        tickTask.cancel()
        val duration = (System.nanoTime() - startTime).nanos.toMillis
        log.info("It took {} ms to replicate {} entries", duration, keys.size)
      }
    case c @ Changed(ORSetKey(id)) =>
      val elements = c.dataValue match {
        case ORSet(e) => e
        case _        => throw new RuntimeException()
      }
      log.info("Current elements: {} -> {}", id, elements)
  }

  override def postStop(): Unit = tickTask.cancel()

}
