/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.jackson.CborSerializable

object ClusterSingletonApiSpec {

  val config = ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1
      akka.cluster.jmx.multi-mbeans-in-same-jvm = on
    """)

  sealed trait PingProtocol
  case object Pong extends CborSerializable
  case class Ping(respondTo: ActorRef[Pong.type]) extends PingProtocol with CborSerializable

  case object Perish extends PingProtocol with CborSerializable

  val pingPong = Behaviors.receiveMessage[PingProtocol] {
    case Ping(respondTo) =>
      respondTo ! Pong
      Behaviors.same

    case Perish =>
      Behaviors.stopped

  }

}

class ClusterSingletonApiSpec
    extends ScalaTestWithActorTestKit(ClusterSingletonApiSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ClusterSingletonApiSpec._

  implicit val testSettings: TestKitSettings = TestKitSettings(system)
  val clusterNode1 = Cluster(system)
  val classicSystem1 = system.toClassic

  val system2 = akka.actor.ActorSystem(
    system.name,
    ConfigFactory.parseString("""
        akka.cluster.roles = ["singleton"]
      """).withFallback(system.settings.config))
  val adaptedSystem2 = system2.toTyped
  val clusterNode2 = Cluster(adaptedSystem2)

  "A typed cluster singleton" must {

    "be accessible from two nodes in a cluster" in {
      val node1UpProbe = TestProbe[SelfUp]()(system)
      clusterNode1.subscriptions ! Subscribe(node1UpProbe.ref, classOf[SelfUp])

      val node2UpProbe = TestProbe[SelfUp]()(adaptedSystem2)
      clusterNode1.subscriptions ! Subscribe(node2UpProbe.ref, classOf[SelfUp])

      clusterNode1.manager ! Join(clusterNode1.selfMember.address)
      clusterNode2.manager ! Join(clusterNode1.selfMember.address)

      node1UpProbe.receiveMessage()
      node2UpProbe.receiveMessage()

      val cs1: ClusterSingleton = ClusterSingleton(system)
      val cs2 = ClusterSingleton(adaptedSystem2)

      val settings = ClusterSingletonSettings(system).withRole("singleton")
      val node1ref = cs1.init(SingletonActor(pingPong, "ping-pong").withStopMessage(Perish).withSettings(settings))
      val node2ref = cs2.init(SingletonActor(pingPong, "ping-pong").withStopMessage(Perish).withSettings(settings))

      // subsequent spawning returns the same refs
      cs1.init(SingletonActor(pingPong, "ping-pong").withStopMessage(Perish).withSettings(settings)) should ===(
        node1ref)
      cs2.init(SingletonActor(pingPong, "ping-pong").withStopMessage(Perish).withSettings(settings)) should ===(
        node2ref)

      val node1PongProbe = TestProbe[Pong.type]()(system)
      val node2PongProbe = TestProbe[Pong.type]()(adaptedSystem2)

      node1PongProbe.awaitAssert({
        node1ref ! Ping(node1PongProbe.ref)
        node1PongProbe.expectMessage(Pong)
      }, 3.seconds)

      node2PongProbe.awaitAssert({
        node2ref ! Ping(node2PongProbe.ref)
        node2PongProbe.expectMessage(Pong)
      }, 3.seconds)

    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Await.result(system2.terminate(), 3.seconds)
  }

}
