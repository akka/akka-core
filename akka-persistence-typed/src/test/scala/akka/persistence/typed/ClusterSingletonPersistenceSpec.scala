/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.typed.ClusterSingleton
import akka.cluster.typed.SingletonActor
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

object ClusterSingletonPersistenceSpec {
  val config = ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1

      akka.coordinated-shutdown.terminate-actor-system = off
      akka.coordinated-shutdown.run-by-actor-system-terminate = off

      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    """.stripMargin)

  sealed trait Command
  final case class Add(s: String) extends Command
  final case class Get(replyTo: ActorRef[String]) extends Command
  private case object StopPlz extends Command

  val persistentActor: Behavior[Command] =
    EventSourcedBehavior[Command, String, String](
      persistenceId = PersistenceId.ofUniqueId("TheSingleton"),
      emptyState = "",
      commandHandler = (state, cmd) =>
        cmd match {
          case Add(s) => Effect.persist(s)
          case Get(replyTo) =>
            replyTo ! state
            Effect.none
          case StopPlz => Effect.stop()
        },
      eventHandler = (state, evt) => if (state.isEmpty) evt else state + "|" + evt)

}

class ClusterSingletonPersistenceSpec
    extends ScalaTestWithActorTestKit(ClusterSingletonPersistenceSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ClusterSingletonPersistenceSpec._

  import akka.actor.typed.scaladsl.adapter._

  implicit val s: ActorSystem[Nothing] = system

  implicit val classicSystem: actor.ActorSystem = system.toClassic
  private val classicCluster = akka.cluster.Cluster(classicSystem)

  "A typed cluster singleton with persistent actor" must {

    classicCluster.join(classicCluster.selfAddress)

    "start persistent actor" in {
      val ref = ClusterSingleton(system).init(SingletonActor(persistentActor, "singleton").withStopMessage(StopPlz))

      val p = TestProbe[String]()

      ref ! Add("a")
      ref ! Add("b")
      ref ! Add("c")
      ref ! Get(p.ref)
      p.expectMessage("a|b|c")
    }
  }
}
