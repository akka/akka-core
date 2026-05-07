/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.testkit.scaladsl

import scala.concurrent.Future

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.sharding.ShardCoordinator
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.typed.ClusterShardingQuery
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.javadsl
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

object TestEntityRefSpec {

  final case class AskReq(s: String, replyTo: ActorRef[Done])

  private val entityTypeKey1 = EntityTypeKey[String]("Key1")
  private val entityTypeKey2 = EntityTypeKey[AskReq]("Key2")

}

class TestEntityRefSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import TestEntityRefSpec._

  "TestEntityRef" must {
    "send message with tell" in {
      val probe = createTestProbe[String]()
      val entityRef = TestEntityRef(entityTypeKey1, "entity-1", probe.ref)
      entityRef ! "msg1"
      entityRef ! "msg2"
      probe.expectMessage("msg1")
      probe.expectMessage("msg2")
    }

    "send message with ask" in {
      val probe = createTestProbe[AskReq]()
      val entityRef = TestEntityRef(entityTypeKey2, "entity-2", probe.ref)
      val reply: Future[Done] = entityRef.ask[Done](replyTo => AskReq("req1", replyTo))
      probe.receiveMessage().replyTo ! Done
      reply.futureValue should ===(Done)
    }

    "be useful will ClusterShardingStub" in {
      val probes = Map("entity-1" -> createTestProbe[String](), "entity-2" -> createTestProbe[String]())

      class ClusterShardingStub extends javadsl.ClusterSharding with ClusterSharding {

        override def init[M, E](entity: Entity[M, E]): ActorRef[E] = ???

        override def entityRefFor[M](
            typeKey: EntityTypeKey[M],
            entityId: String,
            dataCenter: DataCenter): EntityRef[M] =
          TestEntityRef(typeKey, entityId, probes(entityId).ref.asInstanceOf[ActorRef[M]])

        override def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M] =
          entityRefFor(typeKey, entityId, "default")

        override def shard(typeKey: EntityTypeKey[_]): ActorRef[ClusterSharding.ShardCommand] = ???

        override def shard(typeKey: javadsl.EntityTypeKey[_]): ActorRef[javadsl.ClusterSharding.ShardCommand] = ???

        override def shardState: ActorRef[ClusterShardingQuery] = ???

        override def defaultShardAllocationStrategy(
            settings: ClusterShardingSettings): ShardCoordinator.ShardAllocationStrategy =
          ShardAllocationStrategy.leastShardAllocationStrategy(1, 0.1)

        // below are for javadsl
        override def init[M, E](entity: javadsl.Entity[M, E]): ActorRef[E] = ???

        override def entityRefFor[M](typeKey: javadsl.EntityTypeKey[M], entityId: String): javadsl.EntityRef[M] = ???

        override def entityRefFor[M](
            typeKey: javadsl.EntityTypeKey[M],
            entityId: String,
            dataCenter: String): javadsl.EntityRef[M] = ???

      }

      val sharding: ClusterSharding = new ClusterShardingStub

      sharding.entityRefFor(entityTypeKey1, "entity-1") ! "msg1"
      probes("entity-1").expectMessage("msg1")
      sharding.entityRefFor(entityTypeKey1, "entity-2") ! "msg2"
      probes("entity-2").expectMessage("msg2")
    }
  }
}
