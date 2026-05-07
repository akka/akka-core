/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.scaladsl

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.cluster.typed.Leave
import akka.pattern.AskTimeoutException
import akka.serialization.jackson.CborSerializable
import akka.util.Timeout

object ClusterShardingSpec {
  val config = ConfigFactory.parseString("""
      akka.actor.provider = cluster

      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1

      akka.cluster.jmx.multi-mbeans-in-same-jvm = on

      akka.cluster.sharding.number-of-shards = 10

      akka.coordinated-shutdown.terminate-actor-system = off
      akka.coordinated-shutdown.run-by-actor-system-terminate = off
    """)

  sealed trait TestProtocol extends CborSerializable
  final case class ReplyPlz(toMe: ActorRef[String]) extends TestProtocol
  final case class WhoAreYou(replyTo: ActorRef[String]) extends TestProtocol
  final case class WhoAreYou2(x: Int, replyTo: ActorRef[String]) extends TestProtocol
  final case class StopPlz() extends TestProtocol
  final case class PassivatePlz() extends TestProtocol

  sealed trait IdTestProtocol extends CborSerializable
  final case class IdReplyPlz(id: String, toMe: ActorRef[String]) extends IdTestProtocol
  final case class IdWhoAreYou(id: String, replyTo: ActorRef[String]) extends IdTestProtocol
  final case class IdStopPlz() extends IdTestProtocol

  final case class TheReply(s: String)

  val typeKeyWithEnvelopes = EntityTypeKey[TestProtocol]("envelope-shard")
  val typeKeyWithoutEnvelopes = EntityTypeKey[IdTestProtocol]("no-envelope-shard")

  def behavior(shard: ActorRef[ClusterSharding.ShardCommand], stopProbe: Option[ActorRef[String]] = None) =
    Behaviors
      .receivePartial[TestProtocol] {
        case (ctx, PassivatePlz()) =>
          shard ! ClusterSharding.Passivate(ctx.self)
          Behaviors.same

        case (_, StopPlz()) =>
          stopProbe.foreach(_ ! "StopPlz")
          Behaviors.stopped

        case (ctx, WhoAreYou(replyTo)) =>
          val address = Cluster(ctx.system).selfMember.address
          replyTo ! s"I'm ${ctx.self.path.name} at ${address.host.get}:${address.port.get} responding to $replyTo"
          Behaviors.same

        case (_, ReplyPlz(toMe)) =>
          toMe ! "Hello!"
          Behaviors.same
      }
      .receiveSignal {
        case (_, PostStop) =>
          stopProbe.foreach(_ ! "PostStop")
          Behaviors.same
      }

  def behaviorWithId() = Behaviors.receive[IdTestProtocol] {
    case (_, IdStopPlz()) =>
      Behaviors.stopped

    case (ctx, IdWhoAreYou(_, replyTo)) =>
      val address = Cluster(ctx.system).selfMember.address
      replyTo ! s"I'm ${ctx.self.path.name} at ${address.host.get}:${address.port.get}"
      Behaviors.same

    case (_, IdReplyPlz(_, toMe)) =>
      toMe ! "Hello!"
      Behaviors.same
  }

  val idTestProtocolMessageExtractor = ShardingMessageExtractor.noEnvelope[IdTestProtocol](10, IdStopPlz()) {
    case IdReplyPlz(id, _)  => id
    case IdWhoAreYou(id, _) => id
    case other              => throw new IllegalArgumentException(s"Unexpected message $other")
  }
}

class ClusterShardingSpec
    extends ScalaTestWithActorTestKit(ClusterShardingSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import ClusterShardingSpec._

  val sharding = ClusterSharding(system)

  val system2 = ActorSystem(Behaviors.ignore[Any], name = system.name, config = system.settings.config)
  val sharding2 = ClusterSharding(system2)

  override def afterAll(): Unit = {
    ActorTestKit.shutdown(system2)
    super.afterAll()
  }

  private val shardingRefSystem1WithEnvelope: ActorRef[ShardingEnvelope[TestProtocol]] =
    sharding.init(Entity(typeKeyWithEnvelopes)(ctx => behavior(ctx.shard)).withStopMessage(StopPlz()))

  private val shardingRefSystem2WithEnvelope: ActorRef[ShardingEnvelope[TestProtocol]] =
    sharding2.init(Entity(typeKeyWithEnvelopes)(ctx => behavior(ctx.shard)).withStopMessage(StopPlz()))

  private val shardingRefSystem1WithoutEnvelope: ActorRef[IdTestProtocol] = sharding.init(
    Entity(typeKeyWithoutEnvelopes)(_ => behaviorWithId())
      .withMessageExtractor(ShardingMessageExtractor.noEnvelope[IdTestProtocol](10, IdStopPlz()) {
        case IdReplyPlz(id, _)  => id
        case IdWhoAreYou(id, _) => id
        case other              => throw new IllegalArgumentException(s"Unexpected message $other")
      })
      .withStopMessage(IdStopPlz()))

  private val shardingRefSystem2WithoutEnvelope: ActorRef[IdTestProtocol] = sharding2.init(
    Entity(typeKeyWithoutEnvelopes)(_ => behaviorWithId())
      .withMessageExtractor(idTestProtocolMessageExtractor)
      .withStopMessage(IdStopPlz()))

  def totalEntityCount1(): Int = {
    import akka.pattern.ask
    implicit val timeout: Timeout = Timeout(6.seconds)
    val statsBefore =
      (shardingRefSystem1WithEnvelope.toClassic ? akka.cluster.sharding.ShardRegion.GetClusterShardingStats(5.seconds))
        .mapTo[akka.cluster.sharding.ShardRegion.ClusterShardingStats]
    val totalCount = statsBefore.futureValue.regions.values.flatMap(_.stats.values).sum
    totalCount
  }

  "Typed cluster sharding" must {

    "join cluster" in {
      Cluster(system).manager ! Join(Cluster(system).selfMember.address)
      Cluster(system2).manager ! Join(Cluster(system).selfMember.address)

      eventually {
        Cluster(system).state.members.unsorted.map(_.status) should ===(Set[MemberStatus](MemberStatus.Up))
        Cluster(system).state.members.size should ===(2)
      }
      eventually {
        Cluster(system2).state.members.unsorted.map(_.status) should ===(Set[MemberStatus](MemberStatus.Up))
        Cluster(system2).state.members.size should ===(2)
      }

    }

    "send messages via cluster sharding, using envelopes" in {
      (1 to 10).foreach { n =>
        val p = TestProbe[String]()
        shardingRefSystem1WithEnvelope ! ShardingEnvelope(s"test$n", ReplyPlz(p.ref))
        p.expectMessage("Hello!")
      }
      (11 to 21).foreach { n =>
        val p = TestProbe[String]()(system2)
        shardingRefSystem2WithEnvelope ! ShardingEnvelope(s"test$n", ReplyPlz(p.ref))
        p.expectMessage("Hello!")
      }
    }

    "send messages via cluster sharding, without envelopes" in {
      (1 to 10).foreach { n =>
        val p = TestProbe[String]()
        shardingRefSystem1WithoutEnvelope ! IdReplyPlz(s"test$n", p.ref)
        p.expectMessage("Hello!")
      }
      (11 to 21).foreach { n =>
        val p = TestProbe[String]()(system2)
        shardingRefSystem2WithoutEnvelope ! IdReplyPlz(s"test$n", p.ref)
        p.expectMessage("Hello!")
      }
    }

    "be able to passivate with custom stop message" in {
      val stopProbe = TestProbe[String]()
      val p = TestProbe[String]()
      val typeKey3 = EntityTypeKey[TestProtocol]("passivate-test")

      val shardingRef3: ActorRef[ShardingEnvelope[TestProtocol]] =
        sharding.init(Entity(typeKey3)(ctx => behavior(ctx.shard, Some(stopProbe.ref))).withStopMessage(StopPlz()))

      shardingRef3 ! ShardingEnvelope(s"test1", ReplyPlz(p.ref))
      p.expectMessage("Hello!")

      shardingRef3 ! ShardingEnvelope(s"test1", PassivatePlz())
      stopProbe.expectMessage("StopPlz")
      stopProbe.expectMessage("PostStop")

      shardingRef3 ! ShardingEnvelope(s"test1", ReplyPlz(p.ref))
      p.expectMessage("Hello!")
    }

    "be able to passivate with PoisonPill" in {
      val stopProbe = TestProbe[String]()
      val p = TestProbe[String]()
      val typeKey4 = EntityTypeKey[TestProtocol]("passivate-test-poison")

      val shardingRef4: ActorRef[ShardingEnvelope[TestProtocol]] =
        sharding.init(Entity(typeKey4)(ctx => behavior(ctx.shard, Some(stopProbe.ref))))
      // no StopPlz stopMessage

      shardingRef4 ! ShardingEnvelope(s"test4", ReplyPlz(p.ref))
      p.expectMessage("Hello!")

      shardingRef4 ! ShardingEnvelope(s"test4", PassivatePlz())
      // no StopPlz
      stopProbe.expectMessage("PostStop")

      shardingRef4 ! ShardingEnvelope(s"test4", ReplyPlz(p.ref))
      p.expectMessage("Hello!")
    }

    "be able to passivate via extension shard" in {
      val stopProbe = TestProbe[String]()
      val p = TestProbe[String]()
      val typeKey5 = EntityTypeKey[TestProtocol]("ext-passivate-test")

      val shardingRef5: ActorRef[ShardingEnvelope[TestProtocol]] =
        sharding.init(
          Entity(typeKey5)(_ => behavior(ClusterSharding(system).shard(typeKey5), Some(stopProbe.ref)))
            .withStopMessage(StopPlz()))

      shardingRef5 ! ShardingEnvelope(s"test1", ReplyPlz(p.ref))
      p.expectMessage("Hello!")

      shardingRef5 ! ShardingEnvelope(s"test1", PassivatePlz())
      stopProbe.expectMessage("StopPlz")
      stopProbe.expectMessage("PostStop")

      shardingRef5 ! ShardingEnvelope(s"test1", ReplyPlz(p.ref))
      p.expectMessage("Hello!")
    }

    "fail if init sharding for already used typeName, but with a different type" in {
      // sharding has been already initialized with EntityTypeKey[TestProtocol]("envelope-shard")
      val ex = intercept[Exception] {
        sharding.init(
          Entity(EntityTypeKey[IdTestProtocol]("envelope-shard"))(_ => behaviorWithId()).withStopMessage(IdStopPlz()))
      }

      ex.getMessage should include("already initialized")
    }

    "EntityRef - tell" in {
      val charlieRef = sharding.entityRefFor(typeKeyWithEnvelopes, "charlie")

      val p = TestProbe[String]()

      charlieRef ! WhoAreYou(p.ref)
      p.receiveMessage() should startWith("I'm charlie")

      charlieRef.tell(WhoAreYou(p.ref))
      p.receiveMessage() should startWith("I'm charlie")

      charlieRef ! StopPlz()
    }

    "EntityRef - ask" in {
      val bobRef = sharding.entityRefFor(typeKeyWithEnvelopes, "bob")
      val aliceRef = sharding.entityRefFor(typeKeyWithEnvelopes, "alice")

      val reply1 = bobRef.ask(WhoAreYou(_))
      val response = reply1.futureValue.asInstanceOf[String]
      response should startWith("I'm bob")
      // typekey and entity id encoded in promise ref path
      response should include(s"${typeKeyWithEnvelopes.name}-bob")

      val reply2 = aliceRef.ask(WhoAreYou(_))
      reply2.futureValue.asInstanceOf[String] should startWith("I'm alice")

      bobRef ! StopPlz()
    }

    "EntityRef - ActorContext.ask" in {
      val peterRef = sharding.entityRefFor(typeKeyWithEnvelopes, "peter")

      val p = TestProbe[TheReply]()

      spawn(Behaviors.setup[TheReply] { ctx =>
        ctx.ask(peterRef, WhoAreYou.apply) {
          case Success(name) => TheReply(name)
          case Failure(ex)   => TheReply(ex.getMessage)
        }

        Behaviors.receiveMessage[TheReply] { reply =>
          p.ref ! reply
          Behaviors.same
        }
      })

      val response = p.receiveMessage()
      response.s should startWith("I'm peter")
      // typekey and entity id encoded in promise ref path
      response.s should include(s"${typeKeyWithEnvelopes.name}-peter")

      peterRef ! StopPlz()

      // make sure request with multiple parameters compile
      Behaviors.setup[TheReply] { ctx =>
        ctx.ask[WhoAreYou2, String](peterRef, WhoAreYou2(17, _)) {
          case Success(name) => TheReply(name)
          case Failure(ex)   => TheReply(ex.getMessage)
        }
        Behaviors.empty
      }
    }

    "EntityRef - AskTimeoutException" in {
      val ignorantKey = EntityTypeKey[TestProtocol]("ignorant")

      sharding.init(Entity(ignorantKey)(_ => Behaviors.ignore[TestProtocol]).withStopMessage(StopPlz()))

      val ref = sharding.entityRefFor(ignorantKey, "sloppy")

      val reply = ref.ask(WhoAreYou(_))(Timeout(10.millis))
      val exc = reply.failed.futureValue
      exc.getClass should ===(classOf[AskTimeoutException])
      exc.getMessage should startWith("Ask timed out on")
      exc.getMessage should include(ignorantKey.toString)
      exc.getMessage should include("sloppy") // the entity id
      exc.getMessage should include(ref.toString)
      exc.getMessage should include(s"[${classOf[WhoAreYou].getName}]") // message class
      exc.getMessage should include("[10 ms]") // timeout
    }

    "handle classic StartEntity message" in {
      // it is normally using envelopes, but the classic StartEntity message can be sent internally,
      // e.g. for remember entities

      val totalCountBefore = totalEntityCount1()

      val p = TestProbe[Any]()
      shardingRefSystem1WithEnvelope.toClassic
        .tell(akka.cluster.sharding.ShardRegion.StartEntity("startEntity-1"), p.ref.toClassic)
      p.expectMessageType[akka.cluster.sharding.ShardRegion.StartEntityAck]

      eventually {
        val totalCountAfter = totalEntityCount1()
        totalCountAfter should ===(totalCountBefore + 1)
      }
    }

    "handle typed StartEntity message" in {
      val totalCountBefore = totalEntityCount1()

      shardingRefSystem1WithEnvelope ! StartEntity("startEntity-2")

      eventually {
        val totalCountAfter = totalEntityCount1()
        totalCountAfter should ===(totalCountBefore + 1)
      }
    }

    "use the stopMessage for leaving/rebalance" in {
      // use many entities to reduce the risk that all are hashed to the same shard/node
      val numberOfEntities = 100
      val probe1 = TestProbe[String]()
      (1 to numberOfEntities).foreach { n =>
        shardingRefSystem1WithEnvelope ! ShardingEnvelope(s"test$n", WhoAreYou(probe1.ref))
      }
      val replies1 = probe1.receiveMessages(numberOfEntities, 10.seconds)

      Cluster(system2).manager ! Leave(Cluster(system2).selfMember.address)

      eventually {
        // if it wouldn't receive the StopPlz and stop the leaving would take longer and this would fail
        Cluster(system2).isTerminated should ===(true)
      }

      val probe2 = TestProbe[String]()
      (1 to numberOfEntities).foreach { n =>
        shardingRefSystem1WithEnvelope ! ShardingEnvelope(s"test$n", WhoAreYou(probe2.ref))
      }
      val replies2 = probe2.receiveMessages(numberOfEntities, 10.seconds)
      replies2 should !==(replies1) // different addresses
    }
  }
}
