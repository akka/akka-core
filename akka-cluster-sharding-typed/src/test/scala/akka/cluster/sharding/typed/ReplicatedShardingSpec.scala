/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.nowarn

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.ReplicatedShardingSpec.DataCenter
import akka.cluster.sharding.typed.ReplicatedShardingSpec.MyReplicatedIntSet
import akka.cluster.sharding.typed.ReplicatedShardingSpec.MyReplicatedStringSet
import akka.cluster.sharding.typed.ReplicatedShardingSpec.Normal
import akka.cluster.sharding.typed.ReplicatedShardingSpec.ReplicationType
import akka.cluster.sharding.typed.ReplicatedShardingSpec.Role
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.serialization.jackson.CborSerializable

@nowarn("msg=Use Akka Distributed Cluster")
object ReplicatedShardingSpec {
  def commonConfig = ConfigFactory.parseString("""
      akka.loglevel = DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = "cluster"
      akka.remote.artery.canonical.port = 0""").withFallback(PersistenceTestKitPlugin.config)

  def roleAConfig = ConfigFactory.parseString("""
            akka.cluster.roles = ["DC-A"]
            """.stripMargin).withFallback(commonConfig)

  def roleBConfig = ConfigFactory.parseString("""
            akka.cluster.roles = ["DC-B"]
            """.stripMargin).withFallback(commonConfig)

  def dcAConfig = ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "DC-A"
      """).withFallback(commonConfig)

  def dcBConfig = ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "DC-B"
      """).withFallback(commonConfig)

  sealed trait ReplicationType
  case object Role extends ReplicationType
  case object DataCenter extends ReplicationType
  case object Normal extends ReplicationType

  val AllReplicas = Set(ReplicaId("DC-A"), ReplicaId("DC-B"))

  object MyReplicatedStringSet {
    sealed trait Command extends CborSerializable
    case class Add(text: String) extends Command
    case class GetTexts(replyTo: ActorRef[Texts]) extends Command

    case class Texts(texts: Set[String]) extends CborSerializable

    def apply(replicationId: ReplicationId): Behavior[Command] =
      ReplicatedEventSourcing.commonJournalConfig( // it isn't really shared as it is in memory
        replicationId,
        AllReplicas,
        PersistenceTestKitReadJournal.Identifier) { replicationContext =>
        EventSourcedBehavior[Command, String, Set[String]](
          replicationContext.persistenceId,
          Set.empty[String],
          (state, command) =>
            command match {
              case Add(text) =>
                Effect.persist(text)
              case GetTexts(replyTo) =>
                replyTo ! Texts(state)
                Effect.none
            },
          (state, event) => state + event)
          .withJournalPluginId(PersistenceTestKitPlugin.PluginId)
          .withEventPublishing(true)
      }

    def provider(replicationType: ReplicationType) =
      ReplicatedEntityProvider[MyReplicatedStringSet.Command](
        // all replicas
        "StringSet",
        AllReplicas) { (entityTypeKey, replicaId) =>
        // factory for replicated entity for a given replica
        val entity = {
          val e = Entity(entityTypeKey) { entityContext =>
            MyReplicatedStringSet(ReplicationId.fromString(entityContext.entityId))
          }
          replicationType match {
            case Role =>
              e.withRole(replicaId.id)
            case DataCenter =>
              e.withDataCenter(replicaId.id)
            case Normal =>
              e
          }
        }
        ReplicatedEntity(replicaId, entity)
      }.withDirectReplication(true)

  }

  object MyReplicatedIntSet {
    sealed trait Command extends CborSerializable
    case class Add(text: Int) extends Command
    case class GetInts(replyTo: ActorRef[Ints]) extends Command
    case class Ints(ints: Set[Int]) extends CborSerializable

    def apply(id: ReplicationId, allReplicas: Set[ReplicaId]): Behavior[Command] =
      ReplicatedEventSourcing.commonJournalConfig( // it isn't really shared as it is in memory
        id,
        allReplicas,
        PersistenceTestKitReadJournal.Identifier) { replicationContext =>
        EventSourcedBehavior[Command, Int, Set[Int]](
          replicationContext.persistenceId,
          Set.empty[Int],
          (state, command) =>
            command match {
              case Add(int) =>
                Effect.persist(int)
              case GetInts(replyTo) =>
                replyTo ! Ints(state)
                Effect.none
            },
          (state, event) => state + event)
          .withJournalPluginId(PersistenceTestKitPlugin.PluginId)
          .withEventPublishing(true)
      }

    def provider(replicationType: ReplicationType) =
      ReplicatedEntityProvider[MyReplicatedIntSet.Command]("IntSet", AllReplicas) { (entityTypeKey, replicaId) =>
        val entity = {
          val e = Entity(entityTypeKey) { entityContext =>
            val replicationId = ReplicationId.fromString(entityContext.entityId)
            MyReplicatedIntSet(replicationId, AllReplicas)
          }
          replicationType match {
            case Role =>
              e.withRole(replicaId.id)
            case DataCenter =>
              e.withDataCenter(replicaId.id)
            case Normal =>
              e
          }
        }
        ReplicatedEntity(replicaId, entity)
      }.withDirectReplication(true)
  }
}

object ProxyActor {
  sealed trait Command
  case class ForwardToRandomString(entityId: String, msg: MyReplicatedStringSet.Command) extends Command
  case class ForwardToAllString(entityId: String, msg: MyReplicatedStringSet.Command) extends Command
  case class ForwardToRandomInt(entityId: String, msg: MyReplicatedIntSet.Command) extends Command
  case class ForwardToAllInt(entityId: String, msg: MyReplicatedIntSet.Command) extends Command

  def apply(replicationType: ReplicationType): Behavior[Command] = Behaviors.setup { context =>
    val replicatedShardingStringSet: ReplicatedSharding[MyReplicatedStringSet.Command] =
      ReplicatedShardingExtension(context.system).init(MyReplicatedStringSet.provider(replicationType))
    val replicatedShardingIntSet: ReplicatedSharding[MyReplicatedIntSet.Command] =
      ReplicatedShardingExtension(context.system).init(MyReplicatedIntSet.provider(replicationType))

    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case ForwardToAllString(entityId, cmd) =>
          val entityRefs = replicatedShardingStringSet.entityRefsFor(entityId)
          ctx.log.info("Entity refs {}", entityRefs)
          entityRefs.foreach {
            case (replica, ref) =>
              ctx.log.info("Forwarding to replica {} ref {}", replica, ref)
              ref ! cmd
          }
          Behaviors.same
        case ForwardToRandomString(entityId, cmd) =>
          val refs = replicatedShardingStringSet.entityRefsFor(entityId)
          val chosenIdx = ThreadLocalRandom.current().nextInt(refs.size)
          val chosen = refs.values.toIndexedSeq(chosenIdx)
          ctx.log.info("Forwarding to {}", chosen)
          chosen ! cmd
          Behaviors.same
        case ForwardToAllInt(entityId, cmd) =>
          replicatedShardingIntSet.entityRefsFor(entityId).foreach {
            case (_, ref) => ref ! cmd
          }
          Behaviors.same
        case ForwardToRandomInt(entityId, cmd) =>
          val refs =
            replicatedShardingIntSet.entityRefsFor(entityId)
          val chosenIdx = ThreadLocalRandom.current().nextInt(refs.size)
          refs.values.toIndexedSeq(chosenIdx) ! cmd
          Behaviors.same
      }
    }
  }
}

class NormalReplicatedShardingSpec
    extends ReplicatedShardingSpec(Normal, ReplicatedShardingSpec.commonConfig, ReplicatedShardingSpec.commonConfig)
class RoleReplicatedShardingSpec
    extends ReplicatedShardingSpec(Role, ReplicatedShardingSpec.roleAConfig, ReplicatedShardingSpec.roleBConfig)
class DataCenterReplicatedShardingSpec
    extends ReplicatedShardingSpec(DataCenter, ReplicatedShardingSpec.dcAConfig, ReplicatedShardingSpec.dcBConfig)

abstract class ReplicatedShardingSpec(replicationType: ReplicationType, configA: Config, configB: Config)
    extends ScalaTestWithActorTestKit(configA)
    with AnyWordSpecLike
    with LogCapturing {

  // don't retry quite so quickly
  override implicit val patience: PatienceConfig =
    PatienceConfig(testKit.testKitSettings.DefaultTimeout.duration, Span(500, org.scalatest.time.Millis))

  val system2 = ActorSystem(Behaviors.ignore[Any], name = system.name, config = configB)

  override protected def afterAll(): Unit = {
    super.afterAll()
    ActorTestKit.shutdown(system2)
  }

  "Replicated sharding" should {

    "form a one node cluster" in {
      Cluster(system).manager ! Join(Cluster(system).selfMember.address)
      Cluster(system2).manager ! Join(Cluster(system).selfMember.address)

      eventually {
        Cluster(system).state.members.unsorted.size should ===(2)
        Cluster(system).state.members.unsorted.map(_.status) should ===(Set[MemberStatus](MemberStatus.Up))
      }
      eventually {
        Cluster(system2).state.members.unsorted.size should ===(2)
        Cluster(system2).state.members.unsorted.map(_.status) should ===(Set[MemberStatus](MemberStatus.Up))
      }
    }

    "start replicated sharding on both nodes" in {
      def start(sys: ActorSystem[_]) = {
        ReplicatedShardingExtension(sys).init(MyReplicatedStringSet.provider(replicationType))
        ReplicatedShardingExtension(sys).init(MyReplicatedIntSet.provider(replicationType))
      }
      start(system)
      start(system2)
    }

    "forward to replicas" in {
      val proxy: ActorRef[ProxyActor.Command] = spawn(ProxyActor(replicationType))

      proxy ! ProxyActor.ForwardToAllString("id1", MyReplicatedStringSet.Add("to-all"))
      proxy ! ProxyActor.ForwardToRandomString("id1", MyReplicatedStringSet.Add("to-random"))

      eventually {
        val probe = createTestProbe[MyReplicatedStringSet.Texts]()
        proxy ! ProxyActor.ForwardToAllString("id1", MyReplicatedStringSet.GetTexts(probe.ref))
        val responses: Seq[MyReplicatedStringSet.Texts] = probe.receiveMessages(2)
        val uniqueTexts = responses.flatMap(res => res.texts).toSet
        uniqueTexts should ===(Set("to-all", "to-random"))
      }

      proxy ! ProxyActor.ForwardToAllInt("id1", MyReplicatedIntSet.Add(10))
      proxy ! ProxyActor.ForwardToRandomInt("id1", MyReplicatedIntSet.Add(11))

      eventually {
        val probe = createTestProbe[MyReplicatedIntSet.Ints]()
        proxy ! ProxyActor.ForwardToAllInt("id1", MyReplicatedIntSet.GetInts(probe.ref))
        val responses: Seq[MyReplicatedIntSet.Ints] = probe.receiveMessages(2)
        val uniqueTexts = responses.flatMap(res => res.ints).toSet
        uniqueTexts should ===(Set(10, 11))
      }

      // This is also done in 'afterAll', but we do it here as well so we can see the
      // logging to diagnose
      // https://github.com/akka/akka-core/issues/30501 and
      // https://github.com/akka/akka-core/issues/30502
      ActorTestKit.shutdown(system2)
    }
  }

}
