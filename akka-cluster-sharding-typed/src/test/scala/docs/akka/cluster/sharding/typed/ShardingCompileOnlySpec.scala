/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import scala.concurrent.duration._

import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.persistence.typed.PersistenceId
import scala.annotation.nowarn
import docs.akka.persistence.typed.BlogPostEntity
import docs.akka.persistence.typed.BlogPostEntity.Command

import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

@nowarn
object ShardingCompileOnlySpec {

  val system = ActorSystem(Behaviors.empty, "Sharding")

  object Basics {

    //#sharding-extension
    import akka.cluster.sharding.typed.ShardingEnvelope
    import akka.cluster.sharding.typed.scaladsl.ClusterSharding
    import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
    import akka.cluster.sharding.typed.scaladsl.EntityRef

    val sharding = ClusterSharding(system)
    //#sharding-extension

    //#counter
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class GetValue(replyTo: ActorRef[Int]) extends Command

      def apply(entityId: String): Behavior[Command] = {
        def updated(value: Int): Behavior[Command] = {
          Behaviors.receiveMessage[Command] {
            case Increment =>
              updated(value + 1)
            case GetValue(replyTo) =>
              replyTo ! value
              Behaviors.same
          }
        }

        updated(0)

      }
    }
    //#counter

    //#init
    val TypeKey = EntityTypeKey[Counter.Command]("Counter")

    val shardRegion: ActorRef[ShardingEnvelope[Counter.Command]] =
      sharding.init(Entity(TypeKey)(createBehavior = entityContext => Counter(entityContext.entityId)))
    //#init

    //#send
    // With an EntityRef
    val counterOne: EntityRef[Counter.Command] = sharding.entityRefFor(TypeKey, "counter-1")
    counterOne ! Counter.Increment

    // Entity id is specified via an `ShardingEnvelope`
    shardRegion ! ShardingEnvelope("counter-1", Counter.Increment)
    //#send

    //#persistence
    val BlogTypeKey = EntityTypeKey[Command]("BlogPost")

    ClusterSharding(system).init(Entity(BlogTypeKey) { entityContext =>
      BlogPostEntity(entityContext.entityId, PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId))
    })
    //#persistence

    //#roles
    sharding.init(
      Entity(TypeKey)(createBehavior = entityContext => Counter(entityContext.entityId)).withRole("backend"))
    //#roles

  }

  object CounterWithPassivate {
    import akka.cluster.sharding.typed.scaladsl.ClusterSharding
    import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

    //#counter-passivate
    object Counter {
      sealed trait Command
      case object Increment extends Command
      final case class GetValue(replyTo: ActorRef[Int]) extends Command
      private case object Idle extends Command
      case object GoodByeCounter extends Command

      def apply(shard: ActorRef[ClusterSharding.ShardCommand], entityId: String): Behavior[Command] = {
        Behaviors.setup { ctx =>
          def updated(value: Int): Behavior[Command] =
            Behaviors.receiveMessage[Command] {
              case Increment =>
                updated(value + 1)
              case GetValue(replyTo) =>
                replyTo ! value
                Behaviors.same
              case Idle =>
                // after receive timeout
                shard ! ClusterSharding.Passivate(ctx.self)
                Behaviors.same
              case GoodByeCounter =>
                // the stopMessage, used for rebalance and passivate
                Behaviors.stopped
            }

          ctx.setReceiveTimeout(30.seconds, Idle)
          updated(0)
        }
      }
    }
    //#counter-passivate

    //#counter-passivate-init
    val TypeKey = EntityTypeKey[Counter.Command]("Counter")

    ClusterSharding(system).init(Entity(TypeKey)(createBehavior = entityContext =>
      Counter(entityContext.shard, entityContext.entityId)).withStopMessage(Counter.GoodByeCounter))
    //#counter-passivate-init

  }

  object CounterWithResponseToShardedActor {

    import akka.cluster.sharding.typed.scaladsl.ClusterSharding
    import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

    //#sharded-response
    // a sharded actor that needs counter updates
    object CounterConsumer {
      sealed trait Command
      final case class NewCount(count: Long) extends Command
      val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("example-sharded-response")
    }

    // a sharded counter that sends responses to another sharded actor
    object Counter {
      trait Command
      case object Increment extends Command
      final case class GetValue(replyToEntityId: String) extends Command
      val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("example-sharded-counter")

      private def apply(): Behavior[Command] =
        Behaviors.setup { context =>
          counter(ClusterSharding(context.system), 0)
        }

      private def counter(sharding: ClusterSharding, value: Long): Behavior[Command] =
        Behaviors.receiveMessage {
          case Increment =>
            counter(sharding, value + 1)
          case GetValue(replyToEntityId) =>
            val replyToEntityRef = sharding.entityRefFor(CounterConsumer.TypeKey, replyToEntityId)
            replyToEntityRef ! CounterConsumer.NewCount(value)
            Behaviors.same
        }

    }
    //#sharded-response
  }

  object ShardRegionStateQuery {

    object Counter {
      val TypeKey = EntityTypeKey[Basics.Counter.Command]("Counter")
    }

    val replyMessageAdapter: ActorRef[akka.cluster.sharding.ShardRegion.CurrentShardRegionState] = ???

    //#get-shard-region-state
    import akka.cluster.sharding.typed.GetShardRegionState
    import akka.cluster.sharding.ShardRegion.CurrentShardRegionState

    val replyTo: ActorRef[CurrentShardRegionState] = replyMessageAdapter

    ClusterSharding(system).shardState ! GetShardRegionState(Counter.TypeKey, replyTo)
    //#get-shard-region-state
  }

  object ClusterShardingStatsQuery {

    object Counter {
      val TypeKey = EntityTypeKey[Basics.Counter.Command]("Counter")
    }

    val replyMessageAdapter: ActorRef[akka.cluster.sharding.ShardRegion.ClusterShardingStats] = ???

    //#get-cluster-sharding-stats
    import akka.cluster.sharding.typed.GetClusterShardingStats
    import akka.cluster.sharding.ShardRegion.ClusterShardingStats
    import scala.concurrent.duration._

    val replyTo: ActorRef[ClusterShardingStats] = replyMessageAdapter
    val timeout: FiniteDuration = 5.seconds

    ClusterSharding(system).shardState ! GetClusterShardingStats(Counter.TypeKey, timeout, replyTo)
    //#get-cluster-sharding-stats
  }

}
