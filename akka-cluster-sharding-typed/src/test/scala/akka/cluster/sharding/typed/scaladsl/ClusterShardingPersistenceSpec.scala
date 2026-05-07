/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.scaladsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.{ ClusterSharding => ClassicClusterSharding }
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState
import akka.cluster.sharding.ShardRegion.GetShardRegionState
import akka.cluster.sharding.typed.scaladsl.ClusterSharding.Passivate
import akka.cluster.sharding.typed.scaladsl.ClusterSharding.ShardCommand
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

object ClusterShardingPersistenceSpec {
  val config = ConfigFactory.parseString("""
      akka.loglevel = INFO
      #akka.persistence.typed.log-stashing = on

      akka.actor.provider = cluster

      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1

      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.journal.inmem.test-serialization = on
    """)

  sealed trait Command
  final case class Add(s: String) extends Command
  final case class AddWithConfirmation(s: String)(val replyTo: ActorRef[Done]) extends Command
  final case class PassivateAndPersist(s: String)(val replyTo: ActorRef[Done]) extends Command
  final case class Get(replyTo: ActorRef[String]) extends Command
  final case class Echo(msg: String, replyTo: ActorRef[String]) extends Command
  final case class Block(latch: CountDownLatch) extends Command
  case object BeginStashingAddCommands extends Command
  case object UnstashAll extends Command
  case object UnstashAllAndPassivate extends Command

  val TypeKey = EntityTypeKey[Command]("test")

  val lifecycleProbes = new ConcurrentHashMap[String, ActorRef[String]]

  // Need this to be able to send the PoisonPill from the outside, simulating rebalance before recovery and such.
  // Promise completed by the actor when it's started.
  val entityActorRefs = new ConcurrentHashMap[String, Promise[ActorRef[Any]]]

  def persistentEntity(entityId: String, shard: ActorRef[ShardCommand]): Behavior[Command] = {
    Behaviors.setup { ctx =>
      entityActorRefs.get(entityId) match {
        case null    =>
        case promise => promise.trySuccess(ctx.self.unsafeUpcast)
      }

      // transient state (testing purpose)
      var stashing = false

      EventSourcedBehavior[Command, String, String](
        PersistenceId(TypeKey.name, entityId),
        emptyState = "",
        commandHandler = (state, cmd) =>
          cmd match {
            case Add(s) =>
              if (stashing)
                Effect.stash()
              else
                Effect.persist(s)

            case cmd @ AddWithConfirmation(s) =>
              if (stashing)
                Effect.stash()
              else
                Effect.persist(s).thenReply(cmd.replyTo)(_ => Done)

            case Get(replyTo) =>
              replyTo ! s"$entityId:$state"
              Effect.none

            case cmd @ PassivateAndPersist(s) =>
              shard ! Passivate(ctx.self)
              Effect.persist(s).thenReply(cmd.replyTo)(_ => Done)

            case Echo(msg, replyTo) =>
              Effect.none.thenRun(_ => replyTo ! msg)

            case Block(latch) =>
              latch.await(5, TimeUnit.SECONDS)
              Effect.none

            case BeginStashingAddCommands =>
              stashing = true
              Effect.none

            case UnstashAll =>
              stashing = false
              Effect.unstashAll()

            case UnstashAllAndPassivate =>
              stashing = false
              shard ! Passivate(ctx.self)
              Effect.unstashAll()
          },
        eventHandler = (state, evt) => if (state.isEmpty) evt else state + "|" + evt).receiveSignal {
        case (state, RecoveryCompleted) =>
          ctx.log.debug("onRecoveryCompleted: [{}]", state)
          lifecycleProbes.get(entityId) match {
            case null => ctx.log.debug("no lifecycleProbe (onRecoveryCompleted) for [{}]", entityId)
            case p    => p ! s"recoveryCompleted:$state"
          }
        case (_, PostStop) =>
          lifecycleProbes.get(entityId) match {
            case null => ctx.log.debug("no lifecycleProbe (postStop) for [{}]", entityId)
            case p    => p ! "stopped"
          }
      }
    }
  }

}

class ClusterShardingPersistenceSpec
    extends ScalaTestWithActorTestKit(ClusterShardingPersistenceSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ClusterShardingPersistenceSpec._

  private var _entityId = 0
  def nextEntityId(): String = {
    _entityId += 1
    _entityId.toString
  }

  private def awaitEntityTerminatedAndRemoved(ref: ActorRef[_], entityId: String): Unit = {
    val p = TestProbe[Any]()
    p.expectTerminated(ref, p.remainingOrDefault)

    // also make sure that the entity is removed from the Shard before continuing
    // FIXME #24466: rewrite this with Typed API when region queries are supported
    import akka.actor.typed.scaladsl.adapter._
    val regionStateProbe = TestProbe[CurrentShardRegionState]()
    val classicRegion = ClassicClusterSharding(system.toClassic)
    regionStateProbe.awaitAssert {
      classicRegion.shardRegion(TypeKey.name).tell(GetShardRegionState, regionStateProbe.ref.toClassic)
      regionStateProbe.receiveMessage().shards.foreach { shardState =>
        shardState.entityIds should not contain entityId
      }
    }
  }

  "Typed cluster sharding with persistent actor" must {

    ClusterSharding(system).init(Entity(TypeKey)(ctx => persistentEntity(ctx.entityId, ctx.shard)))

    Cluster(system).manager ! Join(Cluster(system).selfMember.address)

    "start persistent actor" in {
      val entityId = nextEntityId()
      val p = TestProbe[String]()

      val ref = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      ref ! Add("a")
      ref ! Add("b")
      ref ! Add("c")
      ref ! Get(p.ref)
      p.expectMessage("1:a|b|c")
    }

    "support ask with thenReply" in {
      val entityId = nextEntityId()
      val p = TestProbe[String]()

      val ref = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      val done1 = ref ? AddWithConfirmation("a")
      done1.futureValue should ===(Done)

      val done2: Future[Done] = ref ? AddWithConfirmation("b")
      done2.futureValue should ===(Done)

      ref ! Get(p.ref)
      p.expectMessage("2:a|b")
    }

    "handle PoisonPill after persist effect" in {
      val entityId = nextEntityId()
      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val p1 = TestProbe[Done]()
      val ref = ClusterSharding(system).entityRefFor(TypeKey, entityId)

      (1 to 10).foreach { n =>
        ref ! PassivateAndPersist(n.toString)(p1.ref)
        lifecycleProbe.expectMessage(max = 10.seconds, "recoveryCompleted:" + (1 until n).map(_.toString).mkString("|"))
        p1.expectMessage(Done)
        lifecycleProbe.expectMessage("stopped")
      }

      val p2 = TestProbe[String]()
      ref ! Get(p2.ref)
      p2.expectMessage(entityId + ":" + (1 to 10).map(_.toString).mkString("|"))
    }

    "handle PoisonPill after several stashed commands that persist" in {
      val entityId = nextEntityId()
      val actorRefPromise = Promise[ActorRef[Any]]()
      entityActorRefs.put(entityId, actorRefPromise)

      val addProbe = TestProbe[Done]()
      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val entityRef = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      // this will wakeup the entity, and complete the entityActorRefPromise
      entityRef ! AddWithConfirmation("a")(addProbe.ref)
      addProbe.expectMessage(Done)
      lifecycleProbe.expectMessage("recoveryCompleted:")
      // now we know that it's in EventSourcedRunning with no stashed commands

      val actorRef = actorRefPromise.future.futureValue

      // not sending via the EntityRef because that would make the test racy
      // these are stashed, since before the PoisonPill
      val latch = new CountDownLatch(1)
      actorRef ! Block(latch)
      actorRef ! AddWithConfirmation("b")(addProbe.ref)
      actorRef ! AddWithConfirmation("c")(addProbe.ref)

      actorRef ! PoisonPill

      // those messages should be ignored since they happen after the PoisonPill,
      actorRef ! AddWithConfirmation("d")(addProbe.ref)
      actorRef ! AddWithConfirmation("e")(addProbe.ref)

      // now we have enqueued the message sequence and start processing them
      latch.countDown()

      addProbe.expectMessage(Done)
      addProbe.expectMessage(Done)
      lifecycleProbe.expectMessage("stopped")

      // wake up again
      awaitEntityTerminatedAndRemoved(actorRef, entityId)
      val p2 = TestProbe[String]()
      entityRef ! AddWithConfirmation("f")(addProbe.ref)
      entityRef ! Get(p2.ref)
      lifecycleProbe.expectMessage("recoveryCompleted:a|b|c")
      p2.expectMessage(entityId + ":a|b|c|f")
    }

    "handle PoisonPill after several stashed commands that DON'T persist" in {
      val entityId = nextEntityId()
      val actorRefPromise = Promise[ActorRef[Any]]()
      entityActorRefs.put(entityId, actorRefPromise)

      val addProbe = TestProbe[Done]()
      val echoProbe = TestProbe[String]()
      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val entityRef = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      // this will wakeup the entity, and complete the entityActorRefPromise
      entityRef ! AddWithConfirmation("a")(addProbe.ref)
      addProbe.expectMessage(Done)
      lifecycleProbe.expectMessage("recoveryCompleted:")
      // now we know that it's in EventSourcedRunning with no stashed commands

      val actorRef = actorRefPromise.future.futureValue
      // not sending via the EntityRef because that would make the test racy
      // these are stashed, since before the PoisonPill
      val latch = new CountDownLatch(1)
      actorRef ! Block(latch)
      actorRef ! AddWithConfirmation("b")(addProbe.ref)
      actorRef ! AddWithConfirmation("c")(addProbe.ref)
      actorRef ! Echo("echo-1", echoProbe.ref)

      actorRef ! PoisonPill

      // those messages should be ignored since they happen after the PoisonPill,
      actorRef ! Echo("echo-2", echoProbe.ref)
      actorRef ! AddWithConfirmation("d")(addProbe.ref)
      actorRef ! AddWithConfirmation("e")(addProbe.ref)
      actorRef ! Echo("echo-3", echoProbe.ref)

      // now we have enqueued the message sequence and start processing them
      latch.countDown()

      echoProbe.expectMessage("echo-1")
      addProbe.expectMessage(Done)
      addProbe.expectMessage(Done)
      lifecycleProbe.expectMessage("stopped")

      // wake up again
      awaitEntityTerminatedAndRemoved(actorRef, entityId)
      val p2 = TestProbe[String]()
      entityRef ! Echo("echo-4", echoProbe.ref)
      echoProbe.expectMessage("echo-4")
      entityRef ! AddWithConfirmation("f")(addProbe.ref)
      entityRef ! Get(p2.ref)
      lifecycleProbe.expectMessage("recoveryCompleted:a|b|c")
      p2.expectMessage(entityId + ":a|b|c|f")
    }

    "handle PoisonPill when stash empty" in {
      val entityId = nextEntityId()
      val actorRefPromise = Promise[ActorRef[Any]]()
      entityActorRefs.put(entityId, actorRefPromise)

      val addProbe = TestProbe[Done]()
      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val entityRef = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      // this will wakeup the entity, and complete the entityActorRefPromise
      entityRef ! AddWithConfirmation("a")(addProbe.ref)
      addProbe.expectMessage(Done)
      lifecycleProbe.expectMessage("recoveryCompleted:")
      // now we know that it's in EventSourcedRunning with no stashed commands

      val actorRef = actorRefPromise.future.futureValue
      // not sending via the EntityRef because that would make the test racy
      val latch = new CountDownLatch(1)
      actorRef ! Block(latch)
      actorRef ! PoisonPill
      // those messages should be ignored since they happen after the PoisonPill,
      actorRef ! AddWithConfirmation("b")(addProbe.ref)

      // now we have enqueued the message sequence and start processing them
      latch.countDown()

      lifecycleProbe.expectMessage("stopped")

      // wake up again
      awaitEntityTerminatedAndRemoved(actorRef, entityId)
      val p2 = TestProbe[String]()
      entityRef ! AddWithConfirmation("c")(addProbe.ref)
      entityRef ! Get(p2.ref)
      lifecycleProbe.expectMessage("recoveryCompleted:a")
      p2.expectMessage(entityId + ":a|c")
    }

    "handle PoisonPill before recovery completed without stashed commands" in {
      val entityId = nextEntityId()
      val actorRefPromise = Promise[ActorRef[Any]]()
      entityActorRefs.put(entityId, actorRefPromise)

      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val entityRef = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      val ignoreFirstEchoProbe = TestProbe[String]()
      val echoProbe = TestProbe[String]()
      // first echo will wakeup the entity, and complete the entityActorRefPromise
      // ignore the first echo reply since it may be racy with the PoisonPill
      entityRef ! Echo("echo-1", ignoreFirstEchoProbe.ref)

      // not using actorRefPromise.future.futureValue because it's polling (slow) and want to run this before
      // recovery completed, to exercise that scenario
      implicit val ec: ExecutionContext = testKit.system.executionContext
      val poisonSent = actorRefPromise.future.map { actorRef =>
        // not sending via the EntityRef because that would make the test racy
        actorRef ! PoisonPill
        actorRef
      }
      val actorRef = poisonSent.futureValue

      lifecycleProbe.expectMessage("recoveryCompleted:")
      lifecycleProbe.expectMessage("stopped")

      // wake up again
      awaitEntityTerminatedAndRemoved(actorRef, entityId)
      entityRef ! Echo("echo-2", echoProbe.ref)
      echoProbe.expectMessage("echo-2")
      lifecycleProbe.expectMessage("recoveryCompleted:")
    }

    "handle PoisonPill before recovery completed with stashed commands" in {
      val entityId = nextEntityId()
      val actorRefPromise = Promise[ActorRef[Any]]()
      entityActorRefs.put(entityId, actorRefPromise)

      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val entityRef = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      val addProbe = TestProbe[Done]()
      val ignoreFirstEchoProbe = TestProbe[String]()
      val echoProbe = TestProbe[String]()
      // first echo will wakeup the entity, and complete the entityActorRefPromise
      // ignore the first echo reply since it may be racy with the PoisonPill
      entityRef ! Echo("echo-1", ignoreFirstEchoProbe.ref)

      // not using actorRefPromise.future.futureValue because it's polling (slow) and want to run this before
      // recovery completed, to exercise that scenario
      implicit val ec: ExecutionContext = testKit.system.executionContext
      val poisonSent = actorRefPromise.future.map { actorRef =>
        // not sending via the EntityRef because that would make the test racy
        // these are stashed, since before the PoisonPill
        actorRef ! Echo("echo-2", echoProbe.ref)
        actorRef ! AddWithConfirmation("a")(addProbe.ref)
        actorRef ! AddWithConfirmation("b")(addProbe.ref)
        actorRef ! Echo("echo-3", echoProbe.ref)

        actorRef ! PoisonPill

        // those messages should be ignored since they happen after the PoisonPill,
        actorRef ! Echo("echo-4", echoProbe.ref)
        actorRef ! AddWithConfirmation("c")(addProbe.ref)
        actorRef
      }
      val actorRef = poisonSent.futureValue

      lifecycleProbe.expectMessage("recoveryCompleted:")
      echoProbe.expectMessage("echo-2")
      echoProbe.expectMessage("echo-3")
      addProbe.expectMessage(Done)
      addProbe.expectMessage(Done)
      lifecycleProbe.expectMessage("stopped")

      // wake up again
      awaitEntityTerminatedAndRemoved(actorRef, entityId)
      entityRef ! Echo("echo-5", echoProbe.ref)
      echoProbe.expectMessage("echo-5")
      lifecycleProbe.expectMessage("recoveryCompleted:a|b")
    }

    "handle PoisonPill before UnstashAll from user stash" in {
      val entityId = nextEntityId()

      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val p1 = TestProbe[Done]()
      val ref = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      ref ! Add("1")
      ref ! Add("2")
      ref ! BeginStashingAddCommands
      ref ! Add("3")
      ref ! Add("4")

      ref ! PassivateAndPersist("5")(p1.ref)
      p1.receiveMessage()
      lifecycleProbe.expectMessage("recoveryCompleted:")
      lifecycleProbe.expectMessage("stopped")

      ref ! Add("6")
      // user was stash discarded, i.e. 3 and 4 not handled
      ref ! UnstashAll

      val probe = TestProbe[String]()
      ref ! Get(probe.ref)
      probe.expectMessage(entityId + ":" + List(1, 2, 5, 6).map(_.toString).mkString("|"))
    }

    "handle PoisonPill after UnstashAll from user stash" in {
      val entityId = nextEntityId()
      val lifecycleProbe = TestProbe[String]()
      lifecycleProbes.put(entityId, lifecycleProbe.ref)

      val ref = ClusterSharding(system).entityRefFor(TypeKey, entityId)
      ref ! Add("1")
      lifecycleProbe.expectMessage(max = 10.seconds, "recoveryCompleted:")
      ref ! BeginStashingAddCommands
      ref ! Add("2")
      ref ! Add("3")
      ref ! Add("4")
      ref ! UnstashAllAndPassivate

      val probe = TestProbe[String]()
      val expected = (1 to 4).map(_.toString).mkString("|")
      lifecycleProbe.awaitAssert {
        ref ! Get(probe.ref)
        lifecycleProbe.expectMessage(max = 1.second, "recoveryCompleted:" + expected)
        probe.expectMessage(entityId + ":" + expected)
      }
    }

  }
}
