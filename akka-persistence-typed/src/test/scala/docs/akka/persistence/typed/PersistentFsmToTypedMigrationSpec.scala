/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://akka.io>
 */

package docs.akka.persistence.typed

import java.util.UUID
import akka.actor.PoisonPill
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.fsm.PersistentFSM.StateChangeEvent
import akka.persistence.fsm.PersistentFSMSpec.CustomerInactive
import akka.persistence.fsm.PersistentFSMSpec.DomainEvent
import akka.persistence.fsm.PersistentFSMSpec.EmptyShoppingCart
import akka.persistence.fsm.PersistentFSMSpec.Item
import akka.persistence.fsm.PersistentFSMSpec.ItemAdded
import akka.persistence.fsm.PersistentFSMSpec.OrderDiscarded
import akka.persistence.fsm.PersistentFSMSpec.OrderExecuted
import akka.persistence.fsm.PersistentFSMSpec.ShoppingCart
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.PersistentFSMMigration
import akka.persistence.typed.EventAdapter
import akka.persistence.typed.EventSeq
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.RecoveryCompleted
import akka.persistence.typed.SnapshotAdapter
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.LogCapturing

import scala.annotation.nowarn

object PersistentFsmToTypedMigrationSpec {
  // cannot be moved to testkit journals as it requires sharing journal content across actor system instances
  val config = ConfigFactory.parseString(s"""
    akka.actor.allow-java-serialization = on
    akka.persistence.journal.leveldb.dir = "target/typed-persistence-${UUID.randomUUID().toString}"
    akka.persistence.journal.plugin = "akka.persistence.journal.leveldb"
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/typed-persistence-${UUID.randomUUID().toString}"
    """)

}

object ShoppingCartBehavior {

  def apply(pid: PersistenceId) = behavior(pid)

  //#commands
  sealed trait Command
  case class AddItem(item: Item) extends Command
  case object Buy extends Command
  case object Leave extends Command
  case class GetCurrentCart(replyTo: ActorRef[ShoppingCart]) extends Command
  private case object Timeout extends Command
  //#commands

  //#state
  sealed trait State
  case class LookingAround(cart: ShoppingCart) extends State
  case class Shopping(cart: ShoppingCart) extends State
  case class Inactive(cart: ShoppingCart) extends State
  case class Paid(cart: ShoppingCart) extends State
  //#state

  //#snapshot-adapter
  val persistentFSMSnapshotAdapter: SnapshotAdapter[State] = PersistentFSMMigration.snapshotAdapter[State] {
    case (stateIdentifier, data, _) =>
      val cart = data.asInstanceOf[ShoppingCart]
      stateIdentifier match {
        case "Looking Around" => LookingAround(cart)
        case "Shopping"       => Shopping(cart)
        case "Inactive"       => Inactive(cart)
        case "Paid"           => Paid(cart)
        case id               => throw new IllegalStateException(s"Unexpected state identifier $id")
      }
  }
  //#snapshot-adapter

  //#event-adapter
  class PersistentFsmEventAdapter extends EventAdapter[DomainEvent, Any] {
    override def toJournal(e: DomainEvent): Any = e
    override def manifest(event: DomainEvent): String = ""
    @nowarn("msg=deprecated")
    override def fromJournal(journalEvent: Any, manifest: String): EventSeq[DomainEvent] = {
      journalEvent match {
        case _: StateChangeEvent =>
          // In this example the state transitions can be inferred from the events
          // Alternatively the StateChangeEvent can be converted to a private event if either the StateChangeEvent.stateIdentifier
          // or StateChangeEvent.timeout is required
          // Many use cases have the same timeout so it can be hard coded, otherwise it cane be stored in the state
          EventSeq.empty
        case other =>
          // If using a new domain event model the conversion would happen here
          EventSeq.single(other.asInstanceOf[DomainEvent])
      }

    }
  }
  //#event-adapter

  val StateTimeout = "state-timeout"

  //#command-handler
  def commandHandler(timers: TimerScheduler[Command])(state: State, command: Command): Effect[DomainEvent, State] =
    state match {
      case LookingAround(cart) =>
        command match {
          case AddItem(item) =>
            Effect.persist(ItemAdded(item)).thenRun(_ => timers.startSingleTimer(StateTimeout, Timeout, 1.second))
          case GetCurrentCart(replyTo) =>
            replyTo ! cart
            Effect.none
          case _ =>
            Effect.none
        }
      case Shopping(cart) =>
        command match {
          case AddItem(item) =>
            Effect.persist(ItemAdded(item)).thenRun(_ => timers.startSingleTimer(StateTimeout, Timeout, 1.second))
          case Buy =>
            Effect.persist(OrderExecuted).thenRun(_ => timers.cancel(StateTimeout))
          case Leave =>
            Effect.persist(OrderDiscarded).thenStop()
          case GetCurrentCart(replyTo) =>
            replyTo ! cart
            Effect.none
          case Timeout =>
            Effect.persist(CustomerInactive)
        }
      case Inactive(_) =>
        command match {
          case AddItem(item) =>
            Effect.persist(ItemAdded(item)).thenRun(_ => timers.startSingleTimer(StateTimeout, Timeout, 1.second))
          case Timeout =>
            Effect.persist(OrderDiscarded)
          case _ =>
            Effect.none
        }
      case Paid(cart) =>
        command match {
          case Leave =>
            Effect.stop()
          case GetCurrentCart(replyTo) =>
            replyTo ! cart
            Effect.none
          case _ =>
            Effect.none
        }
    }
  //#command-handler

  //#event-handler
  def eventHandler(state: State, event: DomainEvent): State = {
    state match {
      case la @ LookingAround(cart) =>
        event match {
          case ItemAdded(item) => Shopping(cart.addItem(item))
          case _               => la
        }
      case Shopping(cart) =>
        event match {
          case ItemAdded(item)  => Shopping(cart.addItem(item))
          case OrderExecuted    => Paid(cart)
          case OrderDiscarded   => state // will be stopped
          case CustomerInactive => Inactive(cart)
        }
      case i @ Inactive(cart) =>
        event match {
          case ItemAdded(item) => Shopping(cart.addItem(item))
          case OrderDiscarded  => i // will be stopped
          case _               => i
        }
      case Paid(_) => state // no events after paid
    }
  }
  //#event-handler

  private def behavior(pid: PersistenceId): Behavior[Command] =
    Behaviors.withTimers[Command] { timers =>
      EventSourcedBehavior[Command, DomainEvent, State](
        pid,
        LookingAround(EmptyShoppingCart),
        commandHandler(timers),
        eventHandler)
        .snapshotAdapter(persistentFSMSnapshotAdapter)
        .eventAdapter(new PersistentFsmEventAdapter())
        //#signal-handler
        .receiveSignal {
          case (state, RecoveryCompleted) =>
            state match {
              case _: Shopping | _: Inactive =>
                timers.startSingleTimer(StateTimeout, Timeout, 1.second)
              case _ =>
            }
        }
    //#signal-handler
    }

}

class PersistentFsmToTypedMigrationSpec extends AnyWordSpec with ScalaFutures with LogCapturing {

  import akka.persistence.fsm.PersistentFSMSpec._

  "PersistentFSM migration to Persistence Typed" must {
    "work when snapshot is not current" in {
      val classicActorSystem = akka.actor.ActorSystem("ClassicSystem", PersistentFsmToTypedMigrationSpec.config)
      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)
      val pid = "no-snapshot"
      try {
        import akka.testkit.TestProbe
        val reportActorProbe = TestProbe()(classicActorSystem)
        val classicProbe = TestProbe()(classicActorSystem)
        implicit val classicRef = classicProbe.ref
        val fsmRef = classicActorSystem.actorOf(WebStoreCustomerFSM.props(pid, reportActorProbe.ref))
        fsmRef ! AddItem(shirt)
        fsmRef ! AddItem(shoes)
        fsmRef.tell(GetCurrentCart, classicProbe.ref)
        classicProbe.expectMsg(NonEmptyShoppingCart(List(shirt, shoes)))

        classicProbe.watch(fsmRef)
        fsmRef ! PoisonPill
        classicProbe.expectTerminated(fsmRef)
      } finally {
        classicActorSystem.terminate().futureValue
      }

      val typedTestKit = ActorTestKit("System", PersistentFsmToTypedMigrationSpec.config)
      try {
        import typedTestKit._
        val typedProbe = akka.actor.testkit.typed.scaladsl.TestProbe[ShoppingCart]()
        val typedReplacement = spawn(ShoppingCartBehavior(PersistenceId.ofUniqueId(pid)))
        typedReplacement ! ShoppingCartBehavior.AddItem(coat)
        typedReplacement ! ShoppingCartBehavior.GetCurrentCart(typedProbe.ref)
        typedProbe.expectMessage(NonEmptyShoppingCart(List(shirt, shoes, coat)))
        typedReplacement ! ShoppingCartBehavior.Buy
        typedReplacement ! ShoppingCartBehavior.Leave
        typedProbe.expectTerminated(typedReplacement)
      } finally {
        typedTestKit.shutdownTestKit()
      }

    }

    "work if snapshot is current" in {
      val classicActorSystem = akka.actor.ActorSystem("CLassicSystem", PersistentFsmToTypedMigrationSpec.config)
      val shirt = Item("1", "Shirt", 59.99f)
      val pid = "current-shapshot"
      try {
        import akka.testkit.TestProbe
        val reportActorProbe = TestProbe()(classicActorSystem)
        val classicProbe = TestProbe()(classicActorSystem)
        implicit val classicRef = classicProbe.ref
        val fsmRef = classicActorSystem.actorOf(WebStoreCustomerFSM.props(pid, reportActorProbe.ref))
        classicProbe.watch(fsmRef)
        fsmRef ! AddItem(shirt)
        fsmRef.tell(GetCurrentCart, classicProbe.ref)
        classicProbe.expectMsg(NonEmptyShoppingCart(Seq(shirt)))
        fsmRef ! Buy
        fsmRef.tell(GetCurrentCart, classicProbe.ref)
        classicProbe.expectMsg(NonEmptyShoppingCart(Seq(shirt)))
      } finally {
        classicActorSystem.terminate().futureValue
      }

      val typedTestKit = ActorTestKit("TypedSystem", PersistentFsmToTypedMigrationSpec.config)
      try {
        import typedTestKit._
        val typedProbe = akka.actor.testkit.typed.scaladsl.TestProbe[ShoppingCart]()
        val typedReplacement = spawn(ShoppingCartBehavior(PersistenceId.ofUniqueId(pid)))
        typedReplacement ! ShoppingCartBehavior.GetCurrentCart(typedProbe.ref)
        typedProbe.expectMessage(NonEmptyShoppingCart(Seq(shirt)))
      } finally {
        typedTestKit.shutdownTestKit()
      }
    }
  }

}
