/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.fsm

import java.io.File

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.commons.io.FileUtils

import akka.actor.{ ActorSystem, _ }
import akka.persistence._
import akka.persistence.fsm.PersistentFSM._
import akka.testkit._

@nowarn("msg=deprecated|Unused import")
abstract class PersistentFSMSpec(config: Config) extends PersistenceSpec(config) with ImplicitSender {
  import PersistentFSMSpec._

  //Dummy report actor, for tests that don't need it
  val dummyReportActorRef = TestProbe().ref

  "PersistentFSM" must {
    "function as a regular FSM " in {
      val persistenceId = name
      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))

      watch(fsmRef)
      fsmRef ! SubscribeTransitionCallBack(testActor)

      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)

      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shirt)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shoes)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(coat)
      fsmRef ! GetCurrentCart
      fsmRef ! Buy
      fsmRef ! GetCurrentCart
      fsmRef ! Leave

      expectMsg(CurrentState(fsmRef, LookingAround, None))
      expectMsg(EmptyShoppingCart)

      expectMsg(Transition(fsmRef, LookingAround, Shopping, Some(1 second)))
      expectMsg(NonEmptyShoppingCart(List(shirt)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      expectMsg(Transition(fsmRef, Shopping, Paid, None))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      expectTerminated(fsmRef)
    }

    "function as a regular FSM on state timeout" taggedAs TimingTest in {
      val persistenceId = name
      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))

      watch(fsmRef)
      fsmRef ! SubscribeTransitionCallBack(testActor)

      val shirt = Item("1", "Shirt", 59.99f)

      // this isn't quite when the state transition happens, but close enough
      val before = System.nanoTime
      fsmRef ! AddItem(shirt)

      expectMsg(CurrentState(fsmRef, LookingAround, None))

      expectMsg(Transition(fsmRef, LookingAround, Shopping, Some(1 second)))

      val adjustedMin = 1.second - (System.nanoTime - before).nanos
      within(min = adjustedMin, max = remainingOrDefault) {
        expectMsg(Transition(fsmRef, Shopping, Inactive, Some(2 seconds)))
      }

      expectTerminated(fsmRef)
    }

    "recover successfully with correct state data" in {
      val persistenceId = name

      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      watch(fsmRef)
      fsmRef ! SubscribeTransitionCallBack(testActor)

      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)

      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shirt)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shoes)
      fsmRef ! GetCurrentCart

      expectMsg(CurrentState(fsmRef, LookingAround, None))
      expectMsg(EmptyShoppingCart)

      expectMsg(Transition(fsmRef, LookingAround, Shopping, Some(1 second)))
      expectMsg(NonEmptyShoppingCart(List(shirt)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes)))

      fsmRef ! PoisonPill
      expectTerminated(fsmRef)

      val recoveredFsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      watch(recoveredFsmRef)
      recoveredFsmRef ! SubscribeTransitionCallBack(testActor)

      recoveredFsmRef ! GetCurrentCart

      recoveredFsmRef ! AddItem(coat)
      recoveredFsmRef ! GetCurrentCart

      recoveredFsmRef ! Buy
      recoveredFsmRef ! GetCurrentCart
      recoveredFsmRef ! Leave

      expectMsg(CurrentState(recoveredFsmRef, Shopping, Some(1 second)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes)))

      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      expectMsg(Transition(recoveredFsmRef, Shopping, Paid, None))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      expectTerminated(recoveredFsmRef)
    }

    "execute the defined actions following successful persistence of state change" in {
      val persistenceId = name

      val reportActorProbe = TestProbe()
      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, reportActorProbe.ref))
      watch(fsmRef)
      fsmRef ! SubscribeTransitionCallBack(testActor)

      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)

      fsmRef ! AddItem(shirt)
      fsmRef ! AddItem(shoes)
      fsmRef ! AddItem(coat)
      fsmRef ! Buy
      fsmRef ! Leave

      expectMsg(CurrentState(fsmRef, LookingAround, None))
      expectMsg(Transition(fsmRef, LookingAround, Shopping, Some(1 second)))
      expectMsg(Transition(fsmRef, Shopping, Paid, None))
      reportActorProbe.expectMsg(PurchaseWasMade(List(shirt, shoes, coat)))
      expectTerminated(fsmRef)
    }

    "execute the defined actions following successful persistence of FSM stop" in {
      val persistenceId = name

      val reportActorProbe = TestProbe()
      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, reportActorProbe.ref))
      watch(fsmRef)
      fsmRef ! SubscribeTransitionCallBack(testActor)

      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)

      fsmRef ! AddItem(shirt)
      fsmRef ! AddItem(shoes)
      fsmRef ! AddItem(coat)
      fsmRef ! Leave

      expectMsg(CurrentState(fsmRef, LookingAround, None))
      expectMsg(Transition(fsmRef, LookingAround, Shopping, Some(1 second)))
      reportActorProbe.expectMsg(ShoppingCardDiscarded)
      expectTerminated(fsmRef)
    }

    "recover successfully with correct state timeout" taggedAs TimingTest in {
      val persistenceId = name
      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))

      watch(fsmRef)
      fsmRef ! SubscribeTransitionCallBack(testActor)

      val shirt = Item("1", "Shirt", 59.99f)

      fsmRef ! AddItem(shirt)

      expectMsg(CurrentState(fsmRef, LookingAround, None))
      expectMsg(Transition(fsmRef, LookingAround, Shopping, Some(1 second)))

      expectNoMessage(0.6 seconds) // arbitrarily chosen delay, less than the timeout, before stopping the FSM
      fsmRef ! PoisonPill
      expectTerminated(fsmRef)

      var recoveredFsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      watch(recoveredFsmRef)
      recoveredFsmRef ! SubscribeTransitionCallBack(testActor)

      // this isn't when it got into that state, but close enough
      val before = System.nanoTime
      expectMsg(CurrentState(recoveredFsmRef, Shopping, Some(1 second)))

      val adjustedMin = 1.second - (System.nanoTime - before).nanos
      within(min = adjustedMin, max = remainingOrDefault) {
        expectMsg(Transition(recoveredFsmRef, Shopping, Inactive, Some(2 seconds)))
      }

      expectNoMessage(0.6 seconds) // arbitrarily chosen delay, less than the timeout, before stopping the FSM
      recoveredFsmRef ! PoisonPill
      expectTerminated(recoveredFsmRef)

      recoveredFsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      watch(recoveredFsmRef)
      recoveredFsmRef ! SubscribeTransitionCallBack(testActor)

      expectMsg(CurrentState(recoveredFsmRef, Inactive, Some(2 seconds)))
      expectTerminated(recoveredFsmRef)
    }

    "not trigger onTransition for stay()" taggedAs TimingTest in {
      val persistenceId = name
      val probe = TestProbe()
      val fsmRef = system.actorOf(SimpleTransitionFSM.props(persistenceId, probe.ref))

      probe.expectMsg(3.seconds, "LookingAround -> LookingAround") // caused by initialize(), OK

      fsmRef ! "goto(the same state)" // causes goto()
      probe.expectMsg(3.seconds, "LookingAround -> LookingAround")

      fsmRef ! "stay" // causes stay()
      probe.expectNoMessage(3.seconds)
    }

    "not persist state change event when staying in the same state" in {
      val persistenceId = name

      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      watch(fsmRef)

      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)

      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shirt)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shoes)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(coat)
      fsmRef ! GetCurrentCart

      expectMsg(EmptyShoppingCart)

      expectMsg(NonEmptyShoppingCart(List(shirt)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      fsmRef ! PoisonPill
      expectTerminated(fsmRef)

      val persistentEventsStreamer = system.actorOf(PersistentEventsStreamer.props(persistenceId, testActor))

      expectMsg(ItemAdded(Item("1", "Shirt", 59.99f)))
      expectMsgType[StateChangeEvent] //because a timeout is defined, State Change is persisted

      expectMsg(ItemAdded(Item("2", "Shoes", 89.99f)))
      expectMsgType[StateChangeEvent] //because a timeout is defined, State Change is persisted

      expectMsg(ItemAdded(Item("3", "Coat", 119.99f)))
      expectMsgType[StateChangeEvent] //because a timeout is defined, State Change is persisted

      watch(persistentEventsStreamer)
      persistentEventsStreamer ! PoisonPill
      expectTerminated(persistentEventsStreamer)
    }

    "can extract state name" in {
      StateChangeEvent("xxx", None) match {
        case StateChangeEvent(name, _) => name should equal("xxx")
      }
    }

    "persist snapshot" in {
      val persistenceId = name

      val fsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      watch(fsmRef)

      val shirt = Item("1", "Shirt", 59.99f)
      val shoes = Item("2", "Shoes", 89.99f)
      val coat = Item("3", "Coat", 119.99f)

      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shirt)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(shoes)
      fsmRef ! GetCurrentCart
      fsmRef ! AddItem(coat)
      fsmRef ! GetCurrentCart
      fsmRef ! Buy
      fsmRef ! GetCurrentCart

      expectMsg(EmptyShoppingCart)

      expectMsg(NonEmptyShoppingCart(List(shirt)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes)))
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))
      expectNoMessage(1 second)

      fsmRef ! PoisonPill
      expectTerminated(fsmRef)

      //Check that PersistentFSM recovers in the correct state
      val recoveredFsmRef = system.actorOf(WebStoreCustomerFSM.props(persistenceId, dummyReportActorRef))
      recoveredFsmRef ! GetCurrentCart
      expectMsg(NonEmptyShoppingCart(List(shirt, shoes, coat)))

      watch(recoveredFsmRef)
      recoveredFsmRef ! PoisonPill
      expectTerminated(recoveredFsmRef)

      //Check that PersistentFSM uses snapshot during recovery
      val persistentEventsStreamer = system.actorOf(PersistentEventsStreamer.props(persistenceId, testActor))

      expectMsgPF() {
        case SnapshotOffer(SnapshotMetadata(_, _, timestamp), PersistentFSMSnapshot(stateIdentifier, cart, None)) =>
          stateIdentifier should ===(Paid.identifier)
          import org.scalatest.matchers.should.Matchers.unconstrainedEquality
          cart should ===(NonEmptyShoppingCart(List(shirt, shoes, coat)))
          timestamp should be > 0L
      }

      watch(persistentEventsStreamer)
      persistentEventsStreamer ! PoisonPill
      expectTerminated(persistentEventsStreamer)
    }

    "allow cancelling stateTimeout by issuing forMax(Duration.Inf)" in {
      val p = TestProbe()(system)

      val fsm = system.actorOf(TimeoutFSM.props(p.ref))

      p.expectMsg(PersistentFSM.StateTimeout)

      fsm ! TimeoutFSM.OverrideTimeoutToInf
      p.expectMsg(TimeoutFSM.OverrideTimeoutToInf)
      p.expectNoMessage(1.seconds)

    }

    "save periodical snapshots if akka.persistence.fsm.enable-snapshot-after = on" in {
      val sys2 = ActorSystem(
        "PersistentFsmSpec2",
        ConfigFactory.parseString("""
            akka.persistence.fsm.enable-snapshot-after = on
            akka.persistence.fsm.snapshot-after = 3
          """).withFallback(PersistenceSpec.config("inmem", "PersistentFSMSpec2")))

      try {
        val probe = TestProbe()
        val fsmRef = sys2.actorOf(SnapshotFSM.props(probe.ref))

        fsmRef ! 1
        fsmRef ! 2
        fsmRef ! 3
        // Needs to wait with expectMsg, before sending the next message to fsmRef.
        // Otherwise, stateData sent to this probe is already updated
        probe.expectMsg("SeqNo=3, StateData=List(3, 2, 1)")

        fsmRef ! "4x" //changes the state to Persist4xAtOnce, also updates SeqNo although nothing is persisted
        fsmRef ! 10 //Persist4xAtOnce = persist 10, 4x times
        // snapshot-after = 3, but the SeqNo is not multiple of 3,
        // as saveStateSnapshot() is called at the end of persistent event "batch" = 4x of 10's.
        probe.expectMsg("SeqNo=8, StateData=List(10, 10, 10, 10, 3, 2, 1)")

      } finally {
        val storageLocations =
          List("akka.persistence.snapshot-store.local.dir").map(s => new File(sys2.settings.config.getString(s)))
        shutdown(sys2)
        storageLocations.foreach(FileUtils.deleteDirectory)
      }
    }
  }
}

@nowarn("msg=deprecated")
object PersistentFSMSpec {
  //#customer-states
  sealed trait UserState extends FSMState
  case object LookingAround extends UserState {
    override def identifier: String = "Looking Around"
  }
  case object Shopping extends UserState {
    override def identifier: String = "Shopping"
  }
  case object Inactive extends UserState {
    override def identifier: String = "Inactive"
  }
  case object Paid extends UserState {
    override def identifier: String = "Paid"
  }
  //#customer-states

  //#customer-states-data
  case class Item(id: String, name: String, price: Float)

  sealed trait ShoppingCart {
    def addItem(item: Item): ShoppingCart
    def empty(): ShoppingCart
  }
  case object EmptyShoppingCart extends ShoppingCart {
    def addItem(item: Item) = NonEmptyShoppingCart(item :: Nil)
    def empty() = this
  }
  case class NonEmptyShoppingCart(items: Seq[Item]) extends ShoppingCart {
    def addItem(item: Item) = NonEmptyShoppingCart(items :+ item)
    def empty() = EmptyShoppingCart
  }
  //#customer-states-data

  //#customer-commands
  sealed trait Command
  case class AddItem(item: Item) extends Command
  case object Buy extends Command
  case object Leave extends Command
  case object GetCurrentCart extends Command
  //#customer-commands

  //#customer-domain-events
  sealed trait DomainEvent
  case class ItemAdded(item: Item) extends DomainEvent
  case object OrderExecuted extends DomainEvent
  case object OrderDiscarded extends DomainEvent
  case object CustomerInactive extends DomainEvent
  //#customer-domain-events

  //Side effects - report events to be sent to some "Report Actor"
  sealed trait ReportEvent
  case class PurchaseWasMade(items: Seq[Item]) extends ReportEvent
  case object ShoppingCardDiscarded extends ReportEvent

  class SimpleTransitionFSM(_persistenceId: String, reportActor: ActorRef)(
      implicit val domainEventClassTag: ClassTag[DomainEvent])
      extends PersistentFSM[UserState, ShoppingCart, DomainEvent] {
    override val persistenceId = _persistenceId

    startWith(LookingAround, EmptyShoppingCart)

    when(LookingAround) {
      case Event("stay", _) => stay()
      case Event(_, _)      => goto(LookingAround)
    }

    onTransition {
      case (from, to) => reportActor ! s"$from -> $to"
    }

    override def applyEvent(domainEvent: DomainEvent, currentData: ShoppingCart): ShoppingCart =
      currentData
  }
  object SimpleTransitionFSM {
    def props(persistenceId: String, reportActor: ActorRef) =
      Props(new SimpleTransitionFSM(persistenceId, reportActor))
  }

  class WebStoreCustomerFSM(_persistenceId: String, reportActor: ActorRef)(
      implicit val domainEventClassTag: ClassTag[DomainEvent])
      extends PersistentFSM[UserState, ShoppingCart, DomainEvent] {

    override def persistenceId = _persistenceId

    //#customer-fsm-body
    startWith(LookingAround, EmptyShoppingCart)

    when(LookingAround) {
      case Event(AddItem(item), _) =>
        goto(Shopping).applying(ItemAdded(item)).forMax(1 seconds)
      case Event(GetCurrentCart, data) =>
        stay().replying(data)
    }

    when(Shopping) {
      case Event(AddItem(item), _) =>
        stay().applying(ItemAdded(item)).forMax(1 seconds)
      case Event(Buy, _) =>
        //#customer-andthen-example
        goto(Paid).applying(OrderExecuted).andThen {
          case NonEmptyShoppingCart(items) =>
            reportActor ! PurchaseWasMade(items)
            //#customer-andthen-example
            saveStateSnapshot()
          case EmptyShoppingCart => saveStateSnapshot()
          //#customer-andthen-example
        }
      //#customer-andthen-example
      case Event(Leave, _) =>
        //#customer-snapshot-example
        stop().applying(OrderDiscarded).andThen {
          case _ =>
            reportActor ! ShoppingCardDiscarded
            saveStateSnapshot()
        }
      //#customer-snapshot-example
      case Event(GetCurrentCart, data) =>
        stay().replying(data)
      case Event(StateTimeout, _) =>
        goto(Inactive).forMax(2 seconds)
    }

    when(Inactive) {
      case Event(AddItem(item), _) =>
        goto(Shopping).applying(ItemAdded(item)).forMax(1 seconds)
      case Event(StateTimeout, _) =>
        stop().applying(OrderDiscarded).andThen {
          case _ => reportActor ! ShoppingCardDiscarded
        }
    }

    when(Paid) {
      case Event(Leave, _) => stop()
      case Event(GetCurrentCart, data) =>
        stay().replying(data)
    }
    //#customer-fsm-body

    /**
     * Override this handler to define the action on Domain Event
     *
     * @param event domain event to apply
     * @param cartBeforeEvent state data of the previous state
     */
    //#customer-apply-event
    override def applyEvent(event: DomainEvent, cartBeforeEvent: ShoppingCart): ShoppingCart = {
      event match {
        case ItemAdded(item)  => cartBeforeEvent.addItem(item)
        case OrderExecuted    => cartBeforeEvent
        case OrderDiscarded   => cartBeforeEvent.empty()
        case CustomerInactive => cartBeforeEvent
      }
    }
    //#customer-apply-event
  }

  object WebStoreCustomerFSM {
    def props(persistenceId: String, reportActor: ActorRef) =
      Props(new WebStoreCustomerFSM(persistenceId, reportActor))
  }

  class PersistentEventsStreamer(id: String, client: ActorRef) extends PersistentActor {
    override val persistenceId: String = id

    def receiveRecover = {
      case RecoveryCompleted => // do nothing
      case persistentEvent   => client ! persistentEvent
    }

    def receiveCommand = {
      case _ => // do nothing
    }
  }

  object PersistentEventsStreamer {
    def props(persistenceId: String, client: ActorRef) =
      Props(new PersistentEventsStreamer(persistenceId, client))
  }

  object TimeoutFSM {
    val OverrideTimeoutToInf = "override-timeout-to-inf"
    case class State(identifier: String) extends PersistentFSM.FSMState
    def props(probe: ActorRef) = Props(new TimeoutFSM(probe))
  }

  class TimeoutFSM(probe: ActorRef)(implicit val domainEventClassTag: ClassTag[String])
      extends Actor
      with PersistentFSM[TimeoutFSM.State, String, String] {

    import TimeoutFSM._
    override def applyEvent(domainEvent: String, currentData: String): String = "whatever"
    override def persistenceId: String = "timeout-test"

    startWith(State("init"), "")

    when(State("init"), stateTimeout = 300.millis) {
      case Event(StateTimeout, _) =>
        probe ! StateTimeout
        stay()

      case Event(OverrideTimeoutToInf, _) =>
        probe ! OverrideTimeoutToInf
        stay().forMax(Duration.Inf)
    }
  }

  sealed trait SnapshotFSMState extends PersistentFSM.FSMState
  case object PersistSingleAtOnce extends SnapshotFSMState { override def identifier: String = "PersistSingleAtOnce" }
  case object Persist4xAtOnce extends SnapshotFSMState { override def identifier: String = "Persist4xAtOnce" }

  sealed trait SnapshotFSMEvent
  case class IntAdded(i: Int) extends SnapshotFSMEvent

  object SnapshotFSM {
    def props(probe: ActorRef) = Props(new SnapshotFSM(probe))
  }

  class SnapshotFSM(probe: ActorRef)(implicit val domainEventClassTag: ClassTag[SnapshotFSMEvent])
      extends Actor
      with PersistentFSM[SnapshotFSMState, List[Int], SnapshotFSMEvent] {

    override def persistenceId: String = "snapshot-fsm-test"

    override def applyEvent(event: SnapshotFSMEvent, currentData: List[Int]): List[Int] = event match {
      case IntAdded(i) => i :: currentData
    }

    startWith(PersistSingleAtOnce, Nil)

    when(PersistSingleAtOnce) {
      case Event(i: Int, _) =>
        stay().applying(IntAdded(i))
      case Event("4x", _) =>
        goto(Persist4xAtOnce)
      case Event(SaveSnapshotSuccess(metadata), _) =>
        probe ! s"SeqNo=${metadata.sequenceNr}, StateData=${stateData}"
        stay()
    }

    when(Persist4xAtOnce) {
      case Event(i: Int, _) =>
        stay().applying(IntAdded(i), IntAdded(i), IntAdded(i), IntAdded(i))
      case Event(SaveSnapshotSuccess(metadata), _) =>
        probe ! s"SeqNo=${metadata.sequenceNr}, StateData=${stateData}"
        stay()
    }
  }

}

class InmemPersistentFSMSpec extends PersistentFSMSpec(PersistenceSpec.config("inmem", "PersistentFSMSpec"))
