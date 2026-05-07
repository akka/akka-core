/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.control.NoStackTrace

import akka.actor._
import akka.persistence.AtLeastOnceDelivery.{ AtLeastOnceDeliverySnapshot, UnconfirmedWarning }
import akka.testkit._

object AtLeastOnceDeliverySpec {

  case class Req(payload: String)
  case object ReqAck
  case object InvalidReq

  sealed trait Evt
  case class AcceptedReq(payload: String, destination: ActorPath) extends Evt
  case class ReqDone(id: Long) extends Evt

  case class Action(id: Long, payload: String)
  case class ActionAck(id: Long)
  case object Boom
  case object SaveSnap
  case class Snap(deliverySnapshot: AtLeastOnceDeliverySnapshot) // typically includes some user data as well

  def senderProps(
      testActor: ActorRef,
      name: String,
      redeliverInterval: FiniteDuration,
      warnAfterNumberOfUnconfirmedAttempts: Int,
      redeliveryBurstLimit: Int,
      destinations: Map[String, ActorPath],
      async: Boolean,
      actorSelectionDelivery: Boolean = false): Props =
    Props(
      new Sender(
        testActor,
        name,
        redeliverInterval,
        warnAfterNumberOfUnconfirmedAttempts,
        redeliveryBurstLimit,
        destinations,
        async,
        actorSelectionDelivery))

  class Sender(
      testActor: ActorRef,
      name: String,
      override val redeliverInterval: FiniteDuration,
      override val warnAfterNumberOfUnconfirmedAttempts: Int,
      override val redeliveryBurstLimit: Int,
      destinations: Map[String, ActorPath],
      async: Boolean,
      actorSelectionDelivery: Boolean)
      extends PersistentActor
      with AtLeastOnceDelivery
      with ActorLogging {

    override def persistenceId: String = name

    // simplistic confirmation mechanism, to tell the requester that a snapshot succeeded
    var lastSnapshotAskedForBy: Option[ActorRef] = None

    def updateState(evt: Evt): Unit = evt match {
      case AcceptedReq(payload, destination) if actorSelectionDelivery =>
        log.debug(s"deliver(destination, deliveryId => Action(deliveryId, $payload)), recovery: " + recoveryRunning)
        deliver(context.actorSelection(destination))(deliveryId => Action(deliveryId, payload))

      case AcceptedReq(payload, destination) =>
        log.debug(s"deliver(destination, deliveryId => Action(deliveryId, $payload)), recovery: " + recoveryRunning)
        deliver(destination)(deliveryId => Action(deliveryId, payload))

      case ReqDone(id) =>
        log.debug(s"confirmDelivery($id), recovery: " + recoveryRunning)
        confirmDelivery(id)
    }

    val receiveCommand: Receive = {
      case Req(payload) =>
        if (payload.isEmpty)
          sender() ! InvalidReq
        else {
          val destination = destinations(payload.take(1).toUpperCase)
          if (async)
            persistAsync(AcceptedReq(payload, destination)) { evt =>
              updateState(evt)
              sender() ! ReqAck
            } else
            persist(AcceptedReq(payload, destination)) { evt =>
              updateState(evt)
              sender() ! ReqAck
            }
        }

      case ActionAck(id) =>
        log.debug("Sender got ack {}", id)
        if (confirmDelivery(id))
          if (async)
            persistAsync(ReqDone(id)) { evt =>
              updateState(evt)
            } else
            persist(ReqDone(id)) { evt =>
              updateState(evt)
            }

      case Boom =>
        log.debug("Boom!")
        throw new RuntimeException("boom") with NoStackTrace

      case SaveSnap =>
        log.debug("Save snapshot!")
        lastSnapshotAskedForBy = Some(sender())
        saveSnapshot(Snap(getDeliverySnapshot))

      case success: SaveSnapshotSuccess =>
        log.debug("Snapshot success!")
        lastSnapshotAskedForBy.map(_ ! success)

      case w: UnconfirmedWarning =>
        log.debug("Sender got unconfirmed warning {}", w)
        testActor ! w

    }

    def receiveRecover: Receive = {
      case evt: Evt =>
        updateState(evt)

      case SnapshotOffer(_, Snap(deliverySnapshot)) =>
        setDeliverySnapshot(deliverySnapshot)
    }
  }

  def destinationProps(testActor: ActorRef): Props =
    Props(new Destination(testActor))

  class Destination(testActor: ActorRef) extends Actor with ActorLogging {

    var allReceived = Set.empty[Long]

    def receive = {
      case a @ Action(id, _) =>
        // discard duplicates (naive impl)
        if (!allReceived.contains(id)) {
          log.debug("Destination got {}, all count {}", a, allReceived.size + 1)
          testActor ! a
          allReceived += id
        }
        sender() ! ActionAck(id)
    }
  }

  def unreliableProps(dropMod: Int, target: ActorRef): Props =
    Props(new Unreliable(dropMod, target))

  class Unreliable(dropMod: Int, target: ActorRef) extends Actor with ActorLogging {
    var count = 0
    def receive = {
      case msg =>
        count += 1
        if (count % dropMod != 0) {
          log.debug("Pass msg {} count {}", msg, count)
          target.forward(msg)
        } else {
          log.debug("Drop msg {} count {}", msg, count)
        }
    }
  }

  class DeliverToStarSelection(name: String) extends PersistentActor with AtLeastOnceDelivery {
    override def persistenceId = name

    override def receiveCommand = {
      case any =>
        // this is not supported currently, so expecting exception
        try deliver(context.actorSelection("*"))(id => s"$any$id")
        catch { case ex: Exception => sender() ! Failure(ex) }
    }

    override def receiveRecover = Actor.emptyBehavior
  }

}

class AtLeastOnceDeliverySpec
    extends PersistenceSpec(PersistenceSpec.config("inmem", "AtLeastOnceDeliverySpec"))
    with ImplicitSender {

  import akka.persistence.AtLeastOnceDeliverySpec._

  "AtLeastOnceDelivery" must {
    List(true, false).foreach { deliverUsingActorSelection =>
      s"deliver messages in order when nothing is lost (using actorSelection: $deliverUsingActorSelection)" taggedAs (TimingTest) in {
        val probe = TestProbe()
        val probeA = TestProbe()
        val destinations = Map("A" -> system.actorOf(destinationProps(probeA.ref)).path)
        val snd = system.actorOf(senderProps(probe.ref, name, 1000.millis, 5, 1000, destinations, async = false), name)
        snd.tell(Req("a"), probe.ref)
        probe.expectMsg(ReqAck)
        probeA.expectMsg(Action(1, "a"))
        probeA.expectNoMessage(1.second)
      }

      s"re-deliver lost messages (using actorSelection: $deliverUsingActorSelection)" taggedAs (TimingTest) in {
        val probe = TestProbe()
        val probeA = TestProbe()
        val dst = system.actorOf(destinationProps(probeA.ref))
        val destinations = Map("A" -> system.actorOf(unreliableProps(3, dst)).path)
        val snd = system.actorOf(
          senderProps(
            probe.ref,
            name,
            2.seconds,
            5,
            1000,
            destinations,
            async = false,
            actorSelectionDelivery = deliverUsingActorSelection),
          name)
        snd.tell(Req("a-1"), probe.ref)
        probe.expectMsg(ReqAck)
        probeA.expectMsg(Action(1, "a-1"))

        snd.tell(Req("a-2"), probe.ref)
        probe.expectMsg(ReqAck)
        probeA.expectMsg(Action(2, "a-2"))

        snd.tell(Req("a-3"), probe.ref)
        snd.tell(Req("a-4"), probe.ref)
        probe.expectMsg(ReqAck)
        probe.expectMsg(ReqAck)
        // a-3 was lost
        probeA.expectMsg(Action(4, "a-4"))
        // and then re-delivered
        probeA.expectMsg(Action(3, "a-3"))
        probeA.expectNoMessage(1.second)
      }
    }

    "not allow using actorSelection with wildcards" in {
      system.actorOf(Props(classOf[DeliverToStarSelection], name)) ! "anything, really."
      expectMsgType[Failure[_]].toString should include("not supported")
    }

    "re-deliver lost messages after restart" taggedAs (TimingTest) in {
      val probe = TestProbe()
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(3, dst)).path)
      val snd = system.actorOf(senderProps(probe.ref, name, 2.seconds, 5, 1000, destinations, async = false), name)
      snd.tell(Req("a-1"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a-1"))

      snd.tell(Req("a-2"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(2, "a-2"))

      snd.tell(Req("a-3"), probe.ref)
      snd.tell(Req("a-4"), probe.ref)
      probe.expectMsg(ReqAck)
      probe.expectMsg(ReqAck)
      // a-3 was lost
      probeA.expectMsg(Action(4, "a-4"))

      // trigger restart
      snd.tell(Boom, probe.ref)

      // and then re-delivered
      probeA.expectMsg(Action(3, "a-3"))

      snd.tell(Req("a-5"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(5, "a-5"))

      probeA.expectNoMessage(1.second)
    }

    "re-send replayed deliveries with an 'initially in-order' strategy, before delivering fresh messages" taggedAs (TimingTest) in {
      val probe = TestProbe()
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(2, dst)).path)
      val snd = system.actorOf(senderProps(probe.ref, name, 2.seconds, 5, 1000, destinations, async = false), name)
      snd.tell(Req("a-1"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a-1"))

      snd.tell(Req("a-2"), probe.ref)
      probe.expectMsg(ReqAck)
      // a-2 was lost

      snd.tell(Req("a-3"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(3, "a-3"))

      snd.tell(Req("a-4"), probe.ref)
      probe.expectMsg(ReqAck)
      // a-4 was lost

      // trigger restart
      snd.tell(Boom, probe.ref)
      snd.tell(Req("a-5"), probe.ref)
      probe.expectMsg(ReqAck)

      // and then re-delivered
      probeA.expectMsg(Action(2, "a-2")) // re-delivered
      // a-4 was re-delivered but lost
      probeA.expectMsgAllOf(
        Action(5, "a-5"), // re-delivered
        Action(4, "a-4")) // re-delivered, 3rd time

      probeA.expectNoMessage(1.second)
    }

    "restore state from snapshot" taggedAs (TimingTest) in {
      val probe = TestProbe()
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(3, dst)).path)
      val snd = system.actorOf(senderProps(probe.ref, name, 2.seconds, 5, 1000, destinations, async = false), name)
      snd.tell(Req("a-1"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(1, "a-1"))

      snd.tell(Req("a-2"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(2, "a-2"))

      snd.tell(Req("a-3"), probe.ref)
      snd.tell(Req("a-4"), probe.ref)
      snd.tell(SaveSnap, probe.ref)
      probe.expectMsg(ReqAck)
      probe.expectMsg(ReqAck)
      // a-3 was lost
      probeA.expectMsg(Action(4, "a-4"))

      // after snapshot succeeded
      probe.expectMsgType[SaveSnapshotSuccess]

      // trigger restart
      snd.tell(Boom, probe.ref)

      // and then re-delivered
      probeA.expectMsg(Action(3, "a-3"))

      snd.tell(Req("a-5"), probe.ref)
      probe.expectMsg(ReqAck)
      probeA.expectMsg(Action(5, "a-5"))

      probeA.expectNoMessage(1.second)
    }

    "warn about unconfirmed messages" taggedAs (TimingTest) in {
      val probe = TestProbe()
      val probeA = TestProbe()
      val probeB = TestProbe()
      val destinations = Map("A" -> probeA.ref.path, "B" -> probeB.ref.path)
      val snd = system.actorOf(senderProps(probe.ref, name, 1000.millis, 3, 1000, destinations, async = false), name)
      snd.tell(Req("a-1"), probe.ref)
      snd.tell(Req("b-1"), probe.ref)
      snd.tell(Req("b-2"), probe.ref)
      probe.expectMsg(ReqAck)
      probe.expectMsg(ReqAck)
      probe.expectMsg(ReqAck)
      val unconfirmed = probe
        .receiveWhile(5.seconds) {
          case UnconfirmedWarning(unconfirmed) => unconfirmed
        }
        .flatten
      unconfirmed.map(_.destination).toSet should ===(Set(probeA.ref.path, probeB.ref.path))
      unconfirmed.map(_.message).toSet should be(Set(Action(1, "a-1"), Action(2, "b-1"), Action(3, "b-2")))
      system.stop(snd)
    }

    "re-deliver many lost messages" taggedAs (TimingTest) in {
      val probe = TestProbe()
      val probeA = TestProbe()
      val probeB = TestProbe()
      val probeC = TestProbe()
      val dstA = system.actorOf(destinationProps(probeA.ref), "destination-a")
      val dstB = system.actorOf(destinationProps(probeB.ref), "destination-b")
      val dstC = system.actorOf(destinationProps(probeC.ref), "destination-c")
      val destinations = Map(
        "A" -> system.actorOf(unreliableProps(2, dstA), "unreliable-a").path,
        "B" -> system.actorOf(unreliableProps(5, dstB), "unreliable-b").path,
        "C" -> system.actorOf(unreliableProps(3, dstC), "unreliable-c").path)
      val snd = system.actorOf(senderProps(probe.ref, name, 1000.millis, 5, 1000, destinations, async = true), name)
      val N = 100
      for (n <- 1 to N) {
        snd.tell(Req("a-" + n), probe.ref)
      }
      for (n <- 1 to N) {
        snd.tell(Req("b-" + n), probe.ref)
      }
      for (n <- 1 to N) {
        snd.tell(Req("c-" + n), probe.ref)
      }
      val deliverWithin = 20.seconds
      probeA.receiveN(N, deliverWithin).collect { case action: Action => action.payload }.toSet should ===(
        (1 to N).map(n => "a-" + n).toSet)
      probeB.receiveN(N, deliverWithin).collect { case action: Action => action.payload }.toSet should ===(
        (1 to N).map(n => "b-" + n).toSet)
      probeC.receiveN(N, deliverWithin).collect { case action: Action => action.payload }.toSet should ===(
        (1 to N).map(n => "c-" + n).toSet)
    }

    "limit the number of messages redelivered at once" taggedAs (TimingTest) in {
      val probe = TestProbe()
      val probeA = TestProbe()
      val dst = system.actorOf(destinationProps(probeA.ref))
      val destinations = Map("A" -> system.actorOf(unreliableProps(2, dst)).path)

      val snd = system.actorOf(senderProps(probe.ref, name, 2.seconds, 5, 2, destinations, async = true), name)

      val N = 10
      for (n <- 1 to N) {
        snd.tell(Req("a-" + n), probe.ref)
      }

      // initially all odd messages should go through
      for (n <- 1 to N if n % 2 == 1) probeA.expectMsg(Action(n, s"a-$n"))
      probeA.expectNoMessage(100.millis)

      // at each redelivery round, 2 (even) messages are sent, the first goes through
      // without throttling, at each round half of the messages would go through
      var toDeliver = (1 to N).filter(_ % 2 == 0).map(_.toLong).toSet
      for (n <- 1 to N) {
        if (n % 2 == 0) {
          toDeliver -= probeA.expectMsgType[Action].id
          probeA.expectNoMessage(100.millis)
        }
      }

      toDeliver should ===(Set.empty[Long])
    }
  }
}
