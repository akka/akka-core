/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.testkit._

object AtLeastOnceDeliveryFailureSpec {
  val config = ConfigFactory.parseString(
    """
      akka.persistence.sender.chaos.live-processing-failure-rate = 0.3
      akka.persistence.sender.chaos.replay-processing-failure-rate = 0.1
      akka.persistence.destination.chaos.confirm-failure-rate = 0.3
      akka.persistence.journal.plugin = "akka.persistence.journal.chaos"
      akka.persistence.journal.chaos.write-failure-rate = 0.3
      akka.persistence.journal.chaos.confirm-failure-rate = 0.2
      akka.persistence.journal.chaos.delete-failure-rate = 0.3
      akka.persistence.journal.chaos.replay-failure-rate = 0.25
      akka.persistence.journal.chaos.read-highest-failure-rate = 0.1
      akka.persistence.journal.chaos.class = akka.persistence.journal.chaos.ChaosJournal
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "target/snapshots-at-least-once-delivery-failure-spec/"
    """)

  val numMessages = 10

  case object Start
  case class Done(ints: Vector[Int])

  case class Ack(i: Int)

  case class Msg(deliveryId: Long, i: Int)
  case class Confirm(deliveryId: Long, i: Int)

  sealed trait Evt
  case class MsgSent(i: Int) extends Evt
  case class MsgConfirmed(deliveryId: Long, i: Int) extends Evt

  trait ChaosSupport { this: Actor =>
    def random = ThreadLocalRandom.current

    def probe: ActorRef

    var state = Vector.empty[Int]

    def contains(i: Int): Boolean =
      state.contains(i)

    def add(i: Int): Unit = {
      state :+= i
      if (state.length == numMessages) probe ! Done(state)
    }

    def shouldFail(rate: Double) =
      random.nextDouble() < rate
  }

  class ChaosSender(destination: ActorRef, val probe: ActorRef)
      extends PersistentActor
      with ChaosSupport
      with ActorLogging
      with AtLeastOnceDelivery {
    val config = context.system.settings.config.getConfig("akka.persistence.sender.chaos")
    val liveProcessingFailureRate = config.getDouble("live-processing-failure-rate")
    val replayProcessingFailureRate = config.getDouble("replay-processing-failure-rate")

    override def redeliverInterval = 500.milliseconds

    override def persistenceId = "chaosSender"

    def receiveCommand: Receive = {
      case i: Int =>
        if (contains(i)) {
          log.debug(debugMessage(s"ignored duplicate ${i}"))
          sender() ! Ack(i)
        } else {
          persist(MsgSent(i)) { evt =>
            updateState(evt)
            sender() ! Ack(i)
            if (shouldFail(liveProcessingFailureRate))
              throw new TestException(debugMessage(s"failed at payload $i"))
            else
              log.debug(debugMessage(s"processed payload $i"))
          }

        }

      case Confirm(deliveryId, i) => persist(MsgConfirmed(deliveryId, i))(updateState)
    }

    def receiveRecover: Receive = {
      case evt: Evt =>
        updateState(evt)
        if (shouldFail(replayProcessingFailureRate))
          throw new TestException(debugMessage(s"replay failed at event $evt"))
        else
          log.debug(debugMessage(s"replayed event $evt"))
    }

    def updateState(evt: Evt): Unit = evt match {
      case MsgSent(i) =>
        add(i)
        deliver(destination.path)(deliveryId => Msg(deliveryId, i))

      case MsgConfirmed(deliveryId, _) =>
        confirmDelivery(deliveryId)
    }

    private def debugMessage(msg: String): String =
      s"[sender] ${msg} (mode = ${if (recoveryRunning) "replay" else "live"} snr = ${lastSequenceNr} state = ${state.sorted})"

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      // mute logging
    }

    override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      // mute logging
    }
  }

  class ChaosDestination(val probe: ActorRef) extends Actor with ChaosSupport with ActorLogging {
    val config = context.system.settings.config.getConfig("akka.persistence.destination.chaos")
    val confirmFailureRate = config.getDouble("confirm-failure-rate")

    def receive = {
      case m @ Msg(deliveryId, i) =>
        if (shouldFail(confirmFailureRate)) {
          log.debug(debugMessage("confirm message failed", m))
        } else if (contains(i)) {
          log.debug(debugMessage("ignored duplicate", m))
          sender() ! Confirm(deliveryId, i)
        } else {
          add(i)
          sender() ! Confirm(deliveryId, i)
          log.debug(debugMessage("received and confirmed message", m))
        }
    }

    private def debugMessage(msg: String, m: Msg): String =
      s"[destination] ${msg} (message = $m)"
  }

  class ChaosApp(probe: ActorRef) extends Actor with ActorLogging {
    val destination = context.actorOf(Props(classOf[ChaosDestination], probe), "destination")
    var snd = createSender()
    var acks = Set.empty[Int]

    def createSender(): ActorRef =
      context.watch(context.actorOf(Props(classOf[ChaosSender], destination, probe), "sender"))

    def receive = {
      case Start         => (1 to numMessages).foreach(snd ! _)
      case Ack(i)        => acks += i
      case Terminated(_) =>
        // snd will be stopped if recovery or persist fails
        log.debug(s"sender stopped, starting it again")
        snd = createSender()
        (1 to numMessages).foreach(i => if (!acks(i)) snd ! i)
    }
  }
}

class AtLeastOnceDeliveryFailureSpec
    extends AkkaSpec(AtLeastOnceDeliveryFailureSpec.config)
    with Cleanup
    with ImplicitSender {
  import AtLeastOnceDeliveryFailureSpec._

  muteDeadLetters(classOf[AnyRef])(system)

  "AtLeastOnceDelivery" must {
    "tolerate and recover from random failures" in {
      system.actorOf(Props(classOf[ChaosApp], testActor), "chaosApp") ! Start
      expectDone() // by sender
      expectDone() // by destination

      system.actorOf(Props(classOf[ChaosApp], testActor), "chaosApp2") // recovery of new instance should have same outcome
      expectDone() // by sender
      // destination doesn't receive messages again because all have been confirmed already
    }
  }

  def expectDone() = within(numMessages.seconds) {
    expectMsgType[Done].ints.sorted should ===(1 to numMessages toVector)
  }
}
