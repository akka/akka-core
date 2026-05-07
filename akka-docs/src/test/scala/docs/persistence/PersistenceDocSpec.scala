/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence

import akka.actor._
import akka.pattern.{ BackoffOpts, BackoffSupervisor }
import akka.persistence._

import scala.annotation.nowarn
import scala.concurrent.duration._

@nowarn("msg=never used") // sample snippets
object PersistenceDocSpec {

  trait SomeOtherMessage

  val persistentActor: ActorRef = ???

  val config =
    """
      //#auto-update-interval
      akka.persistence.view.auto-update-interval = 5s
      //#auto-update-interval
      //#auto-update
      akka.persistence.view.auto-update = off
      //#auto-update
    """

  object RecoverySample {
    trait MyPersistentActor1 extends PersistentActor {
      //#recovery-disabled
      override def recovery = Recovery.none
      //#recovery-disabled
    }

    trait MyPersistentActor2 extends PersistentActor {
      //#recovery-custom
      override def recovery = Recovery(toSequenceNr = 457L)
      //#recovery-custom
    }

    class MyPersistentActor4 extends PersistentActor {
      override def persistenceId = "my-stable-persistence-id"

      //#recovery-completed

      override def receiveRecover: Receive = {
        case RecoveryCompleted =>
        // perform init after recovery, before any other messages
        //...
        case evt => //...
      }

      override def receiveCommand: Receive = {
        case msg => //...
      }
      //#recovery-completed
    }

    trait MyPersistentActor5 extends PersistentActor {
      //#recovery-no-snap
      override def recovery =
        Recovery(fromSnapshot = SnapshotSelectionCriteria.None)
      //#recovery-no-snap
    }
  }

  object PersistenceId {
    trait PersistentActorMethods {
      //#persistence-id
      def persistenceId: String
      //#persistence-id
      //#recovery-status
      def recoveryRunning: Boolean
      def recoveryFinished: Boolean
      //#recovery-status
    }
    class MyPersistentActor1 extends PersistentActor with PersistentActorMethods {
      //#persistence-id-override
      override def persistenceId = "my-stable-persistence-id"
      //#persistence-id-override

      override def receiveRecover: Receive = {
        case _ =>
      }
      override def receiveCommand: Receive = {
        case _ =>
      }
    }
  }

  object BackoffOnStop {
    abstract class MyActor extends Actor {
      import PersistAsync.MyPersistentActor
      //#backoff
      val childProps = Props[MyPersistentActor]()
      val props = BackoffSupervisor.props(BackoffOpts
        .onStop(childProps, childName = "myActor", minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2))
      context.actorOf(props, name = "mySupervisor")
      //#backoff
    }

  }

  object AtLeastOnce {
    //#at-least-once-example
    import akka.actor.{ Actor, ActorSelection }
    import akka.persistence.AtLeastOnceDelivery

    case class Msg(deliveryId: Long, s: String)
    case class Confirm(deliveryId: Long)

    sealed trait Evt
    case class MsgSent(s: String) extends Evt
    case class MsgConfirmed(deliveryId: Long) extends Evt

    class MyPersistentActor(destination: ActorSelection) extends PersistentActor with AtLeastOnceDelivery {

      override def persistenceId: String = "persistence-id"

      override def receiveCommand: Receive = {
        case s: String           => persist(MsgSent(s))(updateState)
        case Confirm(deliveryId) => persist(MsgConfirmed(deliveryId))(updateState)
      }

      override def receiveRecover: Receive = {
        case evt: Evt => updateState(evt)
      }

      def updateState(evt: Evt): Unit = evt match {
        case MsgSent(s) =>
          deliver(destination)(deliveryId => Msg(deliveryId, s))

        case MsgConfirmed(deliveryId) => confirmDelivery(deliveryId)
      }
    }

    class MyDestination extends Actor {
      def receive = {
        case Msg(deliveryId, s) =>
          // ...
          sender() ! Confirm(deliveryId)
      }
    }
    //#at-least-once-example
  }

  object SaveSnapshot {

    class MyPersistentActor extends PersistentActor {
      override def persistenceId = "my-stable-persistence-id"

      def updateState(event: String): Unit = {}

      //#save-snapshot
      var state: Any = _

      val snapShotInterval = 1000
      override def receiveCommand: Receive = {
        case SaveSnapshotSuccess(metadata)         => // ...
        case SaveSnapshotFailure(metadata, reason) => // ...
        case cmd: String =>
          persist(s"evt-$cmd") { e =>
            updateState(e)
            if (lastSequenceNr % snapShotInterval == 0 && lastSequenceNr != 0)
              saveSnapshot(state)
          }
      }
      //#save-snapshot

      override def receiveRecover: Receive = ???
    }
  }

  object OfferSnapshot {
    class MyPersistentActor extends PersistentActor {
      override def persistenceId = "my-stable-persistence-id"

      //#snapshot-criteria
      override def recovery =
        Recovery(
          fromSnapshot = SnapshotSelectionCriteria(maxSequenceNr = 457L, maxTimestamp = System.currentTimeMillis))
      //#snapshot-criteria

      //#snapshot-offer
      var state: Any = _

      override def receiveRecover: Receive = {
        case SnapshotOffer(metadata, offeredSnapshot) => state = offeredSnapshot
        case RecoveryCompleted                        =>
        case event                                    => // ...
      }
      //#snapshot-offer

      override def receiveCommand: Receive = ???
    }

  }

  object PersistAsync {

    //#persist-async
    class MyPersistentActor extends PersistentActor {

      override def persistenceId = "my-stable-persistence-id"

      override def receiveRecover: Receive = {
        case _ => // handle recovery here
      }

      override def receiveCommand: Receive = {
        case c: String => {
          sender() ! c
          persistAsync(s"evt-$c-1") { e =>
            sender() ! e
          }
          persistAsync(s"evt-$c-2") { e =>
            sender() ! e
          }
        }
      }
    }

    // usage
    persistentActor ! "a"
    persistentActor ! "b"

    // possible order of received messages:
    // a
    // b
    // evt-a-1
    // evt-a-2
    // evt-b-1
    // evt-b-2

    //#persist-async
  }

  object Defer {

    //#defer
    class MyPersistentActor extends PersistentActor {

      override def persistenceId = "my-stable-persistence-id"

      override def receiveRecover: Receive = {
        case _ => // handle recovery here
      }

      override def receiveCommand: Receive = {
        case c: String => {
          sender() ! c
          persistAsync(s"evt-$c-1") { e =>
            sender() ! e
          }
          persistAsync(s"evt-$c-2") { e =>
            sender() ! e
          }
          deferAsync(s"evt-$c-3") { e =>
            sender() ! e
          }
        }
      }
    }
    //#defer

    //#defer-caller
    persistentActor ! "a"
    persistentActor ! "b"

    // order of received messages:
    // a
    // b
    // evt-a-1
    // evt-a-2
    // evt-a-3
    // evt-b-1
    // evt-b-2
    // evt-b-3

    //#defer-caller
  }

  object DeferWithPersist {
    //#defer-with-persist
    class MyPersistentActor extends PersistentActor {

      override def persistenceId = "my-stable-persistence-id"

      override def receiveRecover: Receive = {
        case _ => // handle recovery here
      }

      override def receiveCommand: Receive = {
        case c: String => {
          sender() ! c
          persist(s"evt-$c-1") { e =>
            sender() ! e
          }
          persist(s"evt-$c-2") { e =>
            sender() ! e
          }
          defer(s"evt-$c-3") { e =>
            sender() ! e
          }
        }
      }
    }
    //#defer-with-persist
  }

  object NestedPersists {

    class MyPersistentActor extends PersistentActor {
      override def persistenceId = "my-stable-persistence-id"

      override def receiveRecover: Receive = {
        case _ => // handle recovery here
      }

      //#nested-persist-persist
      override def receiveCommand: Receive = {
        case c: String =>
          sender() ! c

          persist(s"$c-1-outer") { outer1 =>
            sender() ! outer1
            persist(s"$c-1-inner") { inner1 =>
              sender() ! inner1
            }
          }

          persist(s"$c-2-outer") { outer2 =>
            sender() ! outer2
            persist(s"$c-2-inner") { inner2 =>
              sender() ! inner2
            }
          }
      }
      //#nested-persist-persist
    }

    //#nested-persist-persist-caller
    persistentActor ! "a"
    persistentActor ! "b"

    // order of received messages:
    // a
    // a-outer-1
    // a-outer-2
    // a-inner-1
    // a-inner-2
    // and only then process "b"
    // b
    // b-outer-1
    // b-outer-2
    // b-inner-1
    // b-inner-2

    //#nested-persist-persist-caller

    class MyPersistAsyncActor extends PersistentActor {
      override def persistenceId = "my-stable-persistence-id"

      override def receiveRecover: Receive = {
        case _ => // handle recovery here
      }

      //#nested-persistAsync-persistAsync
      override def receiveCommand: Receive = {
        case c: String =>
          sender() ! c
          persistAsync(c + "-outer-1") { outer =>
            sender() ! outer
            persistAsync(c + "-inner-1") { inner =>
              sender() ! inner
            }
          }
          persistAsync(c + "-outer-2") { outer =>
            sender() ! outer
            persistAsync(c + "-inner-2") { inner =>
              sender() ! inner
            }
          }
      }
      //#nested-persistAsync-persistAsync
    }

    //#nested-persistAsync-persistAsync-caller
    persistentActor ! "a"
    persistentActor ! "b"

    // order of received messages:
    // a
    // b
    // a-outer-1
    // a-outer-2
    // b-outer-1
    // b-outer-2
    // a-inner-1
    // a-inner-2
    // b-inner-1
    // b-inner-2

    // which can be seen as the following causal relationship:
    // a -> a-outer-1 -> a-outer-2 -> a-inner-1 -> a-inner-2
    // b -> b-outer-1 -> b-outer-2 -> b-inner-1 -> b-inner-2

    //#nested-persistAsync-persistAsync-caller
  }

  object AvoidPoisonPill {

    //#safe-shutdown
    /** Explicit shutdown message */
    case object Shutdown

    class SafePersistentActor extends PersistentActor {
      override def persistenceId = "safe-actor"

      override def receiveCommand: Receive = {
        case c: String =>
          println(c)
          persist(s"handle-$c") { println(_) }
        case Shutdown =>
          context.stop(self)
      }

      override def receiveRecover: Receive = {
        case _ => // handle recovery here
      }
    }
    //#safe-shutdown

    //#safe-shutdown-example-bad
    // UN-SAFE, due to PersistentActor's command stashing:
    persistentActor ! "a"
    persistentActor ! "b"
    persistentActor ! PoisonPill
    // order of received messages:
    // a
    //   # b arrives at mailbox, stashing;        internal-stash = [b]
    // PoisonPill is an AutoReceivedMessage, is handled automatically
    // !! stop !!
    // Actor is stopped without handling `b` nor the `a` handler!
    //#safe-shutdown-example-bad

    //#safe-shutdown-example-good
    // SAFE:
    persistentActor ! "a"
    persistentActor ! "b"
    persistentActor ! Shutdown
    // order of received messages:
    // a
    //   # b arrives at mailbox, stashing;        internal-stash = [b]
    //   # Shutdown arrives at mailbox, stashing; internal-stash = [b, Shutdown]
    // handle-a
    //   # unstashing;                            internal-stash = [Shutdown]
    // b
    // handle-b
    //   # unstashing;                            internal-stash = []
    // Shutdown
    // -- stop --
    //#safe-shutdown-example-good
  }

}
