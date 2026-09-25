/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.Success

import akka.Done
import akka.actor.ActorRef
import akka.actor.Props
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

object JournalReplyOrderingSpec {
  def config(specName: String, numResequencers: Int) =
    ConfigFactory.parseString(s"""
        |akka.persistence.journal.controlled-in-mem.write-reply-ordering-groups = $numResequencers
        """.stripMargin).withFallback(ControlledInmemJournal.config(specName))

  val persistentActorCounter = new AtomicInteger

  val persistentActorProps = Props(new PersistentActor {
    override val persistenceId: String = persistentActorCounter.getAndIncrement().toString

    override def receiveRecover = {
      case _ => // nop, no meaningful state
    }

    override def receiveCommand = {
      case "cull" =>
        context.stop(self)

      case "get-persistence-id" =>
        sender() ! persistenceId

      case (s: String, p: Promise[Done @unchecked]) =>
        persist(s) { _ =>
          p.success(Done)
        }
    }
  })

  val oneSuccessUnit = List(Success(()))
}

abstract class JournalReplyOrderingSpec(specName: String, numResequencers: Int)
    extends AkkaSpec(JournalReplyOrderingSpec.config(specName, numResequencers))
    with ImplicitSender {
  import JournalReplyOrderingSpec._

  require(numResequencers > 0, "must have a positive number of resequencers")

  import system.dispatcher

  val journalInstanceId = system.settings.config.getString("akka.persistence.journal.controlled-in-mem.instance-id")
  def journalProbe = ControlledInmemJournal.getProbe(journalInstanceId)

  @annotation.tailrec
  final def persistentActorCongruentWith(n: Int): ActorRef = {
    require(n < numResequencers && n >= 0)

    val spawned = system.actorOf(persistentActorProps)

    if (((spawned.hashCode & 0x7FFFFFFF) % numResequencers) == n) spawned
    else {
      watch(spawned)
      spawned ! "cull"
      expectTerminated(spawned)
      persistentActorCongruentWith(n)
    }
  }

  // It doesn't particularly matter which group has multiple...
  val groupWithTwo = Random.nextInt(numResequencers)
  val groups = (0 until numResequencers).iterator.map { group =>
    (1 to (if (group == groupWithTwo) 2 else 1)).map(_ => persistentActorCongruentWith(group))
  }.toSeq

  val pidToGroup = Future
    .sequence(groups.iterator.zipWithIndex.flatMap {
      case (group, idx) =>
        group.iterator.map { actor =>
          implicit val timeout: Timeout = 1.second
          (actor ? "get-persistence-id")
            .mapTo[String]
            .map { pid =>
              pid -> (idx -> actor)
            }(ExecutionContext.parasitic)
        }
    }.toSeq)
    .futureValue
    .toMap

  s"A journal with $numResequencers write-reply ordering group(s)" must {
    (0 to numResequencers).toList.permutations.foreach { ordering =>
      s"must properly order write replies (ordering $ordering)" in {
        // flood the zone with persists
        val asks = pidToGroup.iterator.map {
          case (pid, (groupId, actor)) =>
            // could have done an ask, but then would have to adjust the timeout based on the number
            // of resequencers
            val p = Promise[Done]()
            actor ! ("persist" -> p)
            pid -> (groupId -> p.future)
        }.toMap

        val attempts = journalProbe
          .receiveN(1 + numResequencers)
          .map {
            case ControlledInmemJournal.WriteMessagesAttempt(messages, promise) =>
              messages.size shouldBe 1
              val pid = messages.head.persistenceId
              pid -> promise

            case _ => fail("unexpected message on journal probe")
          }
          .toVector

        val attemptsFromGroupWithTwo = attempts.iterator.zipWithIndex.collect {
          case ((pid, _), idx) if (pidToGroup(pid)._1 == groupWithTwo) => idx
        }.toSeq

        attemptsFromGroupWithTwo.size shouldBe 2

        val reversedCompletion = {
          (ordering.indexOf(attemptsFromGroupWithTwo.head), ordering.indexOf(attemptsFromGroupWithTwo(1))) match {
            case (first, second) if (first < second) => false
            case _                                   => true
          }
        }

        val (remainingAsks, waiting) = ordering.foldLeft(asks -> false) {
          case ((remainingAsks, waiting), toComplete) =>
            val (pidCompleting, promise) = attempts(toComplete)

            // complete that persist
            promise.success(oneSuccessUnit)

            // no asks in other groups should be completed
            remainingAsks.valuesIterator.foreach {
              case (group, fut) =>
                if (group != pidToGroup(pidCompleting)._1) {
                  assert(!fut.isCompleted, "futures in other groups should not be completed")
                }
            }

            if (pidToGroup(pidCompleting)._1 != groupWithTwo) {
              // should definitely complete the ask for this pid, since it's the only one in the group
              remainingAsks(pidCompleting)._2.futureValue shouldBe Done
              remainingAsks.removed(pidCompleting) -> waiting
            } else {
              if (attemptsFromGroupWithTwo.head == toComplete) {
                // first one from group with two to hit journal, so nothing blocking it
                remainingAsks(pidCompleting)._2.futureValue shouldBe Done
                if (waiting) {
                  val (waitingPid, _) = attempts(attemptsFromGroupWithTwo(1))
                  remainingAsks(waitingPid)._2.futureValue shouldBe Done
                  remainingAsks.removedAll(Seq(pidCompleting, waitingPid)) -> false
                } else remainingAsks.removed(pidCompleting) -> false
              } else {
                // this is the second one from that group to hit the journal...
                val firstPid = attempts(attemptsFromGroupWithTwo.head)._1
                if (reversedCompletion) {
                  // ...and it's blocked on the not completed first
                  a[TimeoutException] shouldBe thrownBy { Await.ready(remainingAsks(pidCompleting)._2, 10.millis) }
                  a[TimeoutException] shouldBe thrownBy { Await.ready(remainingAsks(firstPid)._2, 10.millis) }
                  remainingAsks -> true
                } else {
                  remainingAsks(pidCompleting)._2.futureValue shouldBe Done
                  remainingAsks.get(firstPid) shouldBe empty
                  remainingAsks.removed(pidCompleting) -> false
                }
              }
            }
        }

        remainingAsks shouldBe empty
        waiting shouldBe false
      }
    }
  }
}

class OneResequencerSpec extends JournalReplyOrderingSpec("one", 1)
class TwoResequencerSpec extends JournalReplyOrderingSpec("two", 2)
class ThreeResequencerSpec extends JournalReplyOrderingSpec("three", 3)
class FourResequencerSpec extends JournalReplyOrderingSpec("four", 4)
class FiveResequencerSpec extends JournalReplyOrderingSpec("five", 5)
