/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal
import akka.actor._
import akka.pattern.CircuitBreakersRegistry
import akka.pattern.pipe
import akka.persistence._
import akka.util.Helpers.toRootLowerCase

/**
 * Abstract journal, optimized for asynchronous, non-blocking writes.
 */
trait AsyncWriteJournal extends Actor with WriteJournalBase with AsyncRecovery {
  import AsyncWriteJournal._
  import JournalProtocol._

  private val extension = Persistence(context.system)
  private val publish = extension.settings.internal.publishPluginCommands
  private val config = extension.configFor(self)

  private val breaker = {
    val id = extension.extensionIdFor(self)
    CircuitBreakersRegistry(context.system).getOrCreate(id, config.getConfig("circuit-breaker"))
  }

  private val replayFilterMode: ReplayFilter.Mode =
    toRootLowerCase(config.getString("replay-filter.mode")) match {
      case "off"                   => ReplayFilter.Disabled
      case "repair-by-discard-old" => ReplayFilter.RepairByDiscardOld
      case "fail"                  => ReplayFilter.Fail
      case "warn"                  => ReplayFilter.Warn
      case other =>
        throw new IllegalArgumentException(
          s"invalid replay-filter.mode [$other], supported values [off, repair-by-discard-old, fail, warn]")
    }
  private def isReplayFilterEnabled: Boolean = replayFilterMode != ReplayFilter.Disabled
  private val replayFilterWindowSize: Int = config.getInt("replay-filter.window-size")
  private val replayFilterMaxOldWriters: Int = config.getInt("replay-filter.max-old-writers")

  private val resequencer = context.actorOf(Props(new Resequencer))
  private var resequencerCounter = 1L

  final def receive = receiveWriteJournal.orElse[Any, Unit](receivePluginInternal)

  final val receiveWriteJournal: Actor.Receive = {
    // cannot be a val in the trait due to binary compatibility
    val replayDebugEnabled: Boolean = config.getBoolean("replay-filter.debug")
    val eventStream = context.system.eventStream // used from Future callbacks
    implicit val ec: ExecutionContext = context.dispatcher

    {
      case WriteMessages(messages, persistentActor, actorInstanceId, bypassCircuitBreaker) =>
        val cctr = resequencerCounter
        resequencerCounter += messages.foldLeft(1)((acc, m) => acc + m.size)

        val atomicWriteCount = messages.count(_.isInstanceOf[AtomicWrite])
        val prepared = Try(preparePersistentBatch(messages))
        val writeResult = (prepared match {
          case Success(prep) if prep.isEmpty =>
            // prep is empty when all messages are instances of NonPersistentRepr (used for defer) in that case,
            // we continue right away without calling the journal plugin (most plugins fail calling head on empty Seq).
            // Ordering of the replies is handled by Resequencer
            Future.successful(Nil)
          case Success(prep) =>
            // try in case the asyncWriteMessages throws
            try {
              if (bypassCircuitBreaker) asyncWriteMessages(prep)
              else breaker.withCircuitBreaker(asyncWriteMessages(prep))
            } catch { case NonFatal(e) => Future.failed(e) }
          case f @ Failure(_) =>
            // exception from preparePersistentBatch => rejected
            Future.successful(messages.collect { case _: AtomicWrite => f })
        }).map { results =>
          if (results.nonEmpty && results.size != atomicWriteCount)
            throw new IllegalStateException(
              "asyncWriteMessages returned invalid number of results. " +
              s"Expected [${prepared.get.size}], but got [${results.size}]")
          results
        }

        writeResult.onComplete {
          case Success(results) =>
            resequencer ! Desequenced(WriteMessagesSuccessful, cctr, persistentActor, self)

            val resultsIter =
              if (results.isEmpty) Iterator.fill(atomicWriteCount)(AsyncWriteJournal.successUnit)
              else results.iterator
            var n = cctr + 1
            messages.foreach {
              case a: AtomicWrite =>
                resultsIter.next() match {
                  case Success(_) =>
                    a.payload.foreach { p =>
                      resequencer ! Desequenced(WriteMessageSuccess(p, actorInstanceId), n, persistentActor, p.sender)
                      n += 1
                    }
                  case Failure(e) =>
                    a.payload.foreach { p =>
                      resequencer ! Desequenced(
                        WriteMessageRejected(p, e, actorInstanceId),
                        n,
                        persistentActor,
                        p.sender)
                      n += 1
                    }
                }

              case r: NonPersistentRepr =>
                resequencer ! Desequenced(LoopMessageSuccess(r.payload, actorInstanceId), n, persistentActor, r.sender)
                n += 1
            }

          case Failure(e) =>
            resequencer ! Desequenced(WriteMessagesFailed(e, atomicWriteCount), cctr, persistentActor, self)
            var n = cctr + 1
            messages.foreach {
              case a: AtomicWrite =>
                a.payload.foreach { p =>
                  resequencer ! Desequenced(WriteMessageFailure(p, e, actorInstanceId), n, persistentActor, p.sender)
                  n += 1
                }
              case r: NonPersistentRepr =>
                resequencer ! Desequenced(LoopMessageSuccess(r.payload, actorInstanceId), n, persistentActor, r.sender)
                n += 1
            }
        }

      case r @ ReplayMessages(fromSequenceNr, toSequenceNr, max, persistenceId, persistentActor) =>
        val replyTo =
          if (isReplayFilterEnabled)
            context.actorOf(
              ReplayFilter.props(
                persistentActor,
                replayFilterMode,
                replayFilterWindowSize,
                replayFilterMaxOldWriters,
                replayDebugEnabled))
          else persistentActor

        val replay =
          this match {
            case asyncReplay: AsyncReplay =>
              asyncReplay.replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max) { p =>
                adaptFromJournal(p).foreach { adaptedPersistentRepr =>
                  replyTo.tell(ReplayedMessage(adaptedPersistentRepr), Actor.noSender)
                }
              }

            case _ =>
              val readHighestSequenceNrFrom = math.max(0L, fromSequenceNr - 1)
              /*
               * The API docs for the [[AsyncRecovery]] say not to rely on asyncReadHighestSequenceNr
               * being called before a call to asyncReplayMessages even tho it currently always is. The Cassandra
               * plugin does rely on this so if you change this change the Cassandra plugin.
               */
              breaker.withCircuitBreaker(asyncReadHighestSequenceNr(persistenceId, readHighestSequenceNrFrom)).flatMap {
                highSeqNr =>
                  val toSeqNr = math.min(toSequenceNr, highSeqNr)
                  if (toSeqNr <= 0L || fromSequenceNr > toSeqNr)
                    Future.successful(highSeqNr)
                  else {
                    // Send replayed messages and replay result to persistentActor directly. No need
                    // to resequence replayed messages relative to written and looped messages.
                    // not possible to use circuit breaker here
                    asyncReplayMessages(persistenceId, fromSequenceNr, toSeqNr, max) { p =>
                      if (!p.deleted) // old records from 2.3 may still have the deleted flag
                        adaptFromJournal(p).foreach { adaptedPersistentRepr =>
                          replyTo.tell(ReplayedMessage(adaptedPersistentRepr), Actor.noSender)
                        }
                    }.map(_ => highSeqNr)(ExecutionContext.parasitic)
                  }
              }
          }

        replay
          .map { highSeqNr =>
            RecoverySuccess(highSeqNr)
          }(ExecutionContext.parasitic)
          .recover {
            case e => ReplayMessagesFailure(e)
          }
          .pipeTo(replyTo)
          .foreach { _ =>
            if (publish) eventStream.publish(r)
          }

      case d @ DeleteMessagesTo(persistenceId, toSequenceNr, persistentActor) =>
        breaker
          .withCircuitBreaker(asyncDeleteMessagesTo(persistenceId, toSequenceNr))
          .map { _ =>
            DeleteMessagesSuccess(toSequenceNr)
          }
          .recover {
            case e => DeleteMessagesFailure(e, toSequenceNr)
          }
          .pipeTo(persistentActor)
          .onComplete { _ =>
            if (publish) eventStream.publish(d)
          }
    }
  }

  //#journal-plugin-api
  /**
   * Plugin API: asynchronously writes a batch (`Seq`) of persistent messages to the
   * journal.
   *
   * The batch is only for performance reasons, i.e. all messages don't have to be written
   * atomically. Higher throughput can typically be achieved by using batch inserts of many
   * records compared to inserting records one-by-one, but this aspect depends on the
   * underlying data store and a journal implementation can implement it as efficient as
   * possible. Journals should aim to persist events in-order for a given `persistenceId`
   * as otherwise in case of a failure, the persistent state may be end up being inconsistent.
   *
   * Each `AtomicWrite` message contains the single `PersistentRepr` that corresponds to
   * the event that was passed to the `persist` method of the `PersistentActor`, or it
   * contains several `PersistentRepr` that corresponds to the events that were passed
   * to the `persistAll` method of the `PersistentActor`. All `PersistentRepr` of the
   * `AtomicWrite` must be written to the data store atomically, i.e. all or none must
   * be stored. If the journal (data store) cannot support atomic writes of multiple
   * events it should reject such writes with a `Try` `Failure` with an
   * `UnsupportedOperationException` describing the issue. This limitation should
   * also be documented by the journal plugin.
   *
   * If there are failures when storing any of the messages in the batch the returned
   * `Future` must be completed with failure. The `Future` must only be completed with
   * success when all messages in the batch have been confirmed to be stored successfully,
   * i.e. they will be readable, and visible, in a subsequent replay. If there is
   * uncertainty about if the messages were stored or not the `Future` must be completed
   * with failure.
   *
   * Data store connection problems must be signaled by completing the `Future` with
   * failure.
   *
   * The journal can also signal that it rejects individual messages (`AtomicWrite`) by
   * the returned `immutable.Seq[Try[Unit]]`. It is possible but not mandatory to reduce
   * number of allocations by returning `Future.successful(Nil)` for the happy path,
   * i.e. when no messages are rejected. Otherwise the returned `Seq` must have as many elements
   * as the input `messages` `Seq`. Each `Try` element signals if the corresponding
   * `AtomicWrite` is rejected or not, with an exception describing the problem. Rejecting
   * a message means it was not stored, i.e. it must not be included in a later replay.
   * Rejecting a message is typically done before attempting to store it, e.g. because of
   * serialization error.
   *
   * Data store connection problems must not be signaled as rejections.
   *
   * It is possible but not mandatory to reduce number of allocations by returning
   * `Future.successful(Nil)` for the happy path, i.e. when no messages are rejected.
   *
   * Calls to this method are serialized by the enclosing journal actor. If you spawn
   * work in asynchronous tasks it is alright that they complete the futures in any order,
   * but the actual writes for a specific persistenceId should be serialized to avoid
   * issues such as events of a later write are visible to consumers (query side, or replay)
   * before the events of an earlier write are visible.
   * A PersistentActor will not send a new WriteMessages request before the previous one
   * has been completed.
   *
   * Please note that the `sender` field of the contained PersistentRepr objects has been
   * nulled out (i.e. set to `ActorRef.noSender`) in order to not use space in the journal
   * for a sender reference that will likely be obsolete during replay.
   *
   * Please also note that requests for the highest sequence number may be made concurrently
   * to this call executing for the same `persistenceId`, in particular it is possible that
   * a restarting actor tries to recover before its outstanding writes have completed. In
   * the latter case it is highly desirable to defer reading the highest sequence number
   * until all outstanding writes have completed, otherwise the PersistentActor may reuse
   * sequence numbers.
   *
   * This call is protected with a circuit-breaker.
   */
  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]]

  /**
   * Plugin API: asynchronously deletes all persistent messages up to `toSequenceNr`
   * (inclusive).
   *
   * This call is protected with a circuit-breaker.
   * Message deletion doesn't affect the highest sequence number of messages,
   * journal must maintain the highest sequence number and never decrease it.
   */
  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit]

  /**
   * Plugin API
   *
   * Allows plugin implementers to use `f pipeTo self` and
   * handle additional messages for implementing advanced features
   *
   */
  def receivePluginInternal: Actor.Receive = Actor.emptyBehavior
  //#journal-plugin-api

}

/**
 * INTERNAL API.
 */
private[persistence] object AsyncWriteJournal {
  val successUnit: Success[Unit] = Success(())

  final case class Desequenced(msg: Any, snr: Long, target: ActorRef, sender: ActorRef)
      extends NoSerializationVerificationNeeded

  class Resequencer extends Actor {
    import scala.collection.mutable.Map

    private val delayed = Map.empty[Long, Desequenced]
    private var delivered = 0L

    def receive = {
      case d: Desequenced => resequence(d)
    }

    @scala.annotation.tailrec
    private def resequence(d: Desequenced): Unit = {
      if (d.snr == delivered + 1) {
        delivered = d.snr
        d.target.tell(d.msg, d.sender)
      } else {
        delayed += (d.snr -> d)
      }
      val ro = delayed.remove(delivered + 1)
      if (ro.isDefined) resequence(ro.get)
    }
  }
}
