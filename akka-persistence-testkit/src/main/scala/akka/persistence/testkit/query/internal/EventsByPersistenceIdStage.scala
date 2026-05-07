/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.query.internal
import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.persistence.journal.Tagged
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.testkit.EventStorage
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageLogicWithLogging
import akka.stream.stage.OutHandler

/**
 * INTERNAL API
 */
@InternalApi
final private[akka] class EventsByPersistenceIdStage[Event](
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    storage: EventStorage,
    sliceForPid: String => Int)
    extends GraphStage[SourceShape[EventEnvelope[Event]]] {
  val out: Outlet[EventEnvelope[Event]] = Outlet("EventsByPersistenceIdSource")
  override def shape: SourceShape[EventEnvelope[Event]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogicWithLogging(shape) with OutHandler {
      private var currentSequenceNr = math.max(fromSequenceNr, 1)
      private var stageActorRef: ActorRef = null
      override def preStart(): Unit = {
        stageActorRef = getStageActor(receiveNotifications).ref
        materializer.system.eventStream.subscribe(stageActorRef, classOf[PersistenceTestKitPlugin.Write])
      }

      private def receiveNotifications(in: (ActorRef, Any)): Unit = {
        val (_, msg) = in
        msg match {
          case PersistenceTestKitPlugin.Write(pid, toSequenceNr) if pid == persistenceId =>
            if (toSequenceNr >= currentSequenceNr) {
              tryPush()
            }
          case _ =>
        }
      }

      private def tryPush(): Unit = {
        if (isAvailable(out)) {
          val event = storage.tryRead(persistenceId, currentSequenceNr, currentSequenceNr, 1)
          log.debug("tryPush available. Query for {} {} result {}", currentSequenceNr, currentSequenceNr, event)
          event.headOption match {
            case Some(pr) =>
              val entityType = PersistenceId.extractEntityType(pr.persistenceId)
              val unwrappedPayload: Option[Event] = Some(pr.payload match {
                case Tagged(payload, _) => payload.asInstanceOf[Event]
                case payload            => payload.asInstanceOf[Event]
              })
              val envelope =
                new EventEnvelope[Event](
                  offset = PersistenceTestKitReadJournal.timestampOffsetFor(pr),
                  persistenceId = pr.persistenceId,
                  sequenceNr = pr.sequenceNr,
                  eventOption = unwrappedPayload,
                  timestamp = pr.timestamp,
                  _eventMetadata = pr.metadata,
                  entityType = entityType,
                  slice = sliceForPid(pr.persistenceId),
                  filtered = false,
                  source = "",
                  tags = PersistenceTestKitReadJournal.tagsFor(pr.payload))
              push(out, envelope)
              if (currentSequenceNr == toSequenceNr) {
                completeStage()
              } else {
                currentSequenceNr += 1
              }
            case None =>
          }
        } else {
          log.debug("tryPush, no demand")
        }
      }

      override def onPull(): Unit = {
        tryPush()
      }

      setHandler(out, this)
    }

  }

}
