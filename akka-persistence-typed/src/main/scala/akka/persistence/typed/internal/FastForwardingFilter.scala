/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.annotation.InternalApi
import akka.persistence.query.EventEnvelope
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait ReplicationStreamControl {
  def fastForward(sequenceNumber: Long): Unit
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class FastForwardingFilter
    extends GraphStageWithMaterializedValue[FlowShape[EventEnvelope, EventEnvelope], ReplicationStreamControl] {

  val in = Inlet[EventEnvelope]("FastForwardingFilter.in")
  val out = Outlet[EventEnvelope]("FastForwardingFilter.out")

  override val shape = FlowShape[EventEnvelope, EventEnvelope](in, out)

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, ReplicationStreamControl) = {
    var replicationStreamControl: ReplicationStreamControl = null
    val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
      // -1 means not currently fast forwarding
      @volatile private var fastForwardTo = -1L

      override def onPush(): Unit = {
        val eventEnvelope = grab(in)
        if (fastForwardTo == -1L)
          push(out, eventEnvelope)
        else {
          if (eventEnvelope.sequenceNr <= fastForwardTo) pull(in)
          else {
            fastForwardTo = -1L
            push(out, eventEnvelope)
          }
        }
      }
      override def onPull(): Unit = pull(in)

      replicationStreamControl = new ReplicationStreamControl {
        override def fastForward(sequenceNumber: Long): Unit = {
          require(sequenceNumber > 0) // only the stage may complete a fast forward
          fastForwardTo = sequenceNumber
        }
      }

      setHandlers(in, out, this)
    }

    (logic, replicationStreamControl)
  }

}
