/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.concurrent.duration._

import akka.annotation.InternalApi
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class AggregateWithBoundary[In, Agg, Out](
    allocate: () => Agg,
    aggregate: (Agg, In) => (Agg, Boolean),
    harvest: Agg => Out,
    emitOnTimer: Option[(Agg => Boolean, FiniteDuration)])
    extends GraphStage[FlowShape[In, Out]] {

  emitOnTimer.foreach {
    case (_, interval) => require(interval.gteq(1.milli), s"timer(${interval.toCoarsest}) must not be smaller than 1ms")
  }

  val in: Inlet[In] = Inlet[In](s"${this.getClass.getName}.in")
  val out: Outlet[Out] = Outlet[Out](s"${this.getClass.getName}.out")
  override val shape: FlowShape[In, Out] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      private[this] var aggregated: Agg = null.asInstanceOf[Agg]

      override def preStart(): Unit = {
        emitOnTimer.foreach {
          case (_, interval) => scheduleWithFixedDelay(s"${this.getClass.getSimpleName}Timer", interval, interval)
        }
      }

      override protected def onTimer(timerKey: Any): Unit = {
        emitOnTimer.foreach {
          case (isReadyOnTimer, _) => if (aggregated != null && isReadyOnTimer(aggregated)) harvestAndEmit()
        }
      }

      // at onPush, isAvailable(in)=true hasBeenPulled(in)=false, isAvailable(out) could be true or false due to timer triggered emit
      override def onPush(): Unit = {
        if (aggregated == null) aggregated = allocate()
        val (updated, result) = aggregate(aggregated, grab(in))
        aggregated = updated
        if (result) harvestAndEmit()
        // the decision to pull entirely depend on isAvailable(out)=true, regardless of result of aggregate
        // 1. aggregate=true: isAvailable(out) will be false
        // 2. aggregate=false: if isAvailable(out)=false, this means timer has caused emit, cannot pull or it could emit indefinitely bypassing back pressure
        if (isAvailable(out)) pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        // Note that emit is asynchronous, it will keep the stage alive until downstream actually take the element
        if (aggregated != null) emit(out, harvest(aggregated))
        completeStage()
      }

      // at onPull, isAvailable(out) is always true indicating downstream is waiting
      // isAvailable(in) and hasBeenPulled(in) can be (true, false) (false, true) or (false, false)
      override def onPull(): Unit = if (!hasBeenPulled(in)) pull(in)

      setHandlers(in, out, this)

      private def harvestAndEmit(): Unit = {
        emit(out, harvest(aggregated))
        aggregated = null.asInstanceOf[Agg]
      }

    }

}
