/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit

import scala.util.control.NonFatal

import akka.actor.NoSerializationVerificationNeeded
import akka.stream._
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.stage.{ GraphStageWithMaterializedValue, InHandler, OutHandler }
import akka.testkit.TestProbe

/**
 * Messages emitted after the corresponding `stageUnderTest` methods has been invoked.
 */
object GraphStageMessages {
  sealed trait StageMessage
  case object Push extends StageMessage with NoSerializationVerificationNeeded
  case object UpstreamFinish extends StageMessage with NoSerializationVerificationNeeded
  case class Failure(ex: Throwable) extends StageMessage with NoSerializationVerificationNeeded

  case object Pull extends StageMessage with NoSerializationVerificationNeeded
  case object DownstreamFinish extends StageMessage with NoSerializationVerificationNeeded

  /**
   * Sent to the probe when the operator callback threw an exception
   * @param operation The operation that failed
   */
  case class StageFailure(operation: StageMessage, exception: Throwable)
}

object TestSinkStage {

  /**
   * Creates a sink out of the `stageUnderTest` that will inform the `probe`
   * of operator events and callbacks by sending it the various messages found under
   * [[GraphStageMessages]].
   *
   * This allows for creation of a "normal" stream ending with the sink while still being
   * able to assert internal events.
   */
  def apply[T, M](stageUnderTest: GraphStageWithMaterializedValue[SinkShape[T], M], probe: TestProbe): Sink[T, M] =
    Sink.fromGraph(new TestSinkStage(stageUnderTest, probe))
}

private[testkit] class TestSinkStage[T, M](
    stageUnderTest: GraphStageWithMaterializedValue[SinkShape[T], M],
    probe: TestProbe)
    extends GraphStageWithMaterializedValue[SinkShape[T], M] {

  val in = Inlet[T]("testSinkStage.in")
  override val shape: SinkShape[T] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    stageUnderTest.shape.in.id = in.id
    val (logic, mat) = stageUnderTest.createLogicAndMaterializedValue(inheritedAttributes)

    val inHandler = logic.handlers(in.id).asInstanceOf[InHandler]
    logic.handlers(in.id) = new InHandler {
      override def onPush(): Unit = {
        try {
          inHandler.onPush()
          probe.ref ! GraphStageMessages.Push
        } catch {
          case NonFatal(ex) =>
            probe.ref ! GraphStageMessages.StageFailure(GraphStageMessages.Push, ex)
            throw ex
        }
      }
      override def onUpstreamFinish(): Unit = {
        try {
          inHandler.onUpstreamFinish()
          probe.ref ! GraphStageMessages.UpstreamFinish
        } catch {
          case NonFatal(ex) =>
            probe.ref ! GraphStageMessages.StageFailure(GraphStageMessages.UpstreamFinish, ex)
            throw ex
        }

      }
      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          inHandler.onUpstreamFailure(ex)
          probe.ref ! GraphStageMessages.Failure(ex)
        } catch {
          case NonFatal(ex) =>
            probe.ref ! GraphStageMessages.StageFailure(GraphStageMessages.Failure(ex), ex)
            throw ex
        }
      }
    }
    (logic, mat)
  }
}

object TestSourceStage {

  /**
   * Creates a source out of the `stageUnderTest` that will inform the `probe`
   * of operator events and callbacks by sending it the various messages found under
   * [[GraphStageMessages]].
   *
   * This allows for creation of a "normal" stream starting with the source while still being
   * able to assert internal events.
   */
  def apply[T, M](stageUnderTest: GraphStageWithMaterializedValue[SourceShape[T], M], probe: TestProbe): Source[T, M] =
    Source.fromGraph(new TestSourceStage(stageUnderTest, probe))
}

private[testkit] class TestSourceStage[T, M](
    stageUnderTest: GraphStageWithMaterializedValue[SourceShape[T], M],
    probe: TestProbe)
    extends GraphStageWithMaterializedValue[SourceShape[T], M] {

  val out = Outlet[T]("testSourceStage.out")
  override val shape: SourceShape[T] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    stageUnderTest.shape.out.id = out.id
    val (logic, mat) = stageUnderTest.createLogicAndMaterializedValue(inheritedAttributes)

    val outHandler = logic.handlers(out.id).asInstanceOf[OutHandler]
    logic.handlers(out.id) = new OutHandler {
      override def onPull(): Unit = {
        try {
          outHandler.onPull()
          probe.ref ! GraphStageMessages.Pull
        } catch {
          case NonFatal(ex) =>
            probe.ref ! GraphStageMessages.StageFailure(GraphStageMessages.Pull, ex)
            throw ex
        }
      }
      override def onDownstreamFinish(cause: Throwable): Unit = {
        try {
          outHandler.onDownstreamFinish(cause)
          probe.ref ! GraphStageMessages.DownstreamFinish
        } catch {
          case NonFatal(ex) =>
            probe.ref ! GraphStageMessages.StageFailure(GraphStageMessages.DownstreamFinish, ex)
            throw ex
        }
      }
    }
    (logic, mat)
  }
}
