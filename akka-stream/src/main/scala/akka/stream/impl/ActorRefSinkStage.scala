/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.actor.{ ActorRef, Terminated }
import akka.annotation.InternalApi
import akka.stream.{ AbruptStageTerminationException, Attributes, Inlet, SinkShape }
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, StageLogging }

/**
 * INTERNAL API
 */
@InternalApi
final private[akka] class ActorRefSinkStage[T](
    ref: ActorRef,
    onCompleteMessage: Any,
    onFailureMessage: Throwable => Any)
    extends GraphStage[SinkShape[T]] {

  val in: Inlet[T] = Inlet("ActorRefSink.in")

  override def shape: SinkShape[T] = SinkShape(in)

  override protected def initialAttributes: Attributes = DefaultAttributes.actorRefSink

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with StageLogging {

      override protected def logSource: Class[_] = classOf[ActorRefSinkStage[_]]

      var completionSignalled = false

      override def preStart(): Unit = {
        getStageActor({
          case (_, Terminated(`ref`)) =>
            completeStage()
          case msg =>
            log.error("Unexpected message to stage actor {}", msg.getClass)
        }).watch(ref)
        pull(in)
      }

      override def onPush(): Unit = {
        val next = grab(in)
        ref.tell(next, ActorRef.noSender)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completionSignalled = true
        ref.tell(onCompleteMessage, ActorRef.noSender)
        completeStage()
      }

      setHandler(in, this)

      override def onUpstreamFailure(ex: Throwable): Unit = {
        completionSignalled = true
        ref.tell(onFailureMessage(ex), ActorRef.noSender)
        failStage(ex)
      }

      override def postStop(): Unit = {
        if (!completionSignalled) {
          ref ! onFailureMessage(new AbruptStageTerminationException(this))
        }
      }
    }
}
