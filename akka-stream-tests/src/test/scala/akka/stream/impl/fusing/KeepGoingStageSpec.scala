/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.actor.NoSerializationVerificationNeeded
import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.SinkShape
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils._

class KeepGoingStageSpec extends StreamSpec {

  sealed trait PingCmd extends NoSerializationVerificationNeeded
  case class Register(probe: ActorRef) extends PingCmd
  case object Ping extends PingCmd
  case object CompleteStage extends PingCmd
  case object FailStage extends PingCmd
  case object Throw extends PingCmd

  sealed trait PingEvt extends NoSerializationVerificationNeeded
  case object Pong extends PingEvt
  case object PostStop extends PingEvt
  case object UpstreamCompleted extends PingEvt
  case object EndOfEventHandler extends PingEvt

  case class PingRef(private val cb: AsyncCallback[PingCmd]) {
    def register(probe: ActorRef): Unit = cb.invoke(Register(probe))
    def ping(): Unit = cb.invoke(Ping)
    def stop(): Unit = cb.invoke(CompleteStage)
    def fail(): Unit = cb.invoke(FailStage)
    def throwEx(): Unit = cb.invoke(Throw)
  }

  class PingableSink(keepAlive: Boolean) extends GraphStageWithMaterializedValue[SinkShape[Int], Future[PingRef]] {
    val shape = SinkShape[Int](Inlet("ping.in"))

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes): (GraphStageLogic, Future[PingRef]) = {
      val promise = Promise[PingRef]()

      val logic = new GraphStageLogic(shape) {
        private var listener: Option[ActorRef] = None

        override def preStart(): Unit = {
          setKeepGoing(keepAlive)
          promise.trySuccess(PingRef(getAsyncCallback(onCommand)))
        }

        private def onCommand(cmd: PingCmd): Unit = cmd match {
          case Register(probe) => listener = Some(probe)
          case Ping            => listener.foreach(_ ! Pong)
          case CompleteStage =>
            completeStage()
            listener.foreach(_ ! EndOfEventHandler)
          case FailStage =>
            failStage(TE("test"))
            listener.foreach(_ ! EndOfEventHandler)
          case Throw =>
            try {
              throw TE("test")
            } finally listener.foreach(_ ! EndOfEventHandler)
        }

        setHandler(shape.in, new InHandler {
          override def onPush(): Unit = pull(shape.in)

          // Ignore finish
          override def onUpstreamFinish(): Unit = listener.foreach(_ ! UpstreamCompleted)
        })

        override def postStop(): Unit = listener.foreach(_ ! PostStop)
      }

      (logic, promise.future)
    }
  }

  "A stage with keep-going" must {

    "still be alive after all ports have been closed until explicitly closed" in {
      val (maybePromise, pingerFuture) = Source.maybe[Int].toMat(new PingableSink(keepAlive = true))(Keep.both).run()
      val pinger = Await.result(pingerFuture, 3.seconds)

      pinger.register(testActor)

      // Before completion
      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      maybePromise.trySuccess(None)
      expectMsg(UpstreamCompleted)

      expectNoMessage(200.millis)

      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      pinger.stop()
      // PostStop should not be concurrent with the event handler. This event here tests this.
      expectMsg(EndOfEventHandler)
      expectMsg(PostStop)

    }

    "still be alive after all ports have been closed until explicitly failed" in {
      val (maybePromise, pingerFuture) = Source.maybe[Int].toMat(new PingableSink(keepAlive = true))(Keep.both).run()
      val pinger = Await.result(pingerFuture, 3.seconds)

      pinger.register(testActor)

      // Before completion
      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      maybePromise.trySuccess(None)
      expectMsg(UpstreamCompleted)

      expectNoMessage(200.millis)

      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      pinger.fail()
      // PostStop should not be concurrent with the event handler. This event here tests this.
      expectMsg(EndOfEventHandler)
      expectMsg(PostStop)

    }

    "still be alive after all ports have been closed until implicitly failed (via exception)" in {
      val (maybePromise, pingerFuture) = Source.maybe[Int].toMat(new PingableSink(keepAlive = true))(Keep.both).run()
      val pinger = Await.result(pingerFuture, 3.seconds)

      pinger.register(testActor)

      // Before completion
      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      maybePromise.trySuccess(None)
      expectMsg(UpstreamCompleted)

      expectNoMessage(200.millis)

      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      pinger.throwEx()
      // PostStop should not be concurrent with the event handler. This event here tests this.
      expectMsg(EndOfEventHandler)
      expectMsg(PostStop)

    }

    "close down early if keepAlive is not requested" in {
      val (maybePromise, pingerFuture) = Source.maybe[Int].toMat(new PingableSink(keepAlive = false))(Keep.both).run()
      val pinger = Await.result(pingerFuture, 3.seconds)

      pinger.register(testActor)

      // Before completion
      pinger.ping()
      expectMsg(Pong)

      pinger.ping()
      expectMsg(Pong)

      maybePromise.trySuccess(None)
      expectMsg(UpstreamCompleted)
      expectMsg(PostStop)

    }

  }

}
