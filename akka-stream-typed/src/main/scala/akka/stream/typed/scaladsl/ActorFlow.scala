/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.scaladsl

import scala.annotation.implicitNotFound
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.pattern.{ AskTimeoutException, StatusReply }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout

/**
 * Collection of Flows aimed at integrating with typed Actors.
 */
object ActorFlow {

  // TODO would be nice to provide Implicits to allow .ask() directly on Flow/Source

  private def askImpl[I, Q, A, O](parallelism: Int)(ref: ActorRef[Q])(
      makeMessage: (I, ActorRef[A]) => Q,
      makeOut: (I, Future[A]) => Future[O])(implicit timeout: Timeout): Flow[I, O, NotUsed] = {
    import akka.actor.typed.scaladsl.adapter._
    val classicRef = ref.toClassic

    val askFlow = Flow[I]
      .watch(classicRef)
      .mapAsync(parallelism) { el =>
        val res = akka.pattern.extended.ask(classicRef, (replyTo: akka.actor.ActorRef) => makeMessage(el, replyTo))
        // we need to cast manually (yet safely, by construction!) since otherwise we need a ClassTag,
        // which in Scala is fine, but then we would force JavaDSL to create one, which is a hassle in the Akka Typed DSL,
        // since one may say "but I already specified the type!", and that we have to go via the classic ask is an implementation detail
        makeOut(el, res.asInstanceOf[Future[A]])
      }
      .mapError {
        case ex: AskTimeoutException =>
          // in Akka Typed we use the `TimeoutException` everywhere
          new java.util.concurrent.TimeoutException(ex.getMessage)

        // the purpose of this recovery is to change the name of the stage in that exception
        // we do so in order to help users find which stage caused the failure -- "the ask stage"
        case ex: WatchedActorTerminatedException =>
          new WatchedActorTerminatedException("ask()", ex.ref)
      }
      .named("ask")

    askFlow
  }

  /**
   * Use the `ask` pattern to send a request-reply message to the target `ref` actor.
   * If any of the asks times out it will fail the stream with a [[java.util.concurrent.TimeoutException]].
   *
   * Do not forget to include the expected response type in the method call, like so:
   *
   * {{{
   * flow.via(ActorFlow.ask[String, Asking, Reply](ref)((el, replyTo) => Asking(el, replyTo)))
   *
   * // or even:
   * flow.via(ActorFlow.ask[String, Asking, Reply](ref)(Asking(_, _)))
   * }}}
   *
   * otherwise `Nothing` will be assumed, which is most likely not what you want.
   *
   * Defaults to parallelism of 2 messages in flight, since while one ask message may be being worked on, the second one
   * still be in the mailbox, so defaulting to sending the second one a bit earlier than when first ask has replied maintains
   * a slightly healthier throughput.
   *
   * The operator fails with an [[akka.stream.WatchedActorTerminatedException]] if the target actor is terminated,
   * or with an [[java.util.concurrent.TimeoutException]] in case the ask exceeds the timeout passed in.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the futures (in submission order) created by the ask pattern internally are completed
   *
   * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream backpressures
   *
   * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted
   *
   * '''Fails when''' the passed in actor terminates, or a timeout is exceeded in any of the asks performed
   *
   * '''Cancels when''' downstream cancels
   *
   * @tparam I Incoming element type of the Flow
   * @tparam Q Question message type that is spoken by the target actor
   * @tparam A Answer type that the Actor is expected to reply with, it will become the Output type of this Flow
   */
  @implicitNotFound("Missing an implicit akka.util.Timeout for the ask() stage")
  def ask[I, Q, A](ref: ActorRef[Q])(makeMessage: (I, ActorRef[A]) => Q)(
      implicit timeout: Timeout): Flow[I, A, NotUsed] =
    ask(parallelism = 2)(ref)(makeMessage)(timeout)

  /**
   * Use the `ask` pattern to send a request-reply message to the target `ref` actor.
   * If any of the asks times out it will fail the stream with a [[java.util.concurrent.TimeoutException]].
   *
   * Do not forget to include the expected response type in the method call, like so:
   *
   * {{{
   * flow.via(ActorFlow.ask[String, Asking, Reply](parallelism = 4)(ref, (el, replyTo) => Asking(el, replyTo)))
   *
   * // or even:
   * flow.via(ActorFlow.ask[String, Asking, Reply](parallelism = 4)(ref, Asking(_, _)))
   * }}}
   *
   * otherwise `Nothing` will be assumed, which is most likely not what you want.
   *
   * The operator fails with an [[akka.stream.WatchedActorTerminatedException]] if the target actor is terminated,
   * or with an [[java.util.concurrent.TimeoutException]] in case the ask exceeds the timeout passed in.
   *
   * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
   *
   * '''Emits when''' the futures (in submission order) created by the ask pattern internally are completed
   *
   * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream backpressures
   *
   * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted
   *
   * '''Fails when''' the passed in actor terminates, or a timeout is exceeded in any of the asks performed
   *
   * '''Cancels when''' downstream cancels
   *
   * @tparam I Incoming element type of the Flow
   * @tparam Q Question message type that is spoken by the target actor
   * @tparam A answer type that the Actor is expected to reply with, it will become the Output type of this Flow
   */
  @implicitNotFound("Missing an implicit akka.util.Timeout for the ask() stage")
  def ask[I, Q, A](parallelism: Int)(ref: ActorRef[Q])(makeMessage: (I, ActorRef[A]) => Q)(
      implicit timeout: Timeout): Flow[I, A, NotUsed] = askImpl(parallelism)(ref)(makeMessage, (_, o: Future[A]) => o)

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply#success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply#error]] arrives the future is instead
   * failed.
   */
  def askWithStatus[I, Q, A](ref: ActorRef[Q])(makeMessage: (I, ActorRef[StatusReply[A]]) => Q)(
      implicit timeout: Timeout): Flow[I, A, NotUsed] =
    askWithStatus(2)(ref)(makeMessage)

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply#success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply#error]] arrives the future is instead
   * failed.
   */
  def askWithStatus[I, Q, A](parallelism: Int)(ref: ActorRef[Q])(makeMessage: (I, ActorRef[StatusReply[A]]) => Q)(
      implicit timeout: Timeout): Flow[I, A, NotUsed] = {
    ActorFlow.ask(parallelism)(ref)(makeMessage).map {
      case StatusReply.Success(a) => a.asInstanceOf[A]
      case StatusReply.Error(err) => throw err
      case _                      => throw new RuntimeException() // compiler exhaustiveness check pleaser
    }

  }

  /**
   * Use the `ask` pattern to send a request-reply message to the target `ref` actor without including the context.
   */
  @implicitNotFound("Missing an implicit akka.util.Timeout for the ask() stage")
  def askWithContext[I, Q, A, Ctx](ref: ActorRef[Q])(makeMessage: (I, ActorRef[A]) => Q)(
      implicit timeout: Timeout): Flow[(I, Ctx), (A, Ctx), NotUsed] =
    askWithContext(parallelism = 2)(ref)(makeMessage)

  /**
   * Use the `ask` pattern to send a request-reply message to the target `ref` actor without including the context.
   */
  @implicitNotFound("Missing an implicit akka.util.Timeout for the ask() stage")
  def askWithContext[I, Q, A, Ctx](parallelism: Int)(ref: ActorRef[Q])(makeMessage: (I, ActorRef[A]) => Q)(
      implicit timeout: Timeout): Flow[(I, Ctx), (A, Ctx), NotUsed] =
    askImpl[(I, Ctx), Q, A, (A, Ctx)](parallelism)(ref)(
      (in, r) => makeMessage(in._1, r),
      (in, o: Future[A]) => o.map(a => a -> in._2)(ExecutionContext.parasitic))

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply#success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply#error]] arrives the future is instead
   * failed.
   */
  def askWithStatusAndContext[I, Q, A, Ctx](ref: ActorRef[Q])(makeMessage: (I, ActorRef[StatusReply[A]]) => Q)(
      implicit timeout: Timeout): Flow[(I, Ctx), (A, Ctx), NotUsed] =
    askWithStatusAndContext(2)(ref)(makeMessage)

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply#success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply#error]] arrives the future is instead
   * failed.
   */
  def askWithStatusAndContext[I, Q, A, Ctx](parallelism: Int)(ref: ActorRef[Q])(
      makeMessage: (I, ActorRef[StatusReply[A]]) => Q)(implicit timeout: Timeout): Flow[(I, Ctx), (A, Ctx), NotUsed] = {
    askImpl[(I, Ctx), Q, StatusReply[A], (StatusReply[A], Ctx)](parallelism)(ref)(
      (in, r) => makeMessage(in._1, r),
      (in, o: Future[StatusReply[A]]) => o.map(a => a -> in._2)(ExecutionContext.parasitic)).map {
      case (StatusReply.Success(a), ctx) => a.asInstanceOf[A] -> ctx
      case (StatusReply.Error(err), _)   => throw err
      case _                             => throw new RuntimeException() // compiler exhaustiveness check pleaser
    }

  }

}
