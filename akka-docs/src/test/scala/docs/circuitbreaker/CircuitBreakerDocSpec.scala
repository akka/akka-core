/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.circuitbreaker

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration._
import akka.pattern.CircuitBreaker
import akka.pattern.StatusReply
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CircuitBreakerDocSpec {
  def config = ConfigFactory.parseString("""
       #config
       akka.circuit-breaker.data-access {
         max-failures = 5
         call-timeout = 10s
         reset-timeout = 1m
       }
       #config
      """)

  trait ThirdPartyWebService {
    def call(id: String, value: String): Future[Done]
  }

  object DataAccess {
    sealed trait Command
    final case class Handle(value: String, replyTo: ActorRef[StatusReply[Done]]) extends Command

    private final case class HandleFailed(replyTo: ActorRef[StatusReply[Done]], failure: Throwable) extends Command
    private final case class HandleSuceeded(replyTo: ActorRef[StatusReply[Done]]) extends Command

    private final case class CircuitBreakerStateChange(newState: String) extends Command

    def apply(id: String, service: ThirdPartyWebService): Behavior[Command] = {
      Behaviors.setup[Command] { context =>
        //#circuit-breaker-initialization
        val circuitBreaker = CircuitBreaker("data-access")(context.system)
        //#circuit-breaker-initialization
        new DataAccess(context, id, service, circuitBreaker).active()
      }
    }
  }

  //#circuit-breaker-usage
  class DataAccess(
      context: ActorContext[DataAccess.Command],
      id: String,
      service: ThirdPartyWebService,
      circuitBreaker: CircuitBreaker) {
    import DataAccess._

    private def active(): Behavior[Command] = {
      Behaviors.receiveMessagePartial {
        case Handle(value, replyTo) =>
          val futureResult: Future[Done] = circuitBreaker.withCircuitBreaker {
            service.call(id, value)
          }
          context.pipeToSelf(futureResult) {
            case Success(_)         => HandleSuceeded(replyTo)
            case Failure(exception) => HandleFailed(replyTo, exception)
          }
          Behaviors.same
        case HandleSuceeded(replyTo) =>
          replyTo ! StatusReply.Ack
          Behaviors.same
        case HandleFailed(replyTo, exception) =>
          context.log.warn("Failed to call web service", exception)
          replyTo ! StatusReply.error("Dependency service not available")
          Behaviors.same
      }

    }
  }
  //#circuit-breaker-usage

  object ApiExamples {
    implicit def system: ActorSystem[_] = ???
    implicit def ec: ExecutionContext = ???

    def showDefineFailure(): Unit = {
      //#even-no-as-failure

      val evenNumberAsFailure: Try[Int] => Boolean = {
        case Success(n) => n % 2 == 0
        case Failure(_) => true
      }

      val breaker = CircuitBreaker("dangerous-breaker")

      // this call will return 8888 and increase failure count at the same time
      breaker.withCircuitBreaker(Future(8888), evenNumberAsFailure)
      //#even-no-as-failure
    }
  }

  object OtherActor {
    sealed trait Command
    case class Call(payload: String, replyTo: ActorRef[StatusReply[Done]]) extends Command
  }

  //#circuit-breaker-tell-pattern
  object CircuitBreakingIntermediateActor {
    sealed trait Command
    case class Call(payload: String, replyTo: ActorRef[StatusReply[Done]]) extends Command
    private case class OtherActorReply(reply: Try[Done], originalReplyTo: ActorRef[StatusReply[Done]]) extends Command
    private case object BreakerOpen extends Command

    def apply(recipient: ActorRef[OtherActor.Command]): Behavior[Command] =
      Behaviors.setup { context =>
        implicit val askTimeout: Timeout = 11.seconds
        import context.executionContext
        // #manual-construction
        import akka.actor.typed.scaladsl.adapter._
        val breaker =
          new CircuitBreaker(
            context.system.scheduler.toClassic,
            maxFailures = 5,
            callTimeout = 10.seconds,
            resetTimeout = 1.minute).onOpen(context.self ! BreakerOpen)
        // #manual-construction

        Behaviors.receiveMessage {
          case Call(payload, replyTo) =>
            if (breaker.isClosed || breaker.isHalfOpen) {
              context.askWithStatus(recipient, OtherActor.Call(payload, _))(OtherActorReply(_, replyTo))
            } else {
              replyTo ! StatusReply.error("Service unavailable")
            }
            Behaviors.same
          case OtherActorReply(reply, originalReplyTo) =>
            if (reply.isSuccess) breaker.succeed()
            else breaker.fail()
            originalReplyTo ! StatusReply.fromTry(reply)
            Behaviors.same
          case BreakerOpen =>
            context.log.warn("Circuit breaker open")
            Behaviors.same
        }
      }
  }
  //#circuit-breaker-tell-pattern
}
