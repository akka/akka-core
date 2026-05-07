/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import java.net.URI
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.pattern.StatusReply
import org.scalatest.wordspec.AnyWordSpecLike

class InteractionPatternsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  private class DummyContext[T](val self: ActorRef[T])

  // #per-session-child
  // dummy data types just for this sample
  case class Keys()

  case class Wallet()

  // #per-session-child

  "The interaction patterns docs" must {

    "contain a sample for fire and forget" in {
      // #fire-and-forget-definition
      object Printer {

        case class PrintMe(message: String)

        def apply(): Behavior[PrintMe] =
          Behaviors.receive {
            case (context, PrintMe(message)) =>
              context.log.info(message)
              Behaviors.same
          }
      }
      // #fire-and-forget-definition

      // #fire-and-forget-doit
      val system = ActorSystem(Printer(), "fire-and-forget-sample")

      // note how the system is also the top level actor ref
      val printer: ActorRef[Printer.PrintMe] = system

      // these are all fire and forget
      printer ! Printer.PrintMe("message 1")
      printer ! Printer.PrintMe("not message 2")
      // #fire-and-forget-doit

      system.terminate()
      system.whenTerminated.futureValue
    }

    "contain a sample for request response" in {

      object CookieFabric {
        // #request-response-protocol
        case class Request(query: String, replyTo: ActorRef[Response])
        case class Response(result: String)
        // #request-response-protocol

        // #request-response-respond
        def apply(): Behaviors.Receive[Request] =
          Behaviors.receiveMessage[Request] {
            case Request(query, replyTo) =>
              // ... process query ...
              replyTo ! Response(s"Here are the cookies for [$query]!")
              Behaviors.same
          }
        // #request-response-respond
      }

      val cookieFabric: ActorRef[CookieFabric.Request] = spawn(CookieFabric())
      val probe = createTestProbe[CookieFabric.Response]()
      // shhh, don't tell anyone
      val context = new DummyContext[CookieFabric.Response](probe.ref)

      // #request-response-send
      cookieFabric ! CookieFabric.Request("give me cookies", context.self)
      // #request-response-send

      probe.receiveMessage()

      Behaviors.setup[Nothing] { context =>
        // #ignore-reply
        cookieFabric ! CookieFabric.Request("don't send cookies back", context.system.ignoreRef)
        // #ignore-reply
        Behaviors.empty
      }
    }

    "contain a sample for adapted response" in {
      // #adapted-response

      object Backend {
        sealed trait Request
        final case class StartTranslationJob(taskId: Int, site: URI, replyTo: ActorRef[Response]) extends Request

        sealed trait Response
        final case class JobStarted(taskId: Int) extends Response
        final case class JobProgress(taskId: Int, progress: Double) extends Response
        final case class JobCompleted(taskId: Int, result: URI) extends Response
      }

      object Frontend {

        sealed trait Command
        final case class Translate(site: URI, replyTo: ActorRef[URI]) extends Command
        private final case class WrappedBackendResponse(response: Backend.Response) extends Command

        def apply(backend: ActorRef[Backend.Request]): Behavior[Command] =
          Behaviors.setup[Command] { context =>
            val backendResponseMapper: ActorRef[Backend.Response] =
              context.messageAdapter(rsp => WrappedBackendResponse(rsp))

            def active(inProgress: Map[Int, ActorRef[URI]], count: Int): Behavior[Command] = {
              Behaviors.receiveMessage[Command] {
                case Translate(site, replyTo) =>
                  val taskId = count + 1
                  backend ! Backend.StartTranslationJob(taskId, site, backendResponseMapper)
                  active(inProgress.updated(taskId, replyTo), taskId)

                case wrapped: WrappedBackendResponse =>
                  wrapped.response match {
                    case Backend.JobStarted(taskId) =>
                      context.log.info("Started {}", taskId)
                      Behaviors.same
                    case Backend.JobProgress(taskId, progress) =>
                      context.log.info("Progress {}: {}", taskId, progress)
                      Behaviors.same
                    case Backend.JobCompleted(taskId, result) =>
                      context.log.info("Completed {}: {}", taskId, result)
                      inProgress(taskId) ! result
                      active(inProgress - taskId, count)
                  }
              }
            }

            active(inProgress = Map.empty, count = 0)
          }
      }
      // #adapted-response

      val backend = spawn(Behaviors.receiveMessage[Backend.Request] {
        case Backend.StartTranslationJob(taskId, _, replyTo) =>
          replyTo ! Backend.JobStarted(taskId)
          replyTo ! Backend.JobProgress(taskId, 0.25)
          replyTo ! Backend.JobProgress(taskId, 0.50)
          replyTo ! Backend.JobProgress(taskId, 0.75)
          replyTo ! Backend.JobCompleted(taskId, new URI("https://akka.io/docs/sv/"))
          Behaviors.same
      })

      val frontend = spawn(Frontend(backend))
      val probe = createTestProbe[URI]()
      frontend ! Frontend.Translate(new URI("https://akka.io/docs/"), probe.ref)
      probe.expectMessage(new URI("https://akka.io/docs/sv/"))
    }

  }

  "contain a sample for scheduling messages to self" in {

    //#timer
    object Buncher {

      sealed trait Command
      final case class ExcitingMessage(message: String) extends Command
      final case class Batch(messages: Vector[Command])
      private case object Timeout extends Command
      private case object TimerKey

      def apply(target: ActorRef[Batch], after: FiniteDuration, maxSize: Int): Behavior[Command] = {
        Behaviors.withTimers(timers => new Buncher(timers, target, after, maxSize).idle())
      }
    }

    class Buncher(
        timers: TimerScheduler[Buncher.Command],
        target: ActorRef[Buncher.Batch],
        after: FiniteDuration,
        maxSize: Int) {
      import Buncher._

      private def idle(): Behavior[Command] = {
        Behaviors.receiveMessage[Command] { message =>
          timers.startSingleTimer(TimerKey, Timeout, after)
          active(Vector(message))
        }
      }

      def active(buffer: Vector[Command]): Behavior[Command] = {
        Behaviors.receiveMessage[Command] {
          case Timeout =>
            target ! Batch(buffer)
            idle()
          case m =>
            val newBuffer = buffer :+ m
            if (newBuffer.size == maxSize) {
              timers.cancel(TimerKey)
              target ! Batch(newBuffer)
              idle()
            } else
              active(newBuffer)
        }
      }
    }
    //#timer

    val probe = createTestProbe[Buncher.Batch]()
    val buncher: ActorRef[Buncher.Command] = spawn(Buncher(probe.ref, 1.second, 10))
    buncher ! Buncher.ExcitingMessage("one")
    buncher ! Buncher.ExcitingMessage("two")
    probe.expectNoMessage()
    probe.expectMessage(
      2.seconds,
      Buncher.Batch(Vector[Buncher.Command](Buncher.ExcitingMessage("one"), Buncher.ExcitingMessage("two"))))
  }

  "contain a sample for ask" in {
    import akka.util.Timeout

    // #actor-ask
    object Hal {
      sealed trait Command
      case class OpenThePodBayDoorsPlease(replyTo: ActorRef[Response]) extends Command
      case class Response(message: String)

      def apply(): Behaviors.Receive[Hal.Command] =
        Behaviors.receiveMessage[Command] {
          case OpenThePodBayDoorsPlease(replyTo) =>
            replyTo ! Response("I'm sorry, Dave. I'm afraid I can't do that.")
            Behaviors.same
        }
    }

    object Dave {

      sealed trait Command
      // this is a part of the protocol that is internal to the actor itself
      private case class AdaptedResponse(message: String) extends Command

      def apply(hal: ActorRef[Hal.Command]): Behavior[Dave.Command] =
        Behaviors.setup[Command] { context =>
          // asking someone requires a timeout, if the timeout hits without response
          // the ask is failed with a TimeoutException
          implicit val timeout: Timeout = 3.seconds

          // Note: The second parameter list takes a function `ActorRef[T] => Message`,
          // as OpenThePodBayDoorsPlease is a case class it has a factory apply method
          // that is what we are passing as the second parameter here it could also be written
          // as `ref => OpenThePodBayDoorsPlease(ref)`
          context.ask(hal, Hal.OpenThePodBayDoorsPlease.apply) {
            case Success(Hal.Response(message)) => AdaptedResponse(message)
            case Failure(_)                     => AdaptedResponse("Request failed")
          }

          // we can also tie in request context into an interaction, it is safe to look at
          // actor internal state from the transformation function, but remember that it may have
          // changed at the time the response arrives and the transformation is done, best is to
          // use immutable state we have closed over like here.
          val requestId = 1
          context.ask(hal, Hal.OpenThePodBayDoorsPlease.apply) {
            case Success(Hal.Response(message)) => AdaptedResponse(s"$requestId: $message")
            case Failure(_)                     => AdaptedResponse(s"$requestId: Request failed")
          }

          Behaviors.receiveMessage {
            // the adapted message ends up being processed like any other
            // message sent to the actor
            case AdaptedResponse(message) =>
              context.log.info("Got response from hal: {}", message)
              Behaviors.same
          }
        }
    }
    // #actor-ask

    // somewhat modified behavior to let us know we saw the two requests
    val monitor = createTestProbe[Hal.Command]()
    val hal = spawn(Behaviors.monitor(monitor.ref, Hal()))
    spawn(Dave(hal))
    monitor.expectMessageType[Hal.OpenThePodBayDoorsPlease]
    monitor.expectMessageType[Hal.OpenThePodBayDoorsPlease]
  }

  "contain a sample for outside ask with status" in {
    import akka.util.Timeout

    // #actor-ask-with-status
    object Hal {
      sealed trait Command
      case class OpenThePodBayDoorsPlease(replyTo: ActorRef[StatusReply[String]]) extends Command

      def apply(): Behaviors.Receive[Hal.Command] =
        Behaviors.receiveMessage[Command] {
          case OpenThePodBayDoorsPlease(replyTo) =>
            // reply with a validation error description
            replyTo ! StatusReply.Error("I'm sorry, Dave. I'm afraid I can't do that.")
            Behaviors.same
        }
    }

    object Dave {

      sealed trait Command
      // this is a part of the protocol that is internal to the actor itself
      private case class AdaptedResponse(message: String) extends Command

      def apply(hal: ActorRef[Hal.Command]): Behavior[Dave.Command] =
        Behaviors.setup[Command] { context =>
          // asking someone requires a timeout, if the timeout hits without response
          // the ask is failed with a TimeoutException
          implicit val timeout: Timeout = 3.seconds

          // A StatusReply.Success(m) ends up as a Success(m) here, while a
          // StatusReply.Error(text) becomes a Failure(ErrorMessage(text))
          context.askWithStatus(hal, Hal.OpenThePodBayDoorsPlease.apply) {
            case Success(message)                        => AdaptedResponse(message)
            case Failure(StatusReply.ErrorMessage(text)) => AdaptedResponse(s"Request denied: $text")
            case Failure(_)                              => AdaptedResponse("Request failed")
          }

          Behaviors.receiveMessage {
            // the adapted message ends up being processed like any other
            // message sent to the actor
            case AdaptedResponse(message) =>
              context.log.info("Got response from hal: {}", message)
              Behaviors.same
          }
        }
    }
    // #actor-ask-with-status

    // somewhat modified behavior to let us know we saw the two requests
    val monitor = createTestProbe[Hal.Command]()
    val hal = spawn(Behaviors.monitor(monitor.ref, Hal()))
    spawn(Dave(hal))
    monitor.expectMessageType[Hal.OpenThePodBayDoorsPlease]
  }

  "contain a sample for per session child" in {

    object KeyCabinet {
      case class GetKeys(whoseKeys: String, replyTo: ActorRef[Keys])

      def apply(): Behavior[GetKeys] =
        Behaviors.receiveMessage {
          case GetKeys(_, replyTo) =>
            replyTo ! Keys()
            Behaviors.same
        }
    }

    object Drawer {
      case class GetWallet(whoseWallet: String, replyTo: ActorRef[Wallet])

      def apply(): Behavior[GetWallet] =
        Behaviors.receiveMessage {
          case GetWallet(_, replyTo) =>
            replyTo ! Wallet()
            Behaviors.same
        }
    }

    // #per-session-child

    object Home {
      sealed trait Command
      case class LeaveHome(who: String, replyTo: ActorRef[ReadyToLeaveHome]) extends Command
      case class ReadyToLeaveHome(who: String, keys: Keys, wallet: Wallet)

      def apply(): Behavior[Command] = {
        Behaviors.setup[Command] { context =>
          val keyCabinet: ActorRef[KeyCabinet.GetKeys] = context.spawn(KeyCabinet(), "key-cabinet")
          val drawer: ActorRef[Drawer.GetWallet] = context.spawn(Drawer(), "drawer")

          Behaviors.receiveMessage[Command] {
            case LeaveHome(who, replyTo) =>
              context.spawn(prepareToLeaveHome(who, replyTo, keyCabinet, drawer), s"leaving-$who")
              Behaviors.same
          }
        }
      }

      // per session actor behavior
      def prepareToLeaveHome(
          whoIsLeaving: String,
          replyTo: ActorRef[ReadyToLeaveHome],
          keyCabinet: ActorRef[KeyCabinet.GetKeys],
          drawer: ActorRef[Drawer.GetWallet]): Behavior[NotUsed] = {
        // we don't _really_ care about the actor protocol here as nobody will send us
        // messages except for responses to our queries, so we just accept any kind of message
        // but narrow that to more limited types when we interact
        Behaviors
          .setup[AnyRef] { context =>
            var wallet: Option[Wallet] = None
            var keys: Option[Keys] = None

            // we narrow the ActorRef type to any subtype of the actual type we accept
            keyCabinet ! KeyCabinet.GetKeys(whoIsLeaving, context.self.narrow[Keys])
            drawer ! Drawer.GetWallet(whoIsLeaving, context.self.narrow[Wallet])

            def nextBehavior(): Behavior[AnyRef] =
              (keys, wallet) match {
                case (Some(w), Some(k)) =>
                  // we got both, "session" is completed!
                  replyTo ! ReadyToLeaveHome(whoIsLeaving, w, k)
                  Behaviors.stopped

                case _ =>
                  Behaviors.same
              }

            Behaviors.receiveMessage {
              case w: Wallet =>
                wallet = Some(w)
                nextBehavior()
              case k: Keys =>
                keys = Some(k)
                nextBehavior()
              case _ =>
                Behaviors.unhandled
            }
          }
          .narrow[NotUsed] // we don't let anyone else know we accept anything
      }
    }
    // #per-session-child

    val requestor = createTestProbe[Home.ReadyToLeaveHome]()

    val home = spawn(Home(), "home")
    home ! Home.LeaveHome("Bobby", requestor.ref)
    requestor.expectMessage(Home.ReadyToLeaveHome("Bobby", Keys(), Wallet()))
  }

  "contain a sample for ask from outside the actor system" in {
    // #standalone-ask
    object CookieFabric {
      sealed trait Command
      case class GiveMeCookies(count: Int, replyTo: ActorRef[Reply]) extends Command

      sealed trait Reply
      case class Cookies(count: Int) extends Reply
      case class InvalidRequest(reason: String) extends Reply

      def apply(): Behaviors.Receive[CookieFabric.GiveMeCookies] =
        Behaviors.receiveMessage { message =>
          if (message.count >= 5)
            message.replyTo ! InvalidRequest("Too many cookies.")
          else
            message.replyTo ! Cookies(message.count)
          Behaviors.same
        }
    }
    // #standalone-ask

    // keep this out of the sample as it uses the testkit spawn
    val cookieFabric = spawn(CookieFabric())

    val theSystem = testKit.system

    // #standalone-ask

    import akka.actor.typed.scaladsl.AskPattern._
    import akka.util.Timeout

    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.seconds
    // implicit ActorSystem in scope
    implicit val system: ActorSystem[_] = theSystem

    val result: Future[CookieFabric.Reply] = cookieFabric.ask(ref => CookieFabric.GiveMeCookies(3, ref))

    // the response callback will be executed on this execution context
    implicit val ec = system.executionContext

    result.onComplete {
      case Success(CookieFabric.Cookies(count))         => println(s"Yay, $count cookies!")
      case Success(CookieFabric.InvalidRequest(reason)) => println(s"No cookies for me. $reason")
      case Failure(ex)                                  => println(s"Boo! didn't get cookies: ${ex.getMessage}")
    }
    // #standalone-ask

    result.futureValue shouldEqual CookieFabric.Cookies(3)

    // #standalone-ask-fail-future
    val cookies: Future[CookieFabric.Cookies] =
      cookieFabric.ask[CookieFabric.Reply](ref => CookieFabric.GiveMeCookies(3, ref)).flatMap {
        case c: CookieFabric.Cookies             => Future.successful(c)
        case CookieFabric.InvalidRequest(reason) => Future.failed(new IllegalArgumentException(reason))
      }

    cookies.onComplete {
      case Success(CookieFabric.Cookies(count)) => println(s"Yay, $count cookies!")
      case Failure(ex)                          => println(s"Boo! didn't get cookies: ${ex.getMessage}")
    }
    // #standalone-ask-fail-future

    cookies.futureValue shouldEqual CookieFabric.Cookies(3)
  }

  "contain a sample for ask with status from outside the actor system" in {
    // #standalone-ask-with-status
    object CookieFabric {
      sealed trait Command
      case class GiveMeCookies(count: Int, replyTo: ActorRef[StatusReply[Cookies]]) extends Command
      case class Cookies(count: Int)

      def apply(): Behaviors.Receive[CookieFabric.GiveMeCookies] =
        Behaviors.receiveMessage { message =>
          if (message.count >= 5)
            message.replyTo ! StatusReply.Error("Too many cookies.")
          else
            message.replyTo ! StatusReply.Success(Cookies(message.count))
          Behaviors.same
        }
    }
    // #standalone-ask-with-status

    // keep this out of the sample as it uses the testkit spawn
    val cookieFabric = spawn(CookieFabric())

    val theSystem = testKit.system

    // #standalone-ask-with-status

    import akka.actor.typed.scaladsl.AskPattern._
    import akka.util.Timeout

    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.seconds
    // implicit ActorSystem in scope
    implicit val system: ActorSystem[_] = theSystem

    val result: Future[CookieFabric.Cookies] = cookieFabric.askWithStatus(ref => CookieFabric.GiveMeCookies(3, ref))

    // the response callback will be executed on this execution context
    implicit val ec = system.executionContext

    result.onComplete {
      case Success(CookieFabric.Cookies(count))      => println(s"Yay, $count cookies!")
      case Failure(StatusReply.ErrorMessage(reason)) => println(s"No cookies for me. $reason")
      case Failure(ex)                               => println(s"Boo! didn't get cookies: ${ex.getMessage}")
    }
    // #standalone-ask-with-status

    result.futureValue shouldEqual CookieFabric.Cookies(3)

    // #standalone-ask-with-status-fail-future
    val cookies: Future[CookieFabric.Cookies] =
      cookieFabric.askWithStatus[CookieFabric.Cookies](ref => CookieFabric.GiveMeCookies(3, ref)).flatMap {
        case c: CookieFabric.Cookies => Future.successful(c)
      }

    cookies.onComplete {
      case Success(CookieFabric.Cookies(count)) => println(s"Yay, $count cookies!")
      case Failure(ex)                          => println(s"Boo! didn't get cookies: ${ex.getMessage}")
    }
    // #standalone-ask-with-status-fail-future

    cookies.futureValue shouldEqual CookieFabric.Cookies(3)
  }

  "contain a sample for pipeToSelf" in {
    //#pipeToSelf

    trait CustomerDataAccess {
      def update(value: Customer): Future[Done]
    }

    final case class Customer(id: String, version: Long, name: String, address: String)

    object CustomerRepository {
      sealed trait Command

      final case class Update(value: Customer, replyTo: ActorRef[UpdateResult]) extends Command
      sealed trait UpdateResult
      final case class UpdateSuccess(id: String) extends UpdateResult
      final case class UpdateFailure(id: String, reason: String) extends UpdateResult
      private final case class WrappedUpdateResult(result: UpdateResult, replyTo: ActorRef[UpdateResult])
          extends Command

      private val MaxOperationsInProgress = 10

      def apply(dataAccess: CustomerDataAccess): Behavior[Command] = {
        next(dataAccess, operationsInProgress = 0)
      }

      private def next(dataAccess: CustomerDataAccess, operationsInProgress: Int): Behavior[Command] = {
        Behaviors.receive { (context, command) =>
          command match {
            case Update(value, replyTo) =>
              if (operationsInProgress == MaxOperationsInProgress) {
                replyTo ! UpdateFailure(value.id, s"Max $MaxOperationsInProgress concurrent operations supported")
                Behaviors.same
              } else {
                val futureResult = dataAccess.update(value)
                context.pipeToSelf(futureResult) {
                  // map the Future value to a message, handled by this actor
                  case Success(_) => WrappedUpdateResult(UpdateSuccess(value.id), replyTo)
                  case Failure(e) => WrappedUpdateResult(UpdateFailure(value.id, e.getMessage), replyTo)
                }
                // increase operationsInProgress counter
                next(dataAccess, operationsInProgress + 1)
              }

            case WrappedUpdateResult(result, replyTo) =>
              // send result to original requestor
              replyTo ! result
              // decrease operationsInProgress counter
              next(dataAccess, operationsInProgress - 1)
          }
        }
      }
    }
    //#pipeToSelf

    val dataAccess = new CustomerDataAccess {
      override def update(value: Customer): Future[Done] = Future.successful(Done)
    }

    val repository = spawn(CustomerRepository(dataAccess))
    val probe = createTestProbe[CustomerRepository.UpdateResult]()
    repository ! CustomerRepository.Update(Customer("123", 1L, "Alice", "Fairy tail road 7"), probe.ref)
    probe.expectMessage(CustomerRepository.UpdateSuccess("123"))
  }
}
