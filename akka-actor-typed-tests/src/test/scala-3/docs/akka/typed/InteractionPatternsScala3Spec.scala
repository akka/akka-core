/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import java.net.URI

class InteractionPatternsScala3Spec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "The interaction patterns docs" must {
    "contain a sample for request response" in {

      object CookieFabric {
        case class Request(query: String, replyTo: ActorRef[Response])

        case class Response(result: String)

        def apply(): Behaviors.Receive[Request] =
          Behaviors.receiveMessage[Request] {
            case Request(query, replyTo) =>
              // ... process query ...
              replyTo ! Response(s"Here are the cookies for [$query]!")
              Behaviors.same
          }
      }

      // #request-response-send
      object CookieMonster {
        sealed trait Command
        case object Munch extends Command

        def apply(cookieFabric: ActorRef[CookieFabric.Request]): Behavior[Command] =
          Behaviors
            .setup[Command | CookieFabric.Response] { context =>
              Behaviors.receiveMessage {
                case Munch =>
                  cookieFabric ! CookieFabric.Request("Give me cookies", context.self)
                  Behaviors.same

                case CookieFabric.Response(response) =>
                  context.log.info("nonomnom got cookies: {}", response)
                  cookieFabric ! CookieFabric.Request("Give me more cookies", context.self)
                  Behaviors.same
              }
            }
            .narrow
      }
      // #request-response-send
    }

    "contain a sample for adapted response" in {

      object Backend {
        sealed trait Request

        final case class StartTranslationJob(taskId: Int, site: URI, replyTo: ActorRef[Response]) extends Request

        sealed trait Response

        final case class JobStarted(taskId: Int) extends Response

        final case class JobProgress(taskId: Int, progress: Double) extends Response

        final case class JobCompleted(taskId: Int, result: URI) extends Response
      }

      // #adapted-response
      object Frontend {

        sealed trait Command

        final case class Translate(site: URI, replyTo: ActorRef[URI]) extends Command

        private type CommandOrResponse = Command | Backend.Response

        def apply(backend: ActorRef[Backend.Request]): Behavior[Command] =
          Behaviors
            .setup[CommandOrResponse] { context =>
              def active(inProgress: Map[Int, ActorRef[URI]], count: Int): Behavior[CommandOrResponse] = {
                Behaviors.receiveMessage[CommandOrResponse] {
                  case Translate(site, replyTo) =>
                    val taskId = count + 1
                    backend ! Backend.StartTranslationJob(taskId, site, context.self)
                    active(inProgress.updated(taskId, replyTo), taskId)

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

              active(inProgress = Map.empty, count = 0)
            }
            .narrow
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

}
