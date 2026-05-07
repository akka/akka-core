/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import docs.akka.typed.IntroSpec.HelloWorld
import org.scalatest.wordspec.AnyWordSpecLike
import scala.annotation.nowarn

//#imports1
import akka.actor.typed.Behavior
import akka.actor.typed.SpawnProtocol
import akka.actor.typed.scaladsl.Behaviors

//#imports1

//#imports2
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Props
import akka.util.Timeout

//#imports2

object SpawnProtocolDocSpec {

  // Silent because we want to name the unused 'context' parameter
  @nowarn("msg=never used")
  //#main
  object HelloWorldMain {
    def apply(): Behavior[SpawnProtocol.Command] =
      Behaviors.setup { context =>
        // Start initial tasks
        // context.spawn(...)

        SpawnProtocol()
      }
  }
  //#main
}

class SpawnProtocolDocSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import SpawnProtocolDocSpec._

  "ActorSystem with SpawnProtocol" must {
    "be able to spawn actors" in {
      //#system-spawn

      implicit val system: ActorSystem[SpawnProtocol.Command] =
        ActorSystem(HelloWorldMain(), "hello")

      // needed in implicit scope for ask (?)
      import akka.actor.typed.scaladsl.AskPattern._
      implicit val ec: ExecutionContext = system.executionContext
      implicit val timeout: Timeout = Timeout(3.seconds)

      val greeter: Future[ActorRef[HelloWorld.Greet]] =
        system.ask(SpawnProtocol.Spawn(behavior = HelloWorld(), name = "greeter", props = Props.empty, _))

      val greetedBehavior = Behaviors.receive[HelloWorld.Greeted] { (context, message) =>
        context.log.info("Greeting for {} from {}", message.whom, message.from)
        Behaviors.stopped
      }

      val greetedReplyTo: Future[ActorRef[HelloWorld.Greeted]] =
        system.ask(SpawnProtocol.Spawn(greetedBehavior, name = "", props = Props.empty, _))

      for (greeterRef <- greeter; replyToRef <- greetedReplyTo) {
        greeterRef ! HelloWorld.Greet("Akka", replyToRef)
      }

      //#system-spawn

      Thread.sleep(500) // it will not fail if too short
      ActorTestKit.shutdown(system)
    }

  }

}
