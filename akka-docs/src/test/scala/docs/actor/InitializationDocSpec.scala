/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

import akka.actor.{ Actor, Props }
import akka.testkit.{ AkkaSpec, ImplicitSender }

object InitializationDocSpec {

  class PreStartInitExample extends Actor {
    override def receive = {
      case _ => // Ignore
    }

    //#preStartInit
    override def preStart(): Unit = {
      // Initialize children here
    }

    // Overriding postRestart to disable the call to preStart()
    // after restarts
    override def postRestart(reason: Throwable): Unit = ()

    // The default implementation of preRestart() stops all the children
    // of the actor. To opt-out from stopping the children, we
    // have to override preRestart()
    override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
      // Keep the call to postStop(), but no stopping of children
      postStop()
    }
    //#preStartInit
  }

  class MessageInitExample extends Actor {
    //#messageInit
    var initializeMe: Option[String] = None

    override def receive = {
      case "init" =>
        initializeMe = Some("Up and running")
        context.become(initialized, discardOld = true)

    }

    def initialized: Receive = {
      case "U OK?" => initializeMe.foreach { sender() ! _ }
    }
    //#messageInit

  }
}

class InitializationDocSpec extends AkkaSpec with ImplicitSender {
  import InitializationDocSpec._

  "Message based initialization example" must {

    "work correctly" in {
      val example = system.actorOf(Props[MessageInitExample](), "messageInitExample")
      val probe = "U OK?"

      example ! probe
      expectNoMessage()

      example ! "init"
      example ! probe
      expectMsg("Up and running")

    }

  }

}
