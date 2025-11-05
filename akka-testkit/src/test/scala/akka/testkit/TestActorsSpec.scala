/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

class TestActorsSpec extends AkkaSpec with ImplicitSender {

  import TestActors.{ echoActorProps, forwardActorProps }

  "A EchoActor" must {
    "send back messages unchanged" in {
      val message = "hello world"
      val echo = system.actorOf(echoActorProps)

      echo ! message

      expectMsg(message)
    }
  }

  "A ForwardActor" must {
    "forward messages to target actor" in {
      val message = "forward me"
      val forward = system.actorOf(forwardActorProps(testActor))

      forward ! message

      expectMsg(message)
    }
  }
}
