/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorPath
import akka.actor.ActorRefProvider
import akka.actor.ActorSystemImpl
import akka.actor.MinimalActorRef
import akka.actor.RootActorPath
import akka.actor.typed.scaladsl.Behaviors

class ActorRefResolverSpec extends AnyWordSpec with ScalaFutures with Matchers {
  "ActorRefResolver" should {
    "not allow serialization of ref originating from other system" in {
      val system1 = ActorSystem(Behaviors.empty[String], "sys1")
      val system2 = ActorSystem(Behaviors.empty[String], "sys2")
      try {
        val ref1 = system1.systemActorOf(Behaviors.empty, "ref1")
        val serialized = ActorRefResolver(system1).toSerializationFormat(ref1)
        serialized should startWith("akka://sys1/")

        intercept[IllegalArgumentException] {
          // wrong system
          ActorRefResolver(system2).toSerializationFormat(ref1)
        }

        // we can't detect that for MinimalActorRef
        import akka.actor.typed.scaladsl.adapter._

        val minRef1: akka.actor.ActorRef = new MinimalActorRef {
          override def provider: ActorRefProvider = system1.toClassic.asInstanceOf[ActorSystemImpl].provider
          override def path: ActorPath = RootActorPath(system1.address) / "minRef1"
        }

        val minRef1Serialized = ActorRefResolver(system2).toSerializationFormat(minRef1)
        minRef1Serialized should startWith("akka://sys2/")

      } finally {
        system1.terminate()
        system2.terminate()
      }
    }

  }

}
