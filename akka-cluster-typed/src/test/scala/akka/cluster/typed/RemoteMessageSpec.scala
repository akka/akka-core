/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.concurrent.Promise

import com.typesafe.config.ConfigFactory

import akka.Done
import akka.actor.{ ActorSystem => ClassicActorSystem }
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.jackson.CborSerializable
import akka.testkit.AkkaSpec

object RemoteMessageSpec {
  def config = ConfigFactory.parseString("""
    akka {
      loglevel = debug
      actor.provider = cluster
      remote.artery {
        canonical {
          hostname = 127.0.0.1
          port = 0
        }
      }
    }
    """)

  case class Ping(sender: ActorRef[String]) extends CborSerializable
}

class RemoteMessageSpec extends AkkaSpec(RemoteMessageSpec.config) {

  import RemoteMessageSpec._

  val typedSystem = system.toTyped

  "the adapted system" should {

    "something something" in {

      val pingPromise = Promise[Done]()
      val ponger = Behaviors.receive[Ping]((_, msg) =>
        msg match {
          case Ping(sender) =>
            pingPromise.success(Done)
            sender ! "pong"
            Behaviors.stopped
        })

      // typed actor on system1
      val pingPongActor = system.spawn(ponger, "pingpong")

      val system2 = ClassicActorSystem(system.name + "-system2", RemoteMessageSpec.config)
      val typedSystem2 = system2.toTyped
      try {

        // resolve the actor from node2
        val remoteRefStr = ActorRefResolver(typedSystem).toSerializationFormat(pingPongActor)
        val remoteRef: ActorRef[Ping] =
          ActorRefResolver(typedSystem2).resolveActorRef[Ping](remoteRefStr)

        val pongPromise = Promise[Done]()
        val recipient = system2.spawn(Behaviors.receive[String] { (_, _) =>
          pongPromise.success(Done)
          Behaviors.stopped
        }, "recipient")
        remoteRef ! Ping(recipient)

        pingPromise.future.futureValue should ===(Done)
        pongPromise.future.futureValue should ===(Done)

      } finally {
        system2.terminate()
      }
    }

  }

}
