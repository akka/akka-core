/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.actor.{ EmptyLocalActorRef, InternalActorRef }
import akka.actor.ActorRefScope
import akka.actor.ExtendedActorSystem
import akka.remote.RemoteActorRef
import akka.testkit.{ EventFilter, TestActors }

class RemoteActorRefProviderSpec extends ArteryMultiNodeSpec {

  val addressA = address(localSystem)
  system.actorOf(TestActors.echoActorProps, "echo")

  val systemB = newRemoteSystem()
  val addressB = address(systemB)
  systemB.actorOf(TestActors.echoActorProps, "echo")

  "RemoteActorRefProvider" must {

    "resolve local actor selection" in {
      val sel = system.actorSelection(s"akka://${system.name}@${addressA.host.get}:${addressA.port.get}/user/echo")
      sel.anchor.asInstanceOf[InternalActorRef].isLocal should be(true)
    }

    "resolve remote actor selection" in {
      val sel = system.actorSelection(s"akka://${systemB.name}@${addressB.host.get}:${addressB.port.get}/user/echo")
      sel.anchor.getClass should ===(classOf[RemoteActorRef])
      sel.anchor.asInstanceOf[InternalActorRef].isLocal should be(false)
    }

    "cache resolveActorRef for local ref" in {
      val provider = localSystem.asInstanceOf[ExtendedActorSystem].provider
      val path = s"akka://${system.name}@${addressA.host.get}:${addressA.port.get}/user/echo"
      val ref1 = provider.resolveActorRef(path)
      ref1.getClass should !==(classOf[EmptyLocalActorRef])
      ref1.asInstanceOf[ActorRefScope].isLocal should ===(true)

      val ref2 = provider.resolveActorRef(path)
      (ref1 should be).theSameInstanceAs(ref2)
    }

    "not cache resolveActorRef for unresolved ref" in {
      val provider = localSystem.asInstanceOf[ExtendedActorSystem].provider
      val path = s"akka://${system.name}@${addressA.host.get}:${addressA.port.get}/user/doesNotExist"
      val ref1 = provider.resolveActorRef(path)
      ref1.getClass should ===(classOf[EmptyLocalActorRef])

      val ref2 = provider.resolveActorRef(path)
      ref1 should not be theSameInstanceAs(ref2)
    }

    "cache resolveActorRef for remote ref" in {
      val provider = localSystem.asInstanceOf[ExtendedActorSystem].provider
      val path = s"akka://${systemB.name}@${addressB.host.get}:${addressB.port.get}/user/echo"
      val ref1 = provider.resolveActorRef(path)
      ref1.getClass should ===(classOf[RemoteActorRef])

      val ref2 = provider.resolveActorRef(path)
      (ref1 should be).theSameInstanceAs(ref2)
    }

    "detect wrong protocol" in {
      EventFilter[IllegalArgumentException](start = "No root guardian at", occurrences = 1).intercept {
        val sel =
          system.actorSelection(s"akka.tcp://${systemB.name}@${addressB.host.get}:${addressB.port.get}/user/echo")
        sel.anchor.getClass should ===(classOf[EmptyLocalActorRef])
      }
    }

  }

}
