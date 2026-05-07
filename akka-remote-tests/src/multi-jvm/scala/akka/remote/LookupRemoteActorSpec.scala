/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import testkit.MultiNodeConfig

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.Props
import akka.pattern.ask
import akka.testkit._

object LookupRemoteActorMultiJvmSpec extends MultiNodeConfig {

  commonConfig(debugConfig(on = false).withFallback(RemotingMultiNodeSpec.commonConfig))

  val leader = role("leader")
  val follower = role("follower")

}

class LookupRemoteActorMultiJvmNode1 extends LookupRemoteActorSpec
class LookupRemoteActorMultiJvmNode2 extends LookupRemoteActorSpec

object LookupRemoteActorSpec {
  class SomeActor extends Actor {
    def receive = {
      case "identify" => sender() ! self
    }
  }
}

abstract class LookupRemoteActorSpec extends RemotingMultiNodeSpec(LookupRemoteActorMultiJvmSpec) {
  import LookupRemoteActorMultiJvmSpec._
  import LookupRemoteActorSpec._

  def initialParticipants = 2

  runOn(leader) {
    system.actorOf(Props[SomeActor](), "service-hello")
  }

  "Remoting" must {
    "lookup remote actor" taggedAs LongRunningTest in {
      runOn(follower) {
        val hello = {
          system.actorSelection(node(leader) / "user" / "service-hello") ! Identify("id1")
          expectMsgType[ActorIdentity].ref.get
        }
        hello.isInstanceOf[RemoteActorRef] should ===(true)
        val masterAddress = testConductor.getAddressFor(leader).await
        (hello ? "identify").await.asInstanceOf[ActorRef].path.address should ===(masterAddress)
      }
      enterBarrier("done")
    }
  }

}
