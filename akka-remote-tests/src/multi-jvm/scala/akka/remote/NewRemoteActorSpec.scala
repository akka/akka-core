/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps
import testkit.MultiNodeConfig

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated

object NewRemoteActorMultiJvmSpec extends MultiNodeConfig {

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.remote.use-unsafe-remote-features-outside-cluster = on
      """).withFallback(RemotingMultiNodeSpec.commonConfig)))

  val leader = role("leader")
  val follower = role("follower")

  deployOn(leader, """
    /service-hello.remote = "@follower@"
    /service-hello-null.remote = "@follower@"
    /service-hello3.remote = "@follower@"
    """)

  deployOnAll("""/service-hello2.remote = "@follower@" """)
}

class NewRemoteActorMultiJvmNode1 extends NewRemoteActorSpec
class NewRemoteActorMultiJvmNode2 extends NewRemoteActorSpec

object NewRemoteActorSpec {
  class SomeActor extends Actor {
    def receive = {
      case "identify" => sender() ! self
    }
  }

  class SomeActorWithParam(@nowarn("msg=never used") ignored: String) extends Actor {
    def receive = {
      case "identify" => sender() ! self
    }
  }
}

abstract class NewRemoteActorSpec extends RemotingMultiNodeSpec(NewRemoteActorMultiJvmSpec) {
  import NewRemoteActorMultiJvmSpec._
  import NewRemoteActorSpec._

  def initialParticipants = roles.size

  // ensure that system.terminate is successful
  override def verifySystemShutdown = true

  "A new remote actor" must {
    "be locally instantiated on a remote node and be able to communicate through its RemoteActorRef" in {

      runOn(leader) {
        val actor = system.actorOf(Props[SomeActor](), "service-hello")
        actor.isInstanceOf[RemoteActorRef] should ===(true)
        actor.path.address should ===(node(follower).address)

        val followerAddress = testConductor.getAddressFor(follower).await
        actor ! "identify"
        expectMsgType[ActorRef].path.address should ===(followerAddress)
      }

      enterBarrier("done")
    }

    "be locally instantiated on a remote node (with null parameter) and be able to communicate through its RemoteActorRef" in {

      runOn(leader) {
        val actor = system.actorOf(Props(classOf[SomeActorWithParam], null), "service-hello-null")
        actor.isInstanceOf[RemoteActorRef] should ===(true)
        actor.path.address should ===(node(follower).address)

        val followerAddress = testConductor.getAddressFor(follower).await
        actor ! "identify"
        expectMsgType[ActorRef].path.address should ===(followerAddress)
      }

      enterBarrier("done")
    }

    "be locally instantiated on a remote node and be able to communicate through its RemoteActorRef (with deployOnAll)" in {

      runOn(leader) {
        val actor = system.actorOf(Props[SomeActor](), "service-hello2")
        actor.isInstanceOf[RemoteActorRef] should ===(true)
        actor.path.address should ===(node(follower).address)

        val followerAddress = testConductor.getAddressFor(follower).await
        actor ! "identify"
        expectMsgType[ActorRef].path.address should ===(followerAddress)
      }

      enterBarrier("done")
    }

    "be able to shutdown system when using remote deployed actor" in within(20 seconds) {
      runOn(leader) {
        val actor = system.actorOf(Props[SomeActor](), "service-hello3")
        actor.isInstanceOf[RemoteActorRef] should ===(true)
        actor.path.address should ===(node(follower).address)
        // This watch is in race with the shutdown of the watched system. This race should remain, as the test should
        // handle both cases:
        //  - remote system receives watch, replies with DeathWatchNotification
        //  - remote system never gets watch, but DeathWatch heartbeats time out, and AddressTerminated is generated
        //    (this needs some time to happen)
        watch(actor)

        enterBarrier("deployed")

        // master system is supposed to be shutdown after follower
        // this should be triggered by follower system.terminate
        expectMsgPF() { case Terminated(`actor`) => true }
      }

      runOn(follower) {
        enterBarrier("deployed")
      }

      // Important that this is the last test.
      // It should not be any barriers here.
      // verifySystemShutdown = true will ensure that system.terminate is successful
    }
  }
}
