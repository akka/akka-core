/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testconductor

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.Deploy
import akka.actor.Identify
import akka.actor.Props
import akka.remote.RemotingMultiNodeSpec
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.testkit.LongRunningTest

object TestConductorMultiJvmSpec extends MultiNodeConfig {
  commonConfig(debugConfig(on = false).withFallback(RemotingMultiNodeSpec.commonConfig))

  val leader = role("leader")
  val follower = role("follower")

  testTransport(on = true)
}

class TestConductorMultiJvmNode1 extends TestConductorSpec
class TestConductorMultiJvmNode2 extends TestConductorSpec

class TestConductorSpec extends RemotingMultiNodeSpec(TestConductorMultiJvmSpec) {

  import TestConductorMultiJvmSpec._

  def initialParticipants = 2

  lazy val echo = {
    system.actorSelection(node(leader) / "user" / "echo") ! Identify(None)
    expectMsgType[ActorIdentity].ref.get
  }

  "A TestConductor" must {

    "enter a barrier" taggedAs LongRunningTest in {
      runOn(leader) {
        system.actorOf(Props(new Actor {
          def receive = {
            case x => testActor ! x; sender() ! x
          }
        }).withDeploy(Deploy.local), "echo")
      }

      enterBarrier("name")
    }

    "support blackhole of network connections" taggedAs LongRunningTest in {

      runOn(follower) {
        // start remote network connection so that it can be blackholed
        echo ! "start"
      }

      expectMsg("start")

      runOn(leader) {
        testConductor.blackhole(follower, leader, Direction.Both).await
      }

      enterBarrier("blackholed")

      runOn(follower) {
        for (i <- 0 to 9) echo ! i
      }

      expectNoMessage(1.second)

      enterBarrier("blackholed2")

      runOn(leader) {
        testConductor.passThrough(follower, leader, Direction.Both).await
      }
      enterBarrier("passThrough")

      runOn(follower) {
        for (i <- 10 to 19) echo ! i
      }

      receiveN(10)

      enterBarrier("after")
    }

  }

}
