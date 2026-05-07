/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import akka.actor._
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object PiercingShouldKeepQuarantineConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(debugConfig(on = false).withFallback(RemotingMultiNodeSpec.commonConfig))

}

class PiercingShouldKeepQuarantineSpecMultiJvmNode1 extends PiercingShouldKeepQuarantineSpec
class PiercingShouldKeepQuarantineSpecMultiJvmNode2 extends PiercingShouldKeepQuarantineSpec

object PiercingShouldKeepQuarantineSpec {
  class Subject extends Actor {
    def receive = {
      case "getuid" => sender() ! context.system.asInstanceOf[ExtendedActorSystem].uid
    }
  }
}

abstract class PiercingShouldKeepQuarantineSpec extends RemotingMultiNodeSpec(PiercingShouldKeepQuarantineConfig) {
  import PiercingShouldKeepQuarantineConfig._
  import PiercingShouldKeepQuarantineSpec._

  override def initialParticipants = roles.size

  "While probing through the quarantine remoting" must {

    "not lose existing quarantine marker" taggedAs LongRunningTest in {
      runOn(first) {
        enterBarrier("actors-started")

        // Communicate with second system
        system.actorSelection(node(second) / "user" / "subject") ! "getuid"
        val uid = expectMsgType[Long](10.seconds)
        enterBarrier("actor-identified")

        // Manually Quarantine the other system
        RARP(system).provider.transport.quarantine(node(second).address, Some(uid), "test")

        // Quarantining is not immediate
        Thread.sleep(1000)

        // Quarantine is up -- Should not be able to communicate with remote system any more
        for (_ <- 1 to 4) {
          system.actorSelection(node(second) / "user" / "subject") ! "getuid"
          expectNoMessage(2.seconds)
        }

        enterBarrier("quarantine-intact")

      }

      runOn(second) {
        system.actorOf(Props[Subject](), "subject")
        enterBarrier("actors-started")
        enterBarrier("actor-identified")
        enterBarrier("quarantine-intact")
      }

    }

  }
}
