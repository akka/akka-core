/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.Address
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.TestMember
import akka.testkit.AkkaSpec
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender
import akka.testkit.WithLogCapturing

class ReplicatorMembershipSpec extends AkkaSpec("""
      akka.actor.provider = "cluster"
      akka.remote.artery.canonical.port = 0
      akka.loglevel = DEBUG
      akka.loggers = ["akka.testkit.TestEventListener"]
      """) with WithLogCapturing with ImplicitSender {

  private def member(host: String, upNumber: Int): Member =
    TestMember(Address("akka", system.name, host, 2552), MemberStatus.Up, upNumber = upNumber, dc = "default")

  "Replicator membership tracking" must {

    "keep membersByAge consistent when upNumber changes (preferOldest)" in {
      val replicator =
        system.actorOf(Replicator.props(ReplicatorSettings(system).withPreferOldest(true)), "preferOldestReplicator")

      val memberA1 = member("a", upNumber = 3)
      val memberB = member("b", upNumber = 5)
      // Same node as memberA1, but with a different upNumber (as can happen
      // when a node transitions through Joining/WeaklyUp -> Up).
      val memberA2 =
        TestMember.withUniqueAddress(memberA1.uniqueAddress, MemberStatus.Up, Set.empty, "default", upNumber = 7)

      EventFilter
        .debug(start = s"MemberUp [${memberA1.uniqueAddress}], membersByAge size [1]", occurrences = 1)
        .intercept {
          replicator ! MemberUp(memberA1)
        }

      EventFilter
        .debug(start = s"MemberUp [${memberB.uniqueAddress}], membersByAge size [2]", occurrences = 1)
        .intercept {
          replicator ! MemberUp(memberB)
        }

      // Re-send MemberUp for node A with a different upNumber.
      EventFilter
        .debug(start = s"MemberUp [${memberA2.uniqueAddress}], membersByAge size [2]", occurrences = 1)
        .intercept {
          replicator ! MemberUp(memberA2)
        }

      // Remove node A.
      EventFilter
        .debug(start = s"MemberRemoved [${memberA2.uniqueAddress}], membersByAge size [1]", occurrences = 1)
        .intercept {
          replicator ! MemberRemoved(memberA2.copy(MemberStatus.Removed), MemberStatus.Exiting)
        }

      EventFilter
        .debug(start = s"MemberRemoved [${memberB.uniqueAddress}], membersByAge size [0]", occurrences = 1)
        .intercept {
          replicator ! MemberRemoved(memberB.copy(MemberStatus.Removed), MemberStatus.Exiting)
        }

      system.stop(replicator)
    }
  }
}
