/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.typed

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.testkit.SocketUtil
import scala.annotation.nowarn
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
//#cluster-imports
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import akka.cluster.typed._
//#cluster-imports
import akka.actor.testkit.typed.scaladsl.TestProbe
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.time.{ Millis, Seconds, Span }

import scala.concurrent.duration._

@nowarn("msg=Use Akka Distributed Cluster")
object BasicClusterExampleSpec {
  val configSystem1 = ConfigFactory.parseString("""
akka.loglevel = DEBUG
#config-seeds
akka {
  actor {
    provider = "cluster"
  }
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2551
    }
  }

  cluster {
    seed-nodes = [
      "akka://ClusterSystem@127.0.0.1:2551",
      "akka://ClusterSystem@127.0.0.1:2552"]
    
    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
  }
}
#config-seeds
     """)

  val configSystem2 = ConfigFactory.parseString("""
        akka.remote.artery.canonical.port = 0
     """).withFallback(configSystem1)

  def illustrateJoinSeedNodes(): Unit = {
    val system: ActorSystem[_] = ???

    //#join-seed-nodes
    import akka.actor.Address
    import akka.actor.AddressFromURIString
    import akka.cluster.typed.JoinSeedNodes

    val seedNodes: List[Address] =
      List("akka://ClusterSystem@127.0.0.1:2551", "akka://ClusterSystem@127.0.0.1:2552").map(AddressFromURIString.parse)
    Cluster(system).manager ! JoinSeedNodes(seedNodes)
    //#join-seed-nodes
  }

  object Backend {
    def apply(): Behavior[_] = Behaviors.empty
  }

  object Frontend {
    def apply(): Behavior[_] = Behaviors.empty
  }

  def illustrateRoles(): Unit = {
    val context: ActorContext[_] = ???

    //#hasRole
    val selfMember = Cluster(context.system).selfMember
    if (selfMember.hasRole("backend")) {
      context.spawn(Backend(), "back")
    } else if (selfMember.hasRole("frontend")) {
      context.spawn(Frontend(), "front")
    }
    //#hasRole
  }

  @nowarn("msg=never used")
  def illustrateDcAccess(): Unit = {
    val system: ActorSystem[_] = ???

    //#dcAccess
    val cluster = Cluster(system)
    // this node's data center
    val dc = cluster.selfMember.dataCenter
    // all known data centers
    val allDc = cluster.state.allDataCenters
    // a specific member's data center
    val aMember = cluster.state.members.head
    val aDc = aMember.dataCenter
    //#dcAccess
  }
}

class BasicClusterConfigSpec extends AnyWordSpec with ScalaFutures with Eventually with Matchers with LogCapturing {
  import BasicClusterExampleSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(100, Millis)))

  "Cluster API" must {
    "init cluster" in {
      // config is pulled into docs, but we don't want to hardcode ports because that makes for brittle tests
      val addresses = SocketUtil.temporaryServerAddresses(2)
      val sys1Port = addresses.head.getPort
      val sys2Port = addresses.last.getPort
      def config(port: Int) = ConfigFactory.parseString(s"""
          akka.remote.artery.canonical.port = $port
          akka.cluster.jmx.multi-mbeans-in-same-jvm = on
          akka.cluster.seed-nodes = [ "akka://ClusterSystem@127.0.0.1:$sys1Port", "akka://ClusterSystem@127.0.0.1:$sys2Port" ]
        """)

      val system1 =
        ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", config(sys1Port).withFallback(configSystem1))
      val system2 =
        ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", config(sys2Port).withFallback(configSystem2))
      try {
        Cluster(system1)
        Cluster(system2)
      } finally {
        ActorTestKit.shutdown(system1)
        ActorTestKit.shutdown(system2)
      }

    }
  }
}

object BasicClusterManualSpec {
  val clusterConfig = ConfigFactory.parseString("""
akka.loglevel = DEBUG
akka.cluster.jmx.multi-mbeans-in-same-jvm = on
#config
akka {
  actor.provider = "cluster"
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2551
    }
  }
}
#config
     """)

  val noPort = ConfigFactory.parseString("""
      akka.remote.artery.canonical.port = 0
    """)

}

class BasicClusterManualSpec extends AnyWordSpec with ScalaFutures with Eventually with Matchers with LogCapturing {

  import BasicClusterManualSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(100, Millis)))

  "Cluster API" must {
    "init cluster" in {

      val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", noPort.withFallback(clusterConfig))
      val system2 = ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", noPort.withFallback(clusterConfig))

      try {
        //#cluster-create
        val cluster = Cluster(system)
        //#cluster-create
        val cluster2 = Cluster(system2)

        //#cluster-join
        cluster.manager ! Join(cluster.selfMember.address)
        //#cluster-join
        cluster2.manager ! Join(cluster.selfMember.address)

        eventually {
          cluster.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up)
          cluster2.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up)
        }

        //#cluster-leave
        cluster2.manager ! Leave(cluster2.selfMember.address)
        //#cluster-leave

        eventually {
          cluster.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up)
          cluster2.isTerminated shouldEqual true
        }
      } finally {
        ActorTestKit.shutdown(system)
        ActorTestKit.shutdown(system2)
      }
    }

    "subscribe to cluster events" in {
      implicit val system1 =
        ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", noPort.withFallback(clusterConfig))
      val system2 = ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", noPort.withFallback(clusterConfig))
      val system3 = ActorSystem[Nothing](Behaviors.empty[Nothing], "ClusterSystem", noPort.withFallback(clusterConfig))

      try {
        val cluster1 = Cluster(system1)
        val cluster2 = Cluster(system2)
        val cluster3 = Cluster(system3)
        def cluster = cluster1

        val probe1 = TestProbe[MemberEvent]()(system1)
        val subscriber = probe1.ref
        //#cluster-subscribe
        cluster.subscriptions ! Subscribe(subscriber, classOf[MemberEvent])
        //#cluster-subscribe

        cluster1.manager ! Join(cluster1.selfMember.address)
        eventually {
          cluster1.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up)
        }
        probe1.expectMessage(MemberUp(cluster1.selfMember))

        cluster2.manager ! Join(cluster1.selfMember.address)
        probe1.within(10.seconds) {
          probe1.expectMessageType[MemberJoined].member.address shouldEqual cluster2.selfMember.address
          probe1.expectMessageType[MemberUp].member.address shouldEqual cluster2.selfMember.address
        }
        eventually {
          cluster1.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up)
          cluster2.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up)
        }

        cluster3.manager ! Join(cluster1.selfMember.address)
        probe1.within(10.seconds) {
          probe1.expectMessageType[MemberJoined].member.address shouldEqual cluster3.selfMember.address
          probe1.expectMessageType[MemberUp].member.address shouldEqual cluster3.selfMember.address
        }
        eventually {
          cluster1.state.members.toList
            .map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up, MemberStatus.up)
          cluster2.state.members.toList
            .map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up, MemberStatus.up)
          cluster3.state.members.toList
            .map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up, MemberStatus.up)
        }

        val anotherMemberAddress = cluster2.selfMember.address
        //#cluster-leave-example
        cluster.manager ! Leave(anotherMemberAddress)
        // subscriber will receive events MemberLeft, MemberExited and MemberRemoved
        //#cluster-leave-example
        probe1.within(10.seconds) {
          probe1.expectMessageType[MemberLeft].member.address shouldEqual cluster2.selfMember.address
          probe1.expectMessageType[MemberExited].member.address shouldEqual cluster2.selfMember.address
          probe1.expectMessageType[MemberRemoved].member.address shouldEqual cluster2.selfMember.address
        }

        eventually {
          cluster1.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up)
          cluster3.state.members.toList.map(_.status) shouldEqual List(MemberStatus.up, MemberStatus.up)
        }

        eventually {
          cluster2.isTerminated should ===(true)
        }
        // via coordinated shutdown
        system2.whenTerminated.futureValue

        system1.log.info("Downing node 3")
        cluster1.manager ! Down(cluster3.selfMember.address)
        probe1.expectMessageType[MemberDowned].member.address shouldEqual cluster3.selfMember.address
        probe1.expectMessageType[MemberRemoved](10.seconds).member.address shouldEqual cluster3.selfMember.address

        probe1.expectNoMessage()

        // via coordinated shutdown
        system3.whenTerminated.futureValue

      } finally {
        ActorTestKit.shutdown(system1)
        ActorTestKit.shutdown(system2)
        ActorTestKit.shutdown(system3)
      }
    }
  }
}
