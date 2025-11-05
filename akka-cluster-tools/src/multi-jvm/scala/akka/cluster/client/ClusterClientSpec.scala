/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster.client

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.client.ClusterClientSpec.TestClientListener.LatestContactPoints
import akka.cluster.client.ClusterClientSpec.TestReceptionistListener.LatestClusterClients
import akka.cluster.pubsub._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._
import akka.util.Timeout
import akka.util.unused

object ClusterClientSpec extends MultiNodeConfig {
  val client = role("client")
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = 0s
    akka.cluster.client.heartbeat-interval = 1s
    akka.cluster.client.acceptable-heartbeat-pause = 3s
    akka.cluster.client.refresh-contacts-interval = 1s
    # number-of-contacts must be >= 4 because we shutdown all but one in the end
    akka.cluster.client.receptionist.number-of-contacts = 4
    akka.cluster.client.receptionist.heartbeat-interval = 10s
    akka.cluster.client.receptionist.acceptable-heartbeat-pause = 10s
    akka.cluster.client.receptionist.failure-detection-interval = 1s
    akka.test.filter-leeway = 10s
    """))

  testTransport(on = true)

  case class Reply(msg: Any, node: Address) extends JavaSerializable

  class TestService(testActor: ActorRef) extends Actor {
    def receive = {
      case "shutdown" =>
        context.system.terminate()
      case msg =>
        testActor.forward(msg)
        sender() ! Reply(s"$msg-ack", Cluster(context.system).selfAddress)
    }
  }

  class Service extends Actor {
    def receive = {
      case msg => sender() ! msg
    }
  }

  //#clientEventsListener
  class ClientListener(targetClient: ActorRef) extends Actor {
    override def preStart(): Unit =
      targetClient ! SubscribeContactPoints

    def receive: Receive =
      receiveWithContactPoints(Set.empty)

    def receiveWithContactPoints(contactPoints: Set[ActorPath]): Receive = {
      case ContactPoints(cps) =>
        context.become(receiveWithContactPoints(cps))
      // Now do something with the up-to-date "cps"
      case ContactPointAdded(cp) =>
        context.become(receiveWithContactPoints(contactPoints + cp))
      // Now do something with an up-to-date "contactPoints + cp"
      case ContactPointRemoved(cp) =>
        context.become(receiveWithContactPoints(contactPoints - cp))
      // Now do something with an up-to-date "contactPoints - cp"
    }
  }
  //#clientEventsListener

  object TestClientListener {
    case object GetLatestContactPoints
    case class LatestContactPoints(contactPoints: Set[ActorPath]) extends NoSerializationVerificationNeeded
  }

  class TestClientListener(targetClient: ActorRef) extends ClientListener(targetClient) {

    import TestClientListener._

    override def receiveWithContactPoints(contactPoints: Set[ActorPath]): Receive = {
      case GetLatestContactPoints =>
        sender() ! LatestContactPoints(contactPoints)
      case msg: Any =>
        super.receiveWithContactPoints(contactPoints)(msg)
    }
  }

  //#receptionistEventsListener
  class ReceptionistListener(targetReceptionist: ActorRef) extends Actor {
    override def preStart(): Unit =
      targetReceptionist ! SubscribeClusterClients

    def receive: Receive =
      receiveWithClusterClients(Set.empty)

    def receiveWithClusterClients(clusterClients: Set[ActorRef]): Receive = {
      case ClusterClients(cs) =>
        context.become(receiveWithClusterClients(cs))
      // Now do something with the up-to-date "c"
      case ClusterClientUp(c) =>
        context.become(receiveWithClusterClients(clusterClients + c))
      // Now do something with an up-to-date "clusterClients + c"
      case ClusterClientUnreachable(c) =>
        context.become(receiveWithClusterClients(clusterClients - c))
      // Now do something with an up-to-date "clusterClients - c"
    }
  }
  //#receptionistEventsListener

  object TestReceptionistListener {
    case object GetLatestClusterClients
    case class LatestClusterClients(clusterClients: Set[ActorRef]) extends NoSerializationVerificationNeeded
  }

  class TestReceptionistListener(targetReceptionist: ActorRef) extends ReceptionistListener(targetReceptionist) {

    import TestReceptionistListener._

    override def receiveWithClusterClients(clusterClients: Set[ActorRef]): Receive = {
      case GetLatestClusterClients =>
        sender() ! LatestClusterClients(clusterClients)
      case msg: Any =>
        super.receiveWithClusterClients(clusterClients)(msg)
    }
  }
}

class ClusterClientMultiJvmNode1 extends ClusterClientSpec
class ClusterClientMultiJvmNode2 extends ClusterClientSpec
class ClusterClientMultiJvmNode3 extends ClusterClientSpec
class ClusterClientMultiJvmNode4 extends ClusterClientSpec
class ClusterClientMultiJvmNode5 extends ClusterClientSpec

@nowarn("msg=deprecated")
class ClusterClientSpec extends MultiNodeSpec(ClusterClientSpec) with STMultiNodeSpec with ImplicitSender {
  import ClusterClientSpec._

  override def initialParticipants = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system).join(node(to).address)
      createReceptionist()
    }
    enterBarrier(from.name + "-joined")
  }

  def createReceptionist(): Unit = ClusterClientReceptionist(system)

  def awaitCount(expected: Int): Unit = {
    awaitAssert {
      DistributedPubSub(system).mediator ! DistributedPubSubMediator.Count
      val actual = expectMsgType[Int]
      actual should ===(expected)
    }
  }

  var remainingServerRoleNames = Set(first, second, third, fourth)

  def roleName(addr: Address): Option[RoleName] = remainingServerRoleNames.find(node(_).address == addr)

  def initialContacts = (remainingServerRoleNames - first - fourth).map { r =>
    node(r) / "system" / "receptionist"
  }

  @unused
  def docOnly = { //not used, only demo
    //#initialContacts
    val initialContacts = Set(
      ActorPath.fromString("akka://OtherSys@host1:2552/system/receptionist"),
      ActorPath.fromString("akka://OtherSys@host2:2552/system/receptionist"))
    val settings = ClusterClientSettings(system).withInitialContacts(initialContacts)
    //#initialContacts

    // make the compiler happy and thinking we use it
    settings.acceptableHeartbeatPause
  }

  "A ClusterClient" must {

    "startup cluster" in within(30 seconds) {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)
      runOn(fourth) {
        val service = system.actorOf(Props(classOf[TestService], testActor), "testService")
        ClusterClientReceptionist(system).registerService(service)
      }
      runOn(first, second, third, fourth) {
        awaitCount(1)
      }

      enterBarrier("after-1")
    }

    "communicate to actor on any node in cluster" in within(10 seconds) {
      runOn(client) {
        val c = system.actorOf(
          ClusterClient.props(ClusterClientSettings(system).withInitialContacts(initialContacts)),
          "client1")
        c ! ClusterClient.Send("/user/testService", "hello", localAffinity = true)
        expectMsgType[Reply].msg should be("hello-ack")
        system.stop(c)
      }
      runOn(fourth) {
        expectMsg("hello")
      }

      enterBarrier("after-2")
    }

    "work with ask" in within(10 seconds) {
      runOn(client) {
        import akka.pattern.ask
        val c = system.actorOf(
          ClusterClient.props(ClusterClientSettings(system).withInitialContacts(initialContacts)),
          "ask-client")
        implicit val timeout: Timeout = Timeout(remaining)
        val reply = c ? ClusterClient.Send("/user/testService", "hello-request", localAffinity = true)
        Await.result(reply.mapTo[Reply], remaining).msg should be("hello-request-ack")
        system.stop(c)
      }
      runOn(fourth) {
        expectMsg("hello-request")
      }

      enterBarrier("after-3")
    }

    "demonstrate usage" in within(15 seconds) {
      def host1 = first
      def host2 = second
      def host3 = third

      //#server
      runOn(host1) {
        val serviceA = system.actorOf(Props[Service](), "serviceA")
        ClusterClientReceptionist(system).registerService(serviceA)
      }

      runOn(host2, host3) {
        val serviceB = system.actorOf(Props[Service](), "serviceB")
        ClusterClientReceptionist(system).registerService(serviceB)
      }
      //#server

      runOn(host1, host2, host3, fourth) {
        awaitCount(4)
      }
      enterBarrier("services-replicated")

      //#client
      runOn(client) {
        val c = system.actorOf(
          ClusterClient.props(ClusterClientSettings(system).withInitialContacts(initialContacts)),
          "client")
        c ! ClusterClient.Send("/user/serviceA", "hello", localAffinity = true)
        c ! ClusterClient.SendToAll("/user/serviceB", "hi")
      }
      //#client

      runOn(client) {
        // note that "hi" was sent to 2 "serviceB"
        receiveN(3).toSet should ===(Set("hello", "hi"))
      }

      // strange, barriers fail without this sleep
      Thread.sleep(1000)
      enterBarrier("after-4")
    }

    "report events" in within(15 seconds) {
      runOn(client) {
        implicit val timeout: Timeout = Timeout(1.second.dilated)
        val client = Await.result(system.actorSelection("/user/client").resolveOne(), timeout.duration)
        val listener = system.actorOf(Props(classOf[TestClientListener], client), "reporter-client-listener")

        val expectedContacts = Set(first, second, third, fourth).map(node(_) / "system" / "receptionist")
        awaitAssert({
          listener ! TestClientListener.GetLatestContactPoints
          expectMsgType[LatestContactPoints].contactPoints should ===(expectedContacts)
        }, max = 10.seconds)
      }

      enterBarrier("reporter-client-listener-tested")

      runOn(first, second, third) {
        // Only run this test on a node that knows about our client. It could be that no node knows
        // but there isn't a means of expressing that at least one of the nodes needs to pass the test.
        implicit val timeout: Timeout = Timeout(2.seconds.dilated)
        val r = ClusterClientReceptionist(system).underlying
        r ! GetClusterClients
        val cps = expectMsgType[ClusterClients]
        if (cps.clusterClients.exists(_.path.name == "client")) {
          log.info("Testing that the receptionist has just one client")
          val l = system.actorOf(Props(classOf[TestReceptionistListener], r), "reporter-receptionist-listener")

          val expectedClient =
            Await.result(system.actorSelection(node(client) / "user" / "client").resolveOne(), timeout.duration)
          awaitAssert({
            val probe = TestProbe()
            l.tell(TestReceptionistListener.GetLatestClusterClients, probe.ref)
            // "ask-client" might still be around, filter
            probe.expectMsgType[LatestClusterClients].clusterClients should contain(expectedClient)
          }, max = 10.seconds)
        }
      }

      enterBarrier("after-5")
    }

    "report a removal of a receptionist" in within(10 seconds) {
      runOn(client) {
        val unreachableContact = node(client) / "system" / "receptionist"
        val expectedRoles = Set(first, second, third, fourth)
        val expectedContacts = expectedRoles.map(node(_) / "system" / "receptionist")

        // We need to slow down things otherwise our receptionists can sometimes tell us
        // that our unreachableContact is unreachable before we get a chance to
        // subscribe to events.
        expectedRoles.foreach { role =>
          testConductor.blackhole(client, role, Direction.Both).await
        }

        val c = system.actorOf(
          ClusterClient.props(ClusterClientSettings(system).withInitialContacts(expectedContacts + unreachableContact)),
          "client5")

        val probe = TestProbe()
        c.tell(SubscribeContactPoints, probe.ref)

        expectedRoles.foreach { role =>
          testConductor.passThrough(client, role, Direction.Both).await
        }

        probe.fishForMessage(10.seconds, "removal") {
          case ContactPointRemoved(`unreachableContact`) => true
          case _                                         => false
        }
      }
      enterBarrier("after-7")
    }

  }
}
