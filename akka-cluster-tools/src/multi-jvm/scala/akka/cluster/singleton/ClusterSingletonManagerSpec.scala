/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps
import scala.annotation.nowarn

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Identify
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.serialization.jackson.CborSerializable
import akka.testkit._
import akka.testkit.TestEvent._

object ClusterSingletonManagerSpec extends MultiNodeConfig {
  val controller = role("controller")
  val observer = role("observer")
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
  val sixth = role("sixth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = 0s
    akka.remote.artery.advanced.aeron.idle-cpu-level = 3
    """))

  nodeConfig(first, second, third, fourth, fifth, sixth)(ConfigFactory.parseString("akka.cluster.roles =[worker]"))

  //#singleton-message-classes
  object PointToPointChannel {
    case object UnregistrationOk extends CborSerializable
    //#singleton-message-classes
    case object RegisterConsumer extends CborSerializable
    case object UnregisterConsumer extends CborSerializable
    case object RegistrationOk extends CborSerializable
    case object UnexpectedRegistration extends CborSerializable
    case object UnexpectedUnregistration extends CborSerializable
    case object Reset extends CborSerializable
    case object ResetOk extends CborSerializable
    //#singleton-message-classes
  }
  //#singleton-message-classes

  /**
   * This channel is extremely strict with regards to
   * registration and unregistration of consumer to
   * be able to detect misbehavior (e.g. two active
   * singleton instances).
   */
  class PointToPointChannel extends Actor with ActorLogging {

    import PointToPointChannel._

    def receive = idle

    def idle: Receive = {
      case RegisterConsumer =>
        log.info("RegisterConsumer: [{}]", sender().path)
        sender() ! RegistrationOk
        context.become(active(sender()))
      case UnregisterConsumer =>
        log.info("UnexpectedUnregistration: [{}]", sender().path)
        sender() ! UnexpectedUnregistration
        context.stop(self)
      case Reset => sender() ! ResetOk
      case _     => // no consumer, drop
    }

    def active(consumer: ActorRef): Receive = {
      case UnregisterConsumer if sender() == consumer =>
        log.info("UnregistrationOk: [{}]", sender().path)
        sender() ! UnregistrationOk
        context.become(idle)
      case UnregisterConsumer =>
        log.info("UnexpectedUnregistration: [{}], expected [{}]", sender().path, consumer.path)
        sender() ! UnexpectedUnregistration
        context.stop(self)
      case RegisterConsumer =>
        log.info("Unexpected RegisterConsumer [{}], active consumer [{}]", sender().path, consumer.path)
        sender() ! UnexpectedRegistration
        context.stop(self)
      case Reset =>
        context.become(idle)
        sender() ! ResetOk
      case msg => consumer ! msg
    }
  }

  //#singleton-message-classes
  object Consumer {
    case object End extends CborSerializable
    case object GetCurrent extends CborSerializable
    case object Ping extends CborSerializable
    case object Pong extends CborSerializable
  }
  //#singleton-message-classes

  /**
   * The Singleton actor
   */
  class Consumer(queue: ActorRef, delegateTo: ActorRef) extends Actor with ActorLogging {

    import Consumer._
    import PointToPointChannel._

    var current = 0
    var stoppedBeforeUnregistration = true

    override def preStart(): Unit = queue ! RegisterConsumer

    override def postStop(): Unit = {
      if (stoppedBeforeUnregistration)
        log.warning("Stopped before unregistration")
    }

    def receive = {
      case n: Int if n <= current =>
        context.stop(self)
      case n: Int =>
        current = n
        delegateTo ! n
      case message @ (RegistrationOk | UnexpectedRegistration) =>
        delegateTo ! message
      case GetCurrent =>
        sender() ! current
      //#consumer-end
      case End =>
        queue ! UnregisterConsumer
      case UnregistrationOk =>
        stoppedBeforeUnregistration = false
        context.stop(self)
      case Ping =>
        sender() ! Pong
      //#consumer-end
    }
  }

}

class ClusterSingletonManagerMultiJvmNode1 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode2 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode3 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode4 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode5 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode6 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode7 extends ClusterSingletonManagerSpec
class ClusterSingletonManagerMultiJvmNode8 extends ClusterSingletonManagerSpec

@nowarn("msg=Use Akka Distributed Cluster")
class ClusterSingletonManagerSpec
    extends MultiNodeSpec(ClusterSingletonManagerSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import ClusterSingletonManagerSpec._
  import ClusterSingletonManagerSpec.Consumer._
  import ClusterSingletonManagerSpec.PointToPointChannel._

  override def initialParticipants = roles.size

  val identifyProbe = TestProbe()

  val controllerRootActorPath = node(controller)

  var _msg = 0

  def msg(): Int = {
    _msg += 1
    _msg
  }

  def queue: ActorRef = {
    // this is used from inside actor construction, i.e. other thread, and must therefore not call `node(controller`
    system.actorSelection(controllerRootActorPath / "user" / "queue").tell(Identify("queue"), identifyProbe.ref)
    identifyProbe.expectMsgType[ActorIdentity].ref.get
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system).join(node(to).address)
      if (Cluster(system).selfRoles.contains("worker")) {
        createSingleton()
        createSingletonProxy()
      }
    }
  }

  def awaitMemberUp(memberProbe: TestProbe, nodes: RoleName*): Unit = {
    runOn(nodes.filterNot(_ == nodes.head): _*) {
      memberProbe.expectMsgType[MemberUp](15.seconds).member.address should ===(node(nodes.head).address)
    }
    runOn(nodes.head) {
      memberProbe.receiveN(nodes.size, 15.seconds).collect { case MemberUp(m) => m.address }.toSet should ===(
        nodes.map(node(_).address).toSet)
    }
    enterBarrier(nodes.head.name + "-up")
  }

  def createSingleton(): ActorRef = {
    //#create-singleton-manager
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(classOf[Consumer], queue, testActor),
        terminationMessage = End,
        settings = ClusterSingletonManagerSettings(system).withRole("worker")),
      name = "consumer")
    //#create-singleton-manager
  }

  def createSingletonProxy(): ActorRef = {
    //#create-singleton-proxy
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = "/user/consumer",
        settings = ClusterSingletonProxySettings(system).withRole("worker")),
      name = "consumerProxy")
    //#create-singleton-proxy
    proxy
  }

  def createSingletonProxyDc(): ActorRef = {
    //#create-singleton-proxy-dc
    val proxyDcB = system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = "/user/consumer",
        settings = ClusterSingletonProxySettings(system).withDataCenter("B")),
      name = "consumerProxyDcB")
    //#create-singleton-proxy-dc
    proxyDcB
  }

  def verifyProxyMsg(oldest: RoleName, proxyNode: RoleName, msg: Int): Unit = {
    enterBarrier("before-" + msg + "-proxy-verified")

    // send a message to the proxy
    runOn(proxyNode) {
      // make sure that the proxy has received membership changes
      // and points to the current singleton
      val p = TestProbe()
      val oldestAddress = node(oldest).address
      within(10.seconds) {
        awaitAssert {
          system.actorSelection("/user/consumerProxy").tell(Ping, p.ref)
          p.expectMsg(1.second, Pong)
          val replyFromAddress = p.lastSender.path.address
          if (oldest == proxyNode)
            replyFromAddress.hasLocalScope should ===(true)
          else
            replyFromAddress should ===(oldestAddress)
        }
      }
      // then send the real message
      system.actorSelection("/user/consumerProxy") ! msg
    }

    enterBarrier(s"sent-msg-$msg")

    // expect a message on the oldest node
    runOn(oldest) {
      expectMsg(msg)
    }

    enterBarrier("after-" + msg + "-proxy-verified")
  }

  def consumer(oldest: RoleName): ActorSelection =
    system.actorSelection(RootActorPath(node(oldest).address) / "user" / "consumer" / "singleton")

  def verifyRegistration(oldest: RoleName): Unit = {
    enterBarrier("before-" + oldest.name + "-registration-verified")
    runOn(oldest) {
      expectMsg(RegistrationOk)
      consumer(oldest) ! GetCurrent
      expectMsg(0)
    }
    enterBarrier("after-" + oldest.name + "-registration-verified")
  }

  def verifyMsg(oldest: RoleName, msg: Int): Unit = {
    enterBarrier("before-" + msg + "-verified")

    runOn(controller) {
      queue ! msg
      // make sure it's not terminated, which would be wrong
      expectNoMessage(1 second)
    }
    runOn(oldest) {
      expectMsg(5.seconds, msg)
    }
    runOn(roles.filterNot(r => r == oldest || r == controller || r == observer): _*) {
      expectNoMessage(1 second)
    }
    enterBarrier("after-" + msg + "-verified")
  }

  def crash(roles: RoleName*): Unit = {
    runOn(controller) {
      queue ! Reset
      expectMsg(ResetOk)
      roles.foreach { r =>
        log.info("Shutdown [{}]", node(r).address)
        testConductor.exit(r, 0).await
      }
    }
  }

  "A ClusterSingletonManager" must {

    "startup 6 node cluster" in within(60 seconds) {

      val memberProbe = TestProbe()
      Cluster(system).subscribe(memberProbe.ref, classOf[MemberUp])
      memberProbe.expectMsgClass(classOf[CurrentClusterState])

      runOn(controller) {
        // watch that it is not terminated, which would indicate misbehavior
        watch(system.actorOf(Props[PointToPointChannel](), "queue"))
      }
      enterBarrier("queue-started")

      join(first, first)
      awaitMemberUp(memberProbe, first)
      verifyRegistration(first)
      verifyMsg(first, msg = msg())

      // join the observer node as well, which should not influence since it doesn't have the "worker" role
      join(observer, first)
      awaitMemberUp(memberProbe, observer, first)
      verifyProxyMsg(first, first, msg = msg())

      join(second, first)
      awaitMemberUp(memberProbe, second, observer, first)
      verifyMsg(first, msg = msg())
      verifyProxyMsg(first, second, msg = msg())

      join(third, first)
      awaitMemberUp(memberProbe, third, second, observer, first)
      verifyMsg(first, msg = msg())
      verifyProxyMsg(first, third, msg = msg())

      join(fourth, first)
      awaitMemberUp(memberProbe, fourth, third, second, observer, first)
      verifyMsg(first, msg = msg())
      verifyProxyMsg(first, fourth, msg = msg())

      join(fifth, first)
      awaitMemberUp(memberProbe, fifth, fourth, third, second, observer, first)
      verifyMsg(first, msg = msg())
      verifyProxyMsg(first, fifth, msg = msg())

      join(sixth, first)
      awaitMemberUp(memberProbe, sixth, fifth, fourth, third, second, observer, first)
      verifyMsg(first, msg = msg())
      verifyProxyMsg(first, sixth, msg = msg())

      enterBarrier("after-1")
    }

    "let the proxy route messages to the singleton in a 6 node cluster" in within(60 seconds) {
      verifyProxyMsg(first, first, msg = msg())
      verifyProxyMsg(first, second, msg = msg())
      verifyProxyMsg(first, third, msg = msg())
      verifyProxyMsg(first, fourth, msg = msg())
      verifyProxyMsg(first, fifth, msg = msg())
      verifyProxyMsg(first, sixth, msg = msg())
    }

    "hand over when oldest leaves in 6 nodes cluster " in within(30 seconds) {
      val leaveRole = first

      runOn(leaveRole) {
        Cluster(system).leave(node(leaveRole).address)
      }

      verifyRegistration(second)
      verifyMsg(second, msg = msg())
      verifyProxyMsg(second, second, msg = msg())
      verifyProxyMsg(second, third, msg = msg())
      verifyProxyMsg(second, fourth, msg = msg())
      verifyProxyMsg(second, fifth, msg = msg())
      verifyProxyMsg(second, sixth, msg = msg())

      runOn(leaveRole) {
        system.actorSelection("/user/consumer").tell(Identify("singleton"), identifyProbe.ref)
        identifyProbe.expectMsgPF() {
          case ActorIdentity("singleton", None) => // already terminated
          case ActorIdentity("singleton", Some(singleton)) =>
            watch(singleton)
            expectTerminated(singleton)
        }
      }

      enterBarrier("after-leave")
    }

    "take over when oldest crashes in 5 nodes cluster" in within(60 seconds) {
      // mute logging of deadLetters during shutdown of systems
      if (!log.isDebugEnabled)
        system.eventStream.publish(Mute(DeadLettersFilter[Any]))
      enterBarrier("logs-muted")

      crash(second)
      verifyRegistration(third)
      verifyMsg(third, msg = msg())
      verifyProxyMsg(third, third, msg = msg())
      verifyProxyMsg(third, fourth, msg = msg())
      verifyProxyMsg(third, fifth, msg = msg())
      verifyProxyMsg(third, sixth, msg = msg())
    }

    "take over when two oldest crash in 3 nodes cluster" in within(60 seconds) {
      crash(third, fourth)
      verifyRegistration(fifth)
      verifyMsg(fifth, msg = msg())
      verifyProxyMsg(fifth, fifth, msg = msg())
      verifyProxyMsg(fifth, sixth, msg = msg())
    }

    "take over when oldest crashes in 2 nodes cluster" in within(60 seconds) {
      crash(fifth)
      verifyRegistration(sixth)
      verifyMsg(sixth, msg = msg())
      verifyProxyMsg(sixth, sixth, msg = msg())
    }

  }
}
