/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import com.typesafe.config._

import akka.actor._
import akka.remote.NotAllowedClassRemoteDeploymentAttemptException
import akka.remote.RARP
import akka.testkit._

object RemoteDeploymentSpec {
  class Echo1 extends Actor {
    var target: ActorRef = context.system.deadLetters

    def receive = {
      case "throwInvalidActorNameException" =>
        // InvalidActorNameException is supported by akka-misc
        throw InvalidActorNameException("wrong name")
      case "throwException" =>
        // no specific serialization binding for Exception
        throw new Exception("crash")
      case x =>
        target = sender(); sender() ! x
    }

    override def preStart(): Unit = {}
    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
      target ! "preRestart"
    }
    override def postRestart(cause: Throwable): Unit = {}
    override def postStop(): Unit = {
      target ! "postStop"
    }
  }

  class NotAllowed extends Echo1

  def parentProps(probe: ActorRef): Props =
    Props(new Parent(probe))

  class Parent(probe: ActorRef) extends Actor {
    var target: ActorRef = context.system.deadLetters

    override val supervisorStrategy = OneForOneStrategy() {
      case e: Exception =>
        probe ! e
        SupervisorStrategy.stop
    }

    def receive = {
      case p: Props =>
        sender() ! context.actorOf(p)

      case (p: Props, numMessages: Int) =>
        val child = context.actorOf(p)
        sender() ! child
        // stress as quick send as possible
        (0 until numMessages).foreach(n => child.tell(n, sender()))
    }
  }

  class DeadOnArrival extends Actor {
    if ("confuse IntellIJ dead code checker".length > 2) {
      throw new Exception("init-crash")
    }

    def receive = Actor.emptyBehavior
  }
}

class RemoteDeploymentSpec
    extends ArteryMultiNodeSpec(
      ConfigFactory.parseString("""
    akka.remote.artery.advanced.inbound-lanes = 10
    akka.remote.artery.advanced.outbound-lanes = 3
    akka.remote.use-unsafe-remote-features-outside-cluster = on
    
    //#allow-list-config
    akka.remote.deployment {
      enable-allow-list = on
      
      allowed-actor-classes = [
        //#allow-list-config
        "NOT_ON_CLASSPATH", # verify we don't throw if a class not on classpath is listed here
        "akka.remote.artery.RemoteDeploymentSpec.DeadOnArrival",
        //#allow-list-config
        "akka.remote.artery.RemoteDeploymentSpec.Echo1"
      ]
    }
    //#allow-list-config
    
    """).withFallback(ArterySpecSupport.defaultConfig)) {

  import RemoteDeploymentSpec._

  val port = RARP(system).provider.getDefaultAddress.port.get
  val conf =
    s"""
    akka.actor.deployment {
      /blub.remote = "akka://${system.name}@localhost:$port"
      /blub2.remote = "akka://${system.name}@localhost:$port"
      "/parent*/*".remote = "akka://${system.name}@localhost:$port"
      /danger-mouse.remote = "akka://${system.name}@localhost:$port"
    }
    akka.remote.artery.advanced.inbound-lanes = 10
    akka.remote.artery.advanced.outbound-lanes = 3    
    """

  val masterSystem = newRemoteSystem(name = Some("Master" + system.name), extraConfig = Some(conf))
  val masterPort = address(masterSystem).port.get

  "Remoting" must {

    "create and supervise children on remote node" in {
      val senderProbe = TestProbe()(masterSystem)
      val r = masterSystem.actorOf(Props[Echo1](), "blub")
      r.path.toString should ===(
        s"akka://${system.name}@localhost:${port}/remote/akka/${masterSystem.name}@localhost:${masterPort}/user/blub")

      r.tell(42, senderProbe.ref)
      senderProbe.expectMsg(42)
      EventFilter[Exception]("wrong name", occurrences = 1).intercept {
        r ! "throwInvalidActorNameException"
      }(masterSystem)
      senderProbe.expectMsg("preRestart")
      r.tell(43, senderProbe.ref)
      senderProbe.expectMsg(43)
      masterSystem.stop(r)
      senderProbe.expectMsg("postStop")
    }

    "create and supervise children on remote node for unknown exception" in {
      val senderProbe = TestProbe()(masterSystem)
      val r = masterSystem.actorOf(Props[Echo1](), "blub2")
      r.path.toString should ===(
        s"akka://${system.name}@localhost:${port}/remote/akka/${masterSystem.name}@localhost:${masterPort}/user/blub2")

      r.tell(42, senderProbe.ref)
      senderProbe.expectMsg(42)
      EventFilter[Exception]("Serialization of [java.lang.Exception] failed. crash", occurrences = 1).intercept {
        r ! "throwException"
      }(masterSystem)
      senderProbe.expectMsg("preRestart")
      r.tell(43, senderProbe.ref)
      senderProbe.expectMsg(43)
      masterSystem.stop(r)
      senderProbe.expectMsg("postStop")
    }

    "notice immediate death" in {
      val parent = masterSystem.actorOf(parentProps(testActor), "parent")
      EventFilter[ActorInitializationException](occurrences = 1).intercept {
        parent.tell(Props[DeadOnArrival](), testActor)
        val child = expectMsgType[ActorRef]
        expectMsgType[ActorInitializationException]

        watch(child)
        expectTerminated(child)
      }(masterSystem)
    }

    "deliver all messages" in {
      val numParents = 10
      val numChildren = 20
      val numMessages = 5

      val parents = (0 until numParents).map { i =>
        masterSystem.actorOf(parentProps(testActor), s"parent-$i")
      }.toVector

      val probes = Vector.fill(numParents, numChildren)(TestProbe()(masterSystem))
      val childProps = Props[Echo1]()
      for (p <- (0 until numParents); c <- (0 until numChildren)) {
        parents(p).tell((childProps, numMessages), probes(p)(c).ref)
      }

      for (p <- (0 until numParents); c <- (0 until numChildren)) {
        val probe = probes(p)(c)
        probe.expectMsgType[ActorRef] // the child
      }

      val expectedMessages = (0 until numMessages).toVector
      for (p <- (0 until numParents); c <- (0 until numChildren)) {
        val probe = probes(p)(c)
        probe.receiveN(numMessages) should equal(expectedMessages)
      }

    }

    "not deploy actor not listed in allow list" in {
      val senderProbe = TestProbe()(masterSystem)
      EventFilter[NotAllowedClassRemoteDeploymentAttemptException](occurrences = 1).intercept {
        val r = masterSystem.actorOf(Props[NotAllowed](), "danger-mouse")
        r.path.toString should ===(
          s"akka://${system.name}@localhost:${port}/remote/akka/${masterSystem.name}@localhost:${masterPort}/user/danger-mouse")

        r.tell(42, senderProbe.ref)
        senderProbe.expectNoMessage()
      }
    }

  }

}
