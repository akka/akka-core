/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import akka.actor.{
  Actor,
  ActorIdentity,
  ActorLogging,
  ActorRef,
  ActorRefScope,
  ActorSelection,
  Identify,
  PoisonPill,
  Props,
  Terminated
}
import akka.testkit.{ ImplicitSender, TestActors }
import akka.testkit.JavaSerializable

object RemoteActorSelectionSpec {
  final case class ActorSelReq(s: String) extends JavaSerializable
  final case class ActorCreateReq(props: Props, name: String) extends JavaSerializable

  class SelectionActor extends Actor with ActorLogging {
    log.info("Started")
    def receive = {
      // if we get props and a name, create a child, send ref back
      case ActorCreateReq(p, n) =>
        log.info(s"Creating child $n")
        sender() ! context.actorOf(p, n)
      // or select actor from here
      case ActorSelReq(s) => sender() ! context.actorSelection(s)
    }
  }
  def selectionActorProps = Props(new SelectionActor)
}

class RemoteActorSelectionSpec extends ArteryMultiNodeSpec with ImplicitSender {

  import RemoteActorSelectionSpec._

  "Remote actor selection" should {

    // TODO fails with not receiving the localGrandchild value, seems to go to dead letters
    "select actors across node boundaries" ignore {

      val remotePort = freePort()
      val remoteSysName = "remote-" + system.name

      val localPort = freePort()
      val localSysName = "local-" + system.name

      def config(port: Int) =
        s"""
          akka {
            remote.artery.port = $port
            actor.deployment {
              /looker2/child.remote = "akka://$remoteSysName@localhost:$remotePort"
              /looker2/child/grandchild.remote = "akka://$localSysName@localhost:$localPort"
            }
          }
        """

      val localSystem = newRemoteSystem(extraConfig = Some(config(localPort)), name = Some(localSysName))

      newRemoteSystem(extraConfig = Some(config(remotePort)), name = Some(remoteSysName))

      val localLooker2 = localSystem.actorOf(selectionActorProps, "looker2")

      // child is configured to be deployed on remoteSystem
      localLooker2 ! ActorCreateReq(selectionActorProps, "child")
      val remoteChild = expectMsgType[ActorRef]

      // grandchild is configured to be deployed on local system but from remote system
      remoteChild ! ActorCreateReq(selectionActorProps, "grandchild")
      val localGrandchild = expectMsgType[ActorRef]
      localGrandchild.asInstanceOf[ActorRefScope].isLocal should ===(true)
      localGrandchild ! 53
      expectMsg(53)

      val localGrandchildSelection = localSystem.actorSelection(localSystem / "looker2" / "child" / "grandchild")
      localGrandchildSelection ! 54
      expectMsg(54)
      lastSender should ===(localGrandchild)
      (lastSender should be).theSameInstanceAs(localGrandchild)
      localGrandchildSelection ! Identify(localGrandchildSelection)
      val grandchild2 = expectMsgType[ActorIdentity].ref
      grandchild2 should ===(Some(localGrandchild))

      localSystem.actorSelection("/user/looker2/child") ! Identify(None)
      expectMsgType[ActorIdentity].ref should ===(Some(remoteChild))

      localLooker2 ! ActorSelReq("child/..")
      expectMsgType[ActorSelection] ! Identify(None)
      (expectMsgType[ActorIdentity].ref.get should be).theSameInstanceAs(localLooker2)

      localSystem.actorSelection(localSystem / "looker2" / "child") ! ActorSelReq("..")
      expectMsgType[ActorSelection] ! Identify(None)
      (expectMsgType[ActorIdentity].ref.get should be).theSameInstanceAs(localLooker2)

      localGrandchild ! ((TestActors.echoActorProps, "grandgrandchild"))
      val grandgrandchild = expectMsgType[ActorRef]

      localSystem.actorSelection("/user/looker2/child") ! Identify("idReq1")
      expectMsg(ActorIdentity("idReq1", Some(remoteChild)))
      localSystem.actorSelection(remoteChild.path) ! Identify("idReq2")
      expectMsg(ActorIdentity("idReq2", Some(remoteChild)))
      localSystem.actorSelection("/user/looker2/*") ! Identify("idReq3")
      expectMsg(ActorIdentity("idReq3", Some(remoteChild)))

      localSystem.actorSelection("/user/looker2/child/grandchild") ! Identify("idReq4")
      expectMsg(ActorIdentity("idReq4", Some(localGrandchild)))
      localSystem.actorSelection(remoteChild.path / "grandchild") ! Identify("idReq5")
      expectMsg(ActorIdentity("idReq5", Some(localGrandchild)))
      localSystem.actorSelection("/user/looker2/*/grandchild") ! Identify("idReq6")
      expectMsg(ActorIdentity("idReq6", Some(localGrandchild)))
      localSystem.actorSelection("/user/looker2/child/*") ! Identify("idReq7")
      expectMsg(ActorIdentity("idReq7", Some(localGrandchild)))
      localSystem.actorSelection(remoteChild.path / "*") ! Identify("idReq8")
      expectMsg(ActorIdentity("idReq8", Some(localGrandchild)))

      localSystem.actorSelection("/user/looker2/child/grandchild/grandgrandchild") ! Identify("idReq9")
      expectMsg(ActorIdentity("idReq9", Some(grandgrandchild)))
      localSystem.actorSelection(remoteChild.path / "grandchild" / "grandgrandchild") ! Identify("idReq10")
      expectMsg(ActorIdentity("idReq10", Some(grandgrandchild)))
      localSystem.actorSelection("/user/looker2/child/*/grandgrandchild") ! Identify("idReq11")
      expectMsg(ActorIdentity("idReq11", Some(grandgrandchild)))
      localSystem.actorSelection("/user/looker2/child/*/*") ! Identify("idReq12")
      expectMsg(ActorIdentity("idReq12", Some(grandgrandchild)))
      localSystem.actorSelection(remoteChild.path / "*" / "grandgrandchild") ! Identify("idReq13")
      expectMsg(ActorIdentity("idReq13", Some(grandgrandchild)))

      val sel1 = localSystem.actorSelection("/user/looker2/child/grandchild/grandgrandchild")
      localSystem.actorSelection(sel1.toSerializationFormat) ! Identify("idReq18")
      expectMsg(ActorIdentity("idReq18", Some(grandgrandchild)))

      remoteChild ! Identify("idReq14")
      expectMsg(ActorIdentity("idReq14", Some(remoteChild)))
      watch(remoteChild)
      remoteChild ! PoisonPill
      expectMsg("postStop")
      expectMsgType[Terminated].actor should ===(remoteChild)
      localLooker2 ! ((TestActors.echoActorProps, "child"))
      val child2 = expectMsgType[ActorRef]
      child2 ! Identify("idReq15")
      expectMsg(ActorIdentity("idReq15", Some(child2)))
      localSystem.actorSelection(remoteChild.path) ! Identify("idReq16")
      expectMsg(ActorIdentity("idReq16", Some(child2)))
      remoteChild ! Identify("idReq17")
      expectMsg(ActorIdentity("idReq17", None))

      child2 ! 55
      expectMsg(55)
      // msg to old ActorRef (different uid) should not get through
      child2.path.uid should not be remoteChild.path.uid
      remoteChild ! 56
      expectNoMessage(1.second)
      localSystem.actorSelection(localSystem / "looker2" / "child") ! 57
      expectMsg(57)
    }

  }

}
