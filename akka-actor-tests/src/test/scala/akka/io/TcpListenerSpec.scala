/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.Socket
import java.nio.channels.{ SelectableChannel, SocketChannel }
import java.nio.channels.SelectionKey.OP_ACCEPT

import scala.concurrent.duration._

import Tcp._

import akka.actor._
import akka.io.SelectionHandler._
import akka.io.TcpListener.{ FailedRegisterIncoming, RegisterIncoming }
import akka.io.TcpListenerSpec.RegisterChannel
import akka.testkit.{ AkkaSpec, EventFilter, TestActorRef, TestProbe }
import akka.testkit.SocketUtil

class TcpListenerSpec extends AkkaSpec("""
    akka.io.tcp.batch-accept-limit = 2
    """) {

  "A TcpListener" must {

    "register its ServerSocketChannel with its selector" in new TestSetup(pullMode = false)

    "let the Bind commander know when binding is completed" in new TestSetup(pullMode = false) {
      listener ! new ChannelRegistration {
        def disableInterest(op: Int) = ()
        def enableInterest(op: Int) = ()
        def cancelAndClose(andThen: () => Unit): Unit = ()
      }
      bindCommander.expectMsgType[Bound]
    }

    "accept acceptable connections and register them with its parent" in new TestSetup(pullMode = false) {
      bindListener()

      attemptConnectionToEndpoint()
      attemptConnectionToEndpoint()
      attemptConnectionToEndpoint()

      // since the batch-accept-limit is 2 we must only receive 2 accepted connections
      listener ! ChannelAcceptable

      expectWorkerForCommand
      expectWorkerForCommand
      selectorRouter.expectNoMessage(100.millis)
      interestCallReceiver.expectMsg(OP_ACCEPT)

      // and pick up the last remaining connection on the next ChannelAcceptable
      listener ! ChannelAcceptable
      expectWorkerForCommand
    }

    "continue to accept connections after a previous accept" in new TestSetup(pullMode = false) {
      bindListener()

      attemptConnectionToEndpoint()
      listener ! ChannelAcceptable
      expectWorkerForCommand
      selectorRouter.expectNoMessage(100.millis)
      interestCallReceiver.expectMsg(OP_ACCEPT)

      attemptConnectionToEndpoint()
      listener ! ChannelAcceptable
      expectWorkerForCommand
      selectorRouter.expectNoMessage(100.millis)
      interestCallReceiver.expectMsg(OP_ACCEPT)
    }

    "not accept connections after a previous accept until read is reenabled" in new TestSetup(pullMode = true) {
      bindListener()

      attemptConnectionToEndpoint()
      expectNoMessage(100.millis)

      listener ! ResumeAccepting(batchSize = 1)
      listener ! ChannelAcceptable
      expectWorkerForCommand
      selectorRouter.expectNoMessage(100.millis)
      interestCallReceiver.expectMsg(OP_ACCEPT)

      // No more accepts are allowed now
      interestCallReceiver.expectNoMessage(100.millis)

      listener ! ResumeAccepting(batchSize = 2)
      interestCallReceiver.expectMsg(OP_ACCEPT)

      attemptConnectionToEndpoint()
      listener ! ChannelAcceptable
      expectWorkerForCommand
      selectorRouter.expectNoMessage(100.millis)
      // There is still one token remaining, accepting
      interestCallReceiver.expectMsg(OP_ACCEPT)

      attemptConnectionToEndpoint()
      listener ! ChannelAcceptable
      expectWorkerForCommand
      selectorRouter.expectNoMessage(100.millis)

      // Tokens are depleted now
      interestCallReceiver.expectNoMessage(100.millis)
    }

    "react to Unbind commands by replying with Unbound and stopping itself" in new TestSetup(pullMode = false) {
      bindListener()

      val unbindCommander = TestProbe()
      unbindCommander.send(listener, Unbind)

      unbindCommander.expectMsg(Unbound)
      parent.expectTerminated(listener)
    }

    "drop an incoming connection if it cannot be registered with a selector" in new TestSetup(pullMode = false) {
      bindListener()

      attemptConnectionToEndpoint()

      listener ! ChannelAcceptable
      val channel = expectWorkerForCommand

      EventFilter.warning(pattern = "selector capacity limit", occurrences = 1).intercept {
        listener ! FailedRegisterIncoming(channel)
        awaitCond(!channel.isOpen)
      }
    }
  }

  val counter = Iterator.from(0)

  class TestSetup(pullMode: Boolean) {
    val handler = TestProbe()
    val handlerRef = handler.ref
    val bindCommander = TestProbe()
    val parent = TestProbe()
    val selectorRouter = TestProbe()
    val endpoint = SocketUtil.temporaryServerAddress()

    var registerCallReceiver = TestProbe()
    var interestCallReceiver = TestProbe()

    private val parentRef = TestActorRef(new ListenerParent(pullMode))

    val register = registerCallReceiver.expectMsgType[RegisterChannel]
    register.initialOps should ===(if (pullMode) 0 else OP_ACCEPT)

    def bindListener(): Unit = {
      listener ! new ChannelRegistration {
        def enableInterest(op: Int): Unit = interestCallReceiver.ref ! op
        def disableInterest(op: Int): Unit = interestCallReceiver.ref ! -op
        def cancelAndClose(andThen: () => Unit): Unit = {
          register.channel.close()
          require(!register.channel.isRegistered)
          andThen()
        }
      }
      bindCommander.expectMsgType[Bound]
    }

    def attemptConnectionToEndpoint(): Unit = new Socket(endpoint.getHostName, endpoint.getPort)

    def listener = parentRef.underlyingActor.listener

    def expectWorkerForCommand: SocketChannel =
      selectorRouter.expectMsgPF() {
        case WorkerForCommand(RegisterIncoming(chan), commander, _) =>
          chan.isOpen should ===(true)
          commander should ===(listener)
          chan
      }

    private class ListenerParent(pullMode: Boolean) extends Actor with ChannelRegistry {
      val listener = context.actorOf(
        props = Props(
          classOf[TcpListener],
          selectorRouter.ref,
          Tcp(system),
          this,
          bindCommander.ref,
          Bind(handler.ref, endpoint, 100, Nil, pullMode)).withDeploy(Deploy.local),
        name = "test-listener-" + counter.next())
      parent.watch(listener)
      def receive: Receive = {
        case msg => parent.ref.forward(msg)
      }
      override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

      def register(channel: SelectableChannel, initialOps: Int)(implicit channelActor: ActorRef): Unit =
        registerCallReceiver.ref.tell(RegisterChannel(channel, initialOps), channelActor)
    }
  }

}
object TcpListenerSpec {
  final case class RegisterChannel(channel: SelectableChannel, initialOps: Int)
      extends NoSerializationVerificationNeeded
}
