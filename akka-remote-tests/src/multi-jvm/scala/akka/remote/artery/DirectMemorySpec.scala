/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorPath, ActorRef, Props }
import akka.remote.RemotingMultiNodeSpec
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, STMultiNodeSpec }
import akka.testkit.ImplicitSender
import akka.testkit.JavaSerializable

object DirectMemorySpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.loglevel = WARNING
      akka.remote.artery.large-message-destinations = ["/user/large"]
      akka.remote.artery.buffer-pool-size = 32
      akka.remote.artery.maximum-frame-size = 256 KiB
      akka.remote.artery.large-buffer-pool-size = 4
      akka.remote.artery.maximum-large-frame-size = 2 MiB
      """))
      .withFallback(RemotingMultiNodeSpec.commonConfig))

  // buffer pool + large buffer pool = 16M, see DirectMemorySpecMultiJvmNode1.opts

  case object Message extends JavaSerializable
  case class Start(rootPath: ActorPath) extends JavaSerializable
  case class Done(actor: ActorRef) extends JavaSerializable
  class CountingEcho(reportTo: ActorRef, private var count: Int) extends Actor {
    override def receive: Receive = {
      case Start(rootPath) =>
        count -= 1
        context.system.actorSelection(rootPath / "user" / self.path.name) ! Message
      case Message if count > 0 =>
        count -= 1
        sender() ! Message
      case Message =>
        reportTo ! Done(self)
    }
  }
}

class DirectMemorySpecMultiJvmNode1 extends DirectMemorySpec
class DirectMemorySpecMultiJvmNode2 extends DirectMemorySpec

abstract class DirectMemorySpec extends MultiNodeSpec(DirectMemorySpec) with STMultiNodeSpec with ImplicitSender {

  import DirectMemorySpec._

  override def initialParticipants: Int = roles.size

  "This test" should {
    "override JVM start-up options" in {
      // it's important that *.opts files have been processed
      assert(System.getProperty("DirectMemorySpec.marker").equals("true"))
    }
  }

  "Direct memory allocation" should {
    "not cause OutOfMemoryError" in within(10.seconds) {
      // twice the buffer pool size
      val nrOfRegularMessages = 2 * system.settings.config.getInt("akka.remote.artery.buffer-pool-size")
      val nrOfLargeMessages = 2 * system.settings.config.getInt("akka.remote.artery.large-buffer-pool-size")

      val large = system.actorOf(Props(classOf[CountingEcho], testActor, nrOfLargeMessages), "large")
      val regular = system.actorOf(Props(classOf[CountingEcho], testActor, nrOfRegularMessages), "regular")

      runOn(first) {
        large ! Start(node(second))
        regular ! Start(node(second))
      }

      enterBarrier("started")

      runOn(first) {
        expectMsgAllOf(Done(large), Done(regular))
      }

      enterBarrier("done")
    }
  }
}
