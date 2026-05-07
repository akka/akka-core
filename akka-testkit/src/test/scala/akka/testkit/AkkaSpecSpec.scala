/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor._
import akka.actor.DeadLetter
import akka.pattern.ask
import akka.util.Timeout

@nowarn
class AkkaSpecSpec extends AnyWordSpec with Matchers {

  "An AkkaSpec" must {

    "warn about unhandled messages" in {
      implicit val system = ActorSystem("AkkaSpec0", AkkaSpec.testConf)
      try {
        val a = system.actorOf(Props.empty)
        EventFilter.warning(start = "unhandled message", occurrences = 1).intercept {
          a ! 42
        }
      } finally {
        TestKit.shutdownActorSystem(system)
      }
    }

    "terminate all actors" in {
      // verbose config just for demonstration purposes, please leave in in case of debugging
      import scala.jdk.CollectionConverters._
      val conf = Map(
        "akka.actor.debug.lifecycle" -> true,
        "akka.actor.debug.event-stream" -> true,
        "akka.loglevel" -> "DEBUG",
        "akka.stdout-loglevel" -> "DEBUG")
      val localSystem = ActorSystem("AkkaSpec1", ConfigFactory.parseMap(conf.asJava).withFallback(AkkaSpec.testConf))
      var refs = Seq.empty[ActorRef]
      val spec = new AkkaSpec(localSystem) { refs = Seq(testActor, localSystem.actorOf(Props.empty, "name")) }
      refs.foreach(_.isTerminated should not be true)
      TestKit.shutdownActorSystem(localSystem)
      spec.awaitCond(refs.forall(_.isTerminated), 2 seconds)
    }

    "stop correctly when sending PoisonPill to rootGuardian" in {
      val system = ActorSystem("AkkaSpec2", AkkaSpec.testConf)
      new AkkaSpec(system) {}
      val latch = new TestLatch(1)(system)
      system.registerOnTermination(latch.countDown())

      system.actorSelection("/") ! PoisonPill

      Await.ready(latch, 2 seconds)
    }

    "enqueue unread messages from testActor to deadLetters" in {
      val system, otherSystem = ActorSystem("AkkaSpec3", AkkaSpec.testConf)

      try {
        var locker = Seq.empty[DeadLetter]
        implicit val timeout: Timeout = TestKitExtension(system).DefaultTimeout.duration.dilated(system)
        val davyJones = otherSystem.actorOf(Props(new Actor {
          def receive = {
            case m: DeadLetter => locker :+= m
            case "Die!"        => sender() ! "finally gone"; context.stop(self)
          }
        }), "davyJones")

        system.eventStream.subscribe(davyJones, classOf[DeadLetter])

        val probe = new TestProbe(system)
        probe.ref.tell(42, davyJones)
        /*
         * this will ensure that the message is actually received, otherwise it
         * may happen that the system.stop() suspends the testActor before it had
         * a chance to put the message into its private queue
         */
        probe.receiveWhile(1 second) {
          case null =>
        }

        val latch = new TestLatch(1)(system)
        system.registerOnTermination(latch.countDown())
        TestKit.shutdownActorSystem(system)
        Await.ready(latch, 2 seconds)
        Await.result(davyJones ? "Die!", timeout.duration) should ===("finally gone")

        // this will typically also contain log messages which were sent after the logger shutdown
        locker should contain(DeadLetter(42, davyJones, probe.ref))
      } finally {
        TestKit.shutdownActorSystem(system)
        TestKit.shutdownActorSystem(otherSystem)
      }
    }
  }
}
