/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit._

object AddressTerminatedTopicBenchSpec {

  class Subscriber(testActor: ActorRef) extends Actor {
    AddressTerminatedTopic(context.system).subscribe(self)
    testActor ! "started"

    override def postStop(): Unit = {
      AddressTerminatedTopic(context.system).unsubscribe(self)
    }

    def receive = Actor.emptyBehavior
  }
}

class AddressTerminatedTopicBenchSpec extends AkkaSpec("akka.loglevel=INFO") {
  import AddressTerminatedTopicBenchSpec._

  "Subscribe and unsubscribe of AddressTerminated" must {

    "be quick" in {
      val sys = ActorSystem(system.name + "2", system.settings.config)
      try {
        val num = 20000

        val t1 = System.nanoTime()
        val p = Props(classOf[Subscriber], testActor)
        Vector.fill(num)(sys.actorOf(p))
        receiveN(num, 10.seconds)
        log.info("Starting {} actors took {} ms", num, (System.nanoTime() - t1).nanos.toMillis)

        val t2 = System.nanoTime()
        shutdown(sys, 10.seconds, verifySystemShutdown = true)
        log.info("Stopping {} actors took {} ms", num, (System.nanoTime() - t2).nanos.toMillis)
      } finally {
        shutdown(sys)
      }
    }

  }
}
