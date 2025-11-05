/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.duration._

import language.postfixOps

import akka.dispatch.ThreadPoolConfig
import akka.testkit.AkkaSpec

object ConsistencySpec {
  val minThreads = 1
  val maxThreads = 2000
  val factor = 1.5d
  val threads = ThreadPoolConfig.scaledPoolSize(minThreads, factor, maxThreads) // Make sure we have more threads than cores

  val config = s"""
      consistency-dispatcher {
        throughput = 1
        executor = "fork-join-executor"
        fork-join-executor {
          parallelism-min = $minThreads
          parallelism-factor = $factor
          parallelism-max = $maxThreads
        }
      }
    """
  class CacheMisaligned(var value: Long, var padding1: Long, var padding2: Long, var padding3: Int) //Vars, no final fences

  class ConsistencyCheckingActor extends Actor {
    var left = new CacheMisaligned(42, 0, 0, 0) //var
    var right = new CacheMisaligned(0, 0, 0, 0) //var
    var lastStep = -1L
    def receive = {
      case step: Long =>
        if (lastStep != (step - 1))
          sender() ! "Test failed: Last step %s, this step %s".format(lastStep, step)

        val shouldBeFortyTwo = left.value + right.value
        if (shouldBeFortyTwo != 42)
          sender() ! "Test failed: 42 failed"
        else {
          left.value += 1
          right.value -= 1
        }

        lastStep = step
      case "done" => sender() ! "done"; context.stop(self)
    }
  }
}

class ConsistencySpec extends AkkaSpec(ConsistencySpec.config) {
  import ConsistencySpec._

  override def expectedTestDuration: FiniteDuration = 5.minutes

  "The Akka actor model implementation" must {
    "provide memory consistency" in {
      val noOfActors = threads + 1
      val props = Props[ConsistencyCheckingActor]().withDispatcher("consistency-dispatcher")
      val actors = Vector.fill(noOfActors)(system.actorOf(props))

      for (i <- 0L until 10000L) {
        actors.foreach(_.tell(i, testActor))
      }

      for (a <- actors) { a.tell("done", testActor) }

      for (_ <- actors) expectMsg(5 minutes, "done")
    }
  }
}
