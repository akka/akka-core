/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeoutException

import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.Status.Failure
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender

object CircuitBreakerStressSpec {
  case object JobDone
  case object GetResult
  case class Result(doneCount: Int, timeoutCount: Int, failCount: Int, circCount: Int)

  class StressActor(breaker: CircuitBreaker) extends Actor with ActorLogging with PipeToSupport {
    import context.dispatcher

    private var doneCount = 0
    private var timeoutCount = 0
    private var failCount = 0
    private var circCount = 0

    private def job = {
      val promise = Promise[JobDone.type]()

      context.system.scheduler.scheduleOnce(ThreadLocalRandom.current.nextInt(300).millisecond) {
        promise.success(JobDone)
      }

      promise.future
    }

    override def receive = {
      case JobDone =>
        doneCount += 1
        breaker.withCircuitBreaker(job).pipeTo(self)
      case Failure(_: CircuitBreakerOpenException) =>
        circCount += 1
        breaker.withCircuitBreaker(job).pipeTo(self)
      case Failure(_: TimeoutException) =>
        timeoutCount += 1
        breaker.withCircuitBreaker(job).pipeTo(self)
      case Failure(_) =>
        failCount += 1
        breaker.withCircuitBreaker(job).pipeTo(self)
      case GetResult =>
        sender() ! Result(doneCount, timeoutCount, failCount, circCount)
    }
  }
}

// reproducer for issue #17415
class CircuitBreakerStressSpec extends AkkaSpec with ImplicitSender {
  import CircuitBreakerStressSpec._

  muteDeadLetters(classOf[AnyRef])(system)

  "A CircuitBreaker" in {
    val breaker = CircuitBreaker(system.scheduler, 5, 200.millisecond, 200.seconds)
    val stressActors = Vector.fill(3) {
      system.actorOf(Props(classOf[StressActor], breaker))
    }
    for (_ <- 0 to 1000; a <- stressActors) {
      a ! JobDone
    }
    // let them work for a while
    Thread.sleep(3000)
    stressActors.foreach { a =>
      a ! GetResult
      val result = expectMsgType[Result]
      result.failCount should be(0)
    }

  }

}
