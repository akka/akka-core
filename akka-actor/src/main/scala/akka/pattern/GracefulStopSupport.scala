/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.actor._
import akka.dispatch.sysmsg.{ Unwatch, Watch }
import akka.util.Timeout

trait GracefulStopSupport {

  /**
   * Returns a [[scala.concurrent.Future]] that will be completed with success (value `true`) when
   * existing messages of the target actor has been processed and the actor has been
   * terminated.
   *
   * Useful when you need to wait for termination or compose ordered termination of several actors,
   * which should only be done outside of the ActorSystem as blocking inside Actors is discouraged.
   *
   * <b>IMPORTANT NOTICE:</b> the actor being terminated and its supervisor
   * being informed of the availability of the deceased actor’s name are two
   * distinct operations, which do not obey any reliable ordering. Especially
   * the following will NOT work:
   *
   * {{{
   * def receive = {
   *   case msg =>
   *     Await.result(gracefulStop(someChild, timeout), timeout)
   *     context.actorOf(Props(...), "someChild") // assuming that that was someChild’s name, this will NOT work
   * }
   * }}}
   *
   * If the target actor isn't terminated within the timeout the [[scala.concurrent.Future]]
   * is completed with failure [[akka.pattern.AskTimeoutException]].
   *
   * If you want to invoke specialized stopping logic on your target actor instead of PoisonPill, you can pass your
   * stop command as a parameter:
   * {{{
   *   gracefulStop(someChild, timeout, MyStopGracefullyMessage).onComplete {
   *      // Do something after someChild being stopped
   *   }
   * }}}
   */
  def gracefulStop(target: ActorRef, timeout: FiniteDuration, stopMessage: Any = PoisonPill): Future[Boolean] = {
    val internalTarget = target.asInstanceOf[InternalActorRef]
    val ref =
      PromiseActorRef(internalTarget.provider, Timeout(timeout), target, stopMessage.getClass.getName, target.path.name)
    internalTarget.sendSystemMessage(Watch(internalTarget, ref))
    target.tell(stopMessage, Actor.noSender)
    ref.result.future.transform({
      case Terminated(t) if t.path == target.path => true
      case _                                      => { internalTarget.sendSystemMessage(Unwatch(target, ref)); false }
    }, t => { internalTarget.sendSystemMessage(Unwatch(target, ref)); t })(ExecutionContext.parasitic)
  }
}
