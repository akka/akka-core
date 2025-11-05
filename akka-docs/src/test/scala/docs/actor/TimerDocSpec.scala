/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

object TimerDocSpec {
  //#timers
  import scala.concurrent.duration._

  import akka.actor.Actor
  import akka.actor.Timers

  object MyActor {
    private case object TickKey
    private case object FirstTick
    private case object Tick
  }

  class MyActor extends Actor with Timers {
    import MyActor._
    timers.startSingleTimer(TickKey, FirstTick, 500.millis)

    def receive = {
      case FirstTick =>
        // do something useful here
        timers.startTimerWithFixedDelay(TickKey, Tick, 1.second)
      case Tick =>
      // do something useful here
    }
  }
  //#timers
}
