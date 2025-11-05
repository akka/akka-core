/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.concurrent.duration._

class SchedulerSpec {

  def compileOnly(): Unit = {
    val system: ActorSystem[Nothing] = ???
    import system.executionContext

    // verify a lambda works
    system.scheduler.scheduleWithFixedDelay(10.milliseconds, 10.milliseconds)(() => system.log.info("Woho!"))
    system.scheduler.scheduleAtFixedRate(10.milliseconds, 10.milliseconds)(() => system.log.info("Woho!"))
    system.scheduler.scheduleOnce(10.milliseconds, () => system.log.info("Woho!"))
  }

}
