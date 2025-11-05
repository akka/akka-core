/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package docs.actor.typed

import akka.actor.typed.scaladsl.ActorContext

object DispatcherDocSpec {

  val context: ActorContext[Integer] = ???

  {
    //#defining-dispatcher-in-code
    import akka.actor.typed.DispatcherSelector
    val myActor =
      context.spawn(PrintActor(), "PrintActor", DispatcherSelector.fromConfig("PrintActor"))
    //#defining-dispatcher-in-code
  }

  {
    //#defining-fixed-pool-size-dispatcher
    import akka.actor.typed.DispatcherSelector
    val myActor =
      context.spawn(PrintActor(), "PrintActor", DispatcherSelector.fromConfig("blocking-io-dispatcher"))
    //#defining-fixed-pool-size-dispatcher
  }

  {
    //#lookup
    // for use with Futures, Scheduler, etc.
    import akka.actor.typed.DispatcherSelector
    implicit val executionContext = context.system.dispatchers.lookup(DispatcherSelector.fromConfig("my-dispatcher"))
    //#lookup
  }
}
