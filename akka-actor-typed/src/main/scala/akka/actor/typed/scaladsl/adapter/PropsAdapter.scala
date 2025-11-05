/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl.adapter

import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors

/**
 * Wrap [[akka.actor.typed.Behavior]] in a classic [[akka.actor.Props]], i.e. when
 * spawning a typed child actor from a classic parent actor.
 * This is normally not needed because you can use the extension methods
 * `spawn` and `spawnAnonymous` on a classic `ActorContext`, but it's needed
 * when using typed actors with an existing library/tool that provides an API that
 * takes a classic [[akka.actor.Props]] parameter. Cluster Sharding is an
 * example of that.
 */
object PropsAdapter {
  def apply[T](behavior: => Behavior[T], deploy: Props = Props.empty): akka.actor.Props =
    akka.actor.typed.internal.adapter.PropsAdapter(
      () => Behaviors.supervise(behavior).onFailure(SupervisorStrategy.stop),
      deploy,
      rethrowTypedFailure = false)
}
