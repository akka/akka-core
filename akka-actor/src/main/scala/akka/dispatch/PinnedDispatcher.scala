/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorCell

/**
 * Dedicates a unique thread for each actor passed in as reference. Served through its messageQueue.
 *
 * The preferred way of creating dispatchers is to define configuration of it and use the
 * the `lookup` method in [[akka.dispatch.Dispatchers]].
 */
class PinnedDispatcher(
    _configurator: MessageDispatcherConfigurator,
    _actor: ActorCell,
    _id: String,
    _shutdownTimeout: FiniteDuration,
    _threadPoolConfig: ThreadPoolConfig)
    extends Dispatcher(
      _configurator,
      _id,
      Int.MaxValue,
      Duration.Zero,
      _threadPoolConfig.copy(corePoolSize = 1, maxPoolSize = 1),
      _shutdownTimeout) {

  @volatile
  private var owner: ActorCell = _actor

  //Relies on an external lock provided by MessageDispatcher.attach
  protected[akka] override def register(actorCell: ActorCell) = {
    val actor = owner
    if ((actor ne null) && actorCell != actor)
      throw new IllegalArgumentException("Cannot register to anyone but " + actor)
    owner = actorCell
    super.register(actorCell)
  }
  //Relies on an external lock provided by MessageDispatcher.detach
  protected[akka] override def unregister(actor: ActorCell) = {
    super.unregister(actor)
    owner = null
  }
}
