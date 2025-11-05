/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.receptionist

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Dispatchers
import akka.actor.typed.Props
import akka.actor.typed.receptionist.Receptionist
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ReceptionistImpl(system: ActorSystem[_]) extends Receptionist {

  override val ref: ActorRef[Receptionist.Command] = {
    val provider: ReceptionistBehaviorProvider =
      if (system.settings.classicSettings.ProviderSelectionType.hasCluster) {
        system.dynamicAccess
          .getObjectFor[ReceptionistBehaviorProvider]("akka.cluster.typed.internal.receptionist.ClusterReceptionist")
          .recover {
            case e =>
              throw new RuntimeException(
                "ClusterReceptionist could not be loaded dynamically. Make sure you have " +
                "'akka-cluster-typed' in the classpath.",
                e)
          }
          .get
      } else LocalReceptionist

    import akka.actor.typed.scaladsl.adapter._
    system.internalSystemActorOf(
      provider.behavior,
      provider.name,
      Props.empty.withDispatcherFromConfig(Dispatchers.InternalDispatcherId))
  }
}
