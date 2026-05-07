/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.state

import akka.actor.ExtendedActorSystem
import akka.persistence.state.DurableStateStoreProvider
import akka.persistence.state.javadsl.{ DurableStateStore => JDurableStateStore }
import akka.persistence.state.scaladsl.DurableStateStore
import akka.persistence.testkit.state.javadsl.{
  PersistenceTestKitDurableStateStore => JPersistenceTestKitDurableStateStore
}
import akka.persistence.testkit.state.scaladsl.PersistenceTestKitDurableStateStore

class PersistenceTestKitDurableStateStoreProvider(system: ExtendedActorSystem) extends DurableStateStoreProvider {
  private val _scaladslDurableStateStore = new PersistenceTestKitDurableStateStore[Any](system)
  override def scaladslDurableStateStore(): DurableStateStore[Any] = _scaladslDurableStateStore

  override def javadslDurableStateStore(): JDurableStateStore[AnyRef] =
    new JPersistenceTestKitDurableStateStore[AnyRef](
      _scaladslDurableStateStore.asInstanceOf[PersistenceTestKitDurableStateStore[AnyRef]])
}
