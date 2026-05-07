/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.state

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.persistence.state.{ DurableStateStoreProvider, DurableStateStoreRegistry }
import akka.persistence.state.scaladsl.{ DurableStateStore, DurableStateUpdateStore, GetObjectResult }
import com.typesafe.config.Config

import akka.persistence.state.javadsl.{ DurableStateStore => JDurableStateStore }
import scala.concurrent.Future

import akka.persistence.state.scaladsl.DurableStateUpdateWithChangeEventStore

//#plugin-provider
class MyStateStoreProvider(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateStoreProvider {

  /**
   * The `DurableStateStore` implementation for the Scala API.
   * This corresponds to the instance that is returned by [[DurableStateStoreRegistry#durableStateStoreFor]].
   */
  override def scaladslDurableStateStore(): DurableStateStore[Any] = new MyStateStore(system, config, cfgPath)

  /**
   * The `DurableStateStore` implementation for the Java API.
   * This corresponds to the instance that is returned by [[DurableStateStoreRegistry#getDurableStateStoreFor]].
   */
  override def javadslDurableStateStore(): JDurableStateStore[AnyRef] = new MyJavaStateStore(system, config, cfgPath)
}
//#plugin-provider

//#plugin-api
class MyStateStore[A](system: ExtendedActorSystem, config: Config, cfgPath: String) extends DurableStateUpdateStore[A] {

  /**
   * Will persist the latest state. If it’s a new persistence id, the record will be inserted.
   *
   * In case of an existing persistence id, the record will be updated only if the revision
   * number of the incoming record is 1 more than the already existing record. Otherwise persist will fail.
   */
  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = ???

  /**
   * Deprecated. Use the deleteObject overload with revision instead.
   */
  override def deleteObject(persistenceId: String): Future[Done] = deleteObject(persistenceId, 0)

  /**
   * Will delete the state by setting it to the empty state and the revision number will be incremented by 1.
   */
  override def deleteObject(persistenceId: String, revision: Long): Future[Done] = ???

  /**
   * Returns the current state for the given persistence id.
   */
  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = ???
}
//#plugin-api

//#plugin-api-change-event
class MyChangeEventStateStore[A](system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateUpdateWithChangeEventStore[A] {

  /**
   * The `changeEvent` is written to the event journal.
   * Same `persistenceId` is used in the journal and the `revision` is used as `sequenceNr`.
   *
   * @param revision sequence number for optimistic locking. starts at 1.
   */
  override def upsertObject(
      persistenceId: String,
      revision: Long,
      value: A,
      tag: String,
      changeEvent: Any): Future[Done] = ???

  override def deleteObject(persistenceId: String, revision: Long, changeEvent: Any): Future[Done] = ???

  /**
   * Will persist the latest state. If it’s a new persistence id, the record will be inserted.
   *
   * In case of an existing persistence id, the record will be updated only if the revision
   * number of the incoming record is 1 more than the already existing record. Otherwise persist will fail.
   */
  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = ???

  /**
   * Deprecated. Use the deleteObject overload with revision instead.
   */
  override def deleteObject(persistenceId: String): Future[Done] = deleteObject(persistenceId, 0)

  /**
   * Will delete the state by setting it to the empty state and the revision number will be incremented by 1.
   */
  override def deleteObject(persistenceId: String, revision: Long): Future[Done] = ???

  /**
   * Returns the current state for the given persistence id.
   */
  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = ???

}
//#plugin-api-change-event
