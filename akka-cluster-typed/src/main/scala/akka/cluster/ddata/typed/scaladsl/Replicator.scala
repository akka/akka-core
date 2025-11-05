/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.scaladsl

import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.cluster.{ ddata => dd }
import akka.cluster.ddata.Key
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.typed.internal.ReplicatorBehavior

/**
 * @see [[akka.cluster.ddata.Replicator]].
 */
object Replicator {

  /**
   * The `Behavior` for the `Replicator` actor.
   */
  def behavior(settings: ReplicatorSettings): Behavior[Command] =
    ReplicatorBehavior(settings, underlyingReplicator = None)

  /**
   * The `Behavior` for the `Replicator` actor.
   * It will use the given underlying [[akka.cluster.ddata.Replicator]]
   */
  def behavior(settings: ReplicatorSettings, underlyingReplicator: akka.actor.ActorRef): Behavior[Command] =
    ReplicatorBehavior(settings, Some(underlyingReplicator))

  type ReadConsistency = dd.Replicator.ReadConsistency
  val ReadLocal = dd.Replicator.ReadLocal
  object ReadFrom {
    def apply(n: Int, timeout: FiniteDuration): ReadFrom =
      dd.Replicator.ReadFrom(n, timeout)
  }
  type ReadFrom = dd.Replicator.ReadFrom
  object ReadMajority {
    def apply(timeout: FiniteDuration): ReadMajority =
      dd.Replicator.ReadMajority(timeout: FiniteDuration)
    def apply(timeout: FiniteDuration, minCap: Int): ReadMajority =
      dd.Replicator.ReadMajority(timeout: FiniteDuration, minCap)
  }
  type ReadMajority = dd.Replicator.ReadMajority
  object ReadAll {
    def apply(timeout: FiniteDuration): ReadAll =
      dd.Replicator.ReadAll(timeout: FiniteDuration)
  }
  type ReadAll = dd.Replicator.ReadAll

  type WriteConsistency = dd.Replicator.WriteConsistency
  val WriteLocal = dd.Replicator.WriteLocal
  object WriteTo {
    def apply(n: Int, timeout: FiniteDuration): WriteTo =
      dd.Replicator.WriteTo(n, timeout: FiniteDuration)
  }
  type WriteTo = dd.Replicator.WriteTo
  object WriteMajority {
    def apply(timeout: FiniteDuration): WriteMajority =
      dd.Replicator.WriteMajority(timeout: FiniteDuration)
    def apply(timeout: FiniteDuration, minCap: Int): WriteMajority =
      dd.Replicator.WriteMajority(timeout: FiniteDuration, minCap)
  }
  type WriteMajority = dd.Replicator.WriteMajority
  object WriteAll {
    def apply(timeout: FiniteDuration): WriteAll =
      dd.Replicator.WriteAll(timeout: FiniteDuration)
  }
  type WriteAll = dd.Replicator.WriteAll

  trait Command

  object Get {

    /**
     * Convenience for `ask`.
     */
    def apply[A <: ReplicatedData](key: Key[A], consistency: ReadConsistency): ActorRef[GetResponse[A]] => Get[A] =
      replyTo => Get(key, consistency, replyTo)
  }

  /**
   * Send this message to the local `Replicator` to retrieve a data value for the
   * given `key`. The `Replicator` will reply with one of the [[GetResponse]] messages.
   */
  final case class Get[A <: ReplicatedData](
      key: Key[A],
      consistency: ReadConsistency,
      replyTo: ActorRef[GetResponse[A]])
      extends Command

  /**
   * Reply from `Get`. The data value is retrieved with [[dd.Replicator.GetSuccess.get]] using the typed key.
   */
  type GetResponse[A <: ReplicatedData] = dd.Replicator.GetResponse[A]
  object GetSuccess {
    def unapply[A <: ReplicatedData](rsp: GetSuccess[A]): Option[Key[A]] = Some(rsp.key)
  }
  type GetSuccess[A <: ReplicatedData] = dd.Replicator.GetSuccess[A]
  type NotFound[A <: ReplicatedData] = dd.Replicator.NotFound[A]
  object NotFound {
    def unapply[A <: ReplicatedData](rsp: NotFound[A]): Option[Key[A]] = Some(rsp.key)
  }

  /**
   * The [[Get]] request could not be fulfill according to the given
   * [[ReadConsistency consistency level]] and [[ReadConsistency#timeout timeout]].
   */
  type GetFailure[A <: ReplicatedData] = dd.Replicator.GetFailure[A]
  object GetFailure {
    def unapply[A <: ReplicatedData](rsp: GetFailure[A]): Option[Key[A]] = Some(rsp.key)
  }

  /**
   * The [[Get]] request couldn't be performed because the entry has been deleted.
   */
  type GetDataDeleted[A <: ReplicatedData] = dd.Replicator.GetDataDeleted[A]
  object GetDataDeleted {
    def unapply[A <: ReplicatedData](rsp: GetDataDeleted[A]): Option[Key[A]] =
      Some(rsp.key)
  }

  object Update {

    /**
     * Modify value of local `Replicator` and replicate with given `writeConsistency`.
     *
     * The current value for the `key` is passed to the `modify` function.
     * If there is no current data value for the `key` the `initial` value will be
     * passed to the `modify` function.
     */
    def apply[A <: ReplicatedData](
        key: Key[A],
        initial: A,
        writeConsistency: WriteConsistency,
        replyTo: ActorRef[UpdateResponse[A]])(modify: A => A): Update[A] =
      Update(key, writeConsistency, replyTo)(modifyWithInitial(initial, modify))

    /**
     * Convenience for `ask`.
     */
    def apply[A <: ReplicatedData](key: Key[A], initial: A, writeConsistency: WriteConsistency)(
        modify: A => A): ActorRef[UpdateResponse[A]] => Update[A] =
      replyTo => Update(key, writeConsistency, replyTo)(modifyWithInitial(initial, modify))

    private def modifyWithInitial[A <: ReplicatedData](initial: A, modify: A => A): Option[A] => A = {
      case Some(data) => modify(data)
      case None       => modify(initial)
    }
  }

  /**
   * Send this message to the local `Replicator` to update a data value for the
   * given `key`. The `Replicator` will reply with one of the [[UpdateResponse]] messages.
   *
   * Note that the [[Replicator.Update$ companion]] object provides `apply` functions for convenient
   * construction of this message.
   *
   * The current data value for the `key` is passed as parameter to the `modify` function.
   * It is `None` if there is no value for the `key`, and otherwise `Some(data)`. The function
   * is supposed to return the new value of the data, which will then be replicated according to
   * the given `writeConsistency`.
   *
   * The `modify` function is called by the `Replicator` actor and must therefore be a pure
   * function that only uses the data parameter and stable fields from enclosing scope. It must
   * for example not access `sender()` reference of an enclosing actor.
   */
  final case class Update[A <: ReplicatedData](
      key: Key[A],
      writeConsistency: WriteConsistency,
      replyTo: ActorRef[UpdateResponse[A]])(val modify: Option[A] => A)
      extends Command

  type UpdateResponse[A <: ReplicatedData] = dd.Replicator.UpdateResponse[A]
  type UpdateSuccess[A <: ReplicatedData] = dd.Replicator.UpdateSuccess[A]
  object UpdateSuccess {
    def unapply[A <: ReplicatedData](rsp: UpdateSuccess[A]): Option[Key[A]] =
      Some(rsp.key)
  }
  type UpdateFailure[A <: ReplicatedData] = dd.Replicator.UpdateFailure[A]
  object UpdateFailure {
    def unapply[A <: ReplicatedData](rsp: UpdateFailure[A]): Option[Key[A]] =
      Some(rsp.key)
  }

  /**
   * The direct replication of the [[Update]] could not be fulfill according to
   * the given [[WriteConsistency consistency level]] and
   * [[WriteConsistency#timeout timeout]].
   *
   * The `Update` was still performed locally and possibly replicated to some nodes.
   * It will eventually be disseminated to other replicas, unless the local replica
   * crashes before it has been able to communicate with other replicas.
   */
  type UpdateTimeout[A <: ReplicatedData] = dd.Replicator.UpdateTimeout[A]
  object UpdateTimeout {
    def unapply[A <: ReplicatedData](rsp: UpdateTimeout[A]): Option[Key[A]] =
      Some(rsp.key)
  }

  /**
   * The [[Update]] couldn't be performed because the entry has been deleted.
   */
  type UpdateDataDeleted[A <: ReplicatedData] = dd.Replicator.UpdateDataDeleted[A]
  object UpdateDataDeleted {
    def unapply[A <: ReplicatedData](rsp: UpdateDataDeleted[A]): Option[Key[A]] =
      Some(rsp.key)
  }

  /**
   * If the `modify` function of the [[Update]] throws an exception the reply message
   * will be this `ModifyFailure` message. The original exception is included as `cause`.
   */
  type ModifyFailure[A <: ReplicatedData] = dd.Replicator.ModifyFailure[A]
  object ModifyFailure {
    def unapply[A <: ReplicatedData](rsp: ModifyFailure[A]): Option[(Key[A], String, Throwable)] =
      Some((rsp.key, rsp.errorMessage, rsp.cause))
  }

  /**
   * The local store or direct replication of the [[Update]] could not be fulfill according to
   * the given [[WriteConsistency consistency level]] due to durable store errors. This is
   * only used for entries that have been configured to be durable.
   *
   * The `Update` was still performed in memory locally and possibly replicated to some nodes,
   * but it might not have been written to durable storage.
   * It will eventually be disseminated to other replicas, unless the local replica
   * crashes before it has been able to communicate with other replicas.
   */
  type StoreFailure[A <: ReplicatedData] = dd.Replicator.StoreFailure[A]
  object StoreFailure {
    def unapply[A <: ReplicatedData](rsp: StoreFailure[A]): Option[Key[A]] =
      Some(rsp.key)
  }

  /**
   * Register a subscriber that will be notified with a [[Changed]] message
   * when the value of the given `key` is changed. Current value is also
   * sent as a [[Changed]] message to a new subscriber.
   *
   * Subscribers will be notified periodically with the configured `notify-subscribers-interval`,
   * and it is also possible to send an explicit `FlushChanges` message to
   * the `Replicator` to notify the subscribers immediately.
   *
   * The subscriber will automatically be unregistered if it is terminated.
   *
   * If the key is deleted the subscriber is notified with a [[Deleted]]
   * message.
   */
  final case class Subscribe[A <: ReplicatedData](key: Key[A], subscriber: ActorRef[SubscribeResponse[A]])
      extends Command

  /**
   * Unregister a subscriber.
   *
   * @see [[Subscribe]]
   */
  final case class Unsubscribe[A <: ReplicatedData](key: Key[A], subscriber: ActorRef[SubscribeResponse[A]])
      extends Command

  /**
   * @see [[Subscribe]]
   */
  type SubscribeResponse[A <: ReplicatedData] = dd.Replicator.SubscribeResponse[A]

  /**
   * The data value is retrieved with [[dd.Replicator.Changed.get]] using the typed key.
   *
   * @see [[Subscribe]]
   */
  object Changed {
    def unapply[A <: ReplicatedData](chg: Changed[A]): Option[Key[A]] = Some(chg.key)
  }

  /**
   * The data value is retrieved with [[dd.Replicator.Changed.get]] using the typed key.
   *
   * @see [[Subscribe]]
   */
  type Changed[A <: ReplicatedData] = dd.Replicator.Changed[A]

  object Deleted {
    def unapply[A <: ReplicatedData](del: Deleted[A]): Option[Key[A]] = Some(del.key)
  }

  /**
   * @see [[Delete]]
   */
  type Deleted[A <: ReplicatedData] = dd.Replicator.Deleted[A]

  object Expired {
    def unapply[A <: ReplicatedData](exp: Expired[A]): Option[Key[A]] = Some(exp.key)
  }

  /**
   * @see [[Expired]]
   */
  type Expired[A <: ReplicatedData] = dd.Replicator.Expired[A]

  object Delete {

    /**
     * Convenience for `ask`.
     */
    def apply[A <: ReplicatedData](
        key: Key[A],
        consistency: WriteConsistency): ActorRef[DeleteResponse[A]] => Delete[A] =
      replyTo => Delete(key, consistency, replyTo)
  }

  /**
   * Send this message to the local `Replicator` to delete a data value for the
   * given `key`. The `Replicator` will reply with one of the [[DeleteResponse]] messages.
   */
  final case class Delete[A <: ReplicatedData](
      key: Key[A],
      consistency: WriteConsistency,
      replyTo: ActorRef[DeleteResponse[A]])
      extends Command

  type DeleteResponse[A <: ReplicatedData] = dd.Replicator.DeleteResponse[A]
  type DeleteSuccess[A <: ReplicatedData] = dd.Replicator.DeleteSuccess[A]
  object DeleteSuccess {
    def unapply[A <: ReplicatedData](rsp: DeleteSuccess[A]): Option[Key[A]] =
      Some(rsp.key)
  }
  type DeleteFailure[A <: ReplicatedData] = dd.Replicator.ReplicationDeleteFailure[A]
  object DeleteFailure {
    def unapply[A <: ReplicatedData](rsp: DeleteFailure[A]): Option[Key[A]] =
      Some(rsp.key)
  }
  type DataDeleted[A <: ReplicatedData] = dd.Replicator.DataDeleted[A]
  object DataDeleted {
    def unapply[A <: ReplicatedData](rsp: DataDeleted[A]): Option[Key[A]] =
      Some(rsp.key)
  }

  object GetReplicaCount {

    /**
     * Convenience for `ask`.
     */
    def apply(): ActorRef[ReplicaCount] => GetReplicaCount =
      replyTo => GetReplicaCount(replyTo)
  }

  /**
   * Get current number of replicas, including the local replica.
   * Will reply to sender with [[ReplicaCount]].
   */
  final case class GetReplicaCount(replyTo: ActorRef[ReplicaCount]) extends Command

  /**
   * Current number of replicas. Reply to `GetReplicaCount`.
   */
  type ReplicaCount = dd.Replicator.ReplicaCount
  object ReplicaCount {
    def unapply[A <: ReplicatedData](rsp: ReplicaCount): Option[Int] =
      Some(rsp.n)
  }

  /**
   * Notify subscribers of changes now, otherwise they will be notified periodically
   * with the configured `notify-subscribers-interval`.
   */
  object FlushChanges extends Command

}
