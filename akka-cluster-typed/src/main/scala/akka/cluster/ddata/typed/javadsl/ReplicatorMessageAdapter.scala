/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.javadsl

import java.time.Duration
import java.util.function.{ Function => JFunction }

import scala.jdk.DurationConverters._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.javadsl.ActorContext
import akka.cluster.ddata.Key
import akka.cluster.ddata.ReplicatedData
import akka.util.Timeout

/**
 * When interacting with the `Replicator` from an actor this class provides convenient
 * methods that adapts the response messages to the requesting actor's message protocol.
 *
 * One `ReplicatorMessageAdapter` instance can be used for a given `ReplicatedData` type,
 * e.g. an `OrSet<String>`. Interaction with several [[Key]]s can be used via the same adapter
 * but they must all be of the same `ReplicatedData` type. For interaction with several different
 * `ReplicatedData` types, e.g. an `OrSet<String>` and a `GCounter`, an adapter can be created
 * for each type.
 *
 * For the default replicator in the [[DistributedData]] extension a `ReplicatorMessageAdapter`
 * can be created with [[DistributedData.withReplicatorMessageAdapter]].
 *
 * *Warning*: `ReplicatorMessageAdapter` is not thread-safe and must only be used from the actor
 * corresponding to the given `ActorContext`. It must not be accessed from threads other
 * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]]
 * callbacks. It must not be shared between several actor instances.
 *
 * @param context              The [[ActorContext]] of the requesting actor.  The `ReplicatorMessageAdapter` can
 *                             only be used in this actor.
 * @param replicator           The replicator to interact with, typically `DistributedData.get(system).replicator`.
 * @param unexpectedAskTimeout The timeout to use for `ask` operations. This should be longer than
 *                             the `timeout` given in [[Replicator.WriteConsistency]] and
 *                             [[Replicator.ReadConsistency]]. The replicator will always send
 *                             a reply within those timeouts so the `unexpectedAskTimeout` should
 *                             not occur, but for cleanup in a failure situation it must still exist.
 *                             If `askUpdate`, `askGet` or `askDelete` takes longer then this
 *                             `unexpectedAskTimeout` a [[java.util.concurrent.TimeoutException]]
 *                             will be thrown by the requesting actor and may be handled by supervision.
 * @tparam A Message type of the requesting actor.
 * @tparam B Type of the [[ReplicatedData]].
 */
class ReplicatorMessageAdapter[A, B <: ReplicatedData](
    context: ActorContext[A],
    replicator: ActorRef[Replicator.Command],
    unexpectedAskTimeout: Duration) {

  private implicit val askTimeout: Timeout = Timeout(unexpectedAskTimeout.toScala)

  private var changedMessageAdapters: Map[Key[B], ActorRef[Replicator.SubscribeResponse[B]]] = Map.empty

  /**
   * Subscribe to changes of the given `key`. The [[Replicator.Changed]] and [[Replicator.Deleted]] messages from
   * the replicator are transformed to the message protocol of the requesting actor with
   * the given `responseAdapter` function.
   */
  def subscribe(key: Key[B], responseAdapter: akka.japi.function.Function[Replicator.SubscribeResponse[B], A]): Unit = {
    // unsubscribe in case it's called more than once per key
    unsubscribe(key)

    val replyTo: ActorRef[Replicator.SubscribeResponse[B]] =
      context.messageAdapter(classOf[Replicator.SubscribeResponse[B]], responseAdapter)
    changedMessageAdapters = changedMessageAdapters.updated(key, replyTo)
    replicator ! Replicator.Subscribe(key, replyTo)
  }

  /**
   * Unsubscribe from a previous subscription of a given `key`.
   * @see [[ReplicatorMessageAdapter.subscribe]]
   */
  def unsubscribe(key: Key[B]): Unit = {
    changedMessageAdapters.get(key).foreach { subscriber =>
      replicator ! Replicator.Unsubscribe(key, subscriber)
    }
  }

  /**
   * Send a [[Replicator.Update]] request to the replicator. The [[Replicator.UpdateResponse]]
   * message is transformed to the message protocol of the requesting actor with the given
   * `responseAdapter` function.
   *
   * Note that `createRequest` is a function that creates the `Update` message from the provided
   * `ActorRef[UpdateResponse]` that the the replicator will send the response message back through.
   * Use that `ActorRef[UpdateResponse]` as the `replyTo` parameter in the `Update` message.
   */
  def askUpdate(
      createRequest: JFunction[ActorRef[Replicator.UpdateResponse[B]], Replicator.Update[B]],
      responseAdapter: JFunction[Replicator.UpdateResponse[B], A]): Unit = {
    context.asScala
      .ask[Replicator.Update[B], Replicator.UpdateResponse[B]](replicator, askReplyTo => createRequest(askReplyTo)) {
        case Success(value) => responseAdapter(value)
        case Failure(ex)    => throw ex // unexpected ask timeout
      }
  }

  /**
   * Send a [[Replicator.Get]] request to the replicator. The [[Replicator.GetResponse]]
   * message is transformed to the message protocol of the requesting actor with the given
   * `responseAdapter` function.
   *
   * Note that `createRequest` is a function that creates the `Get` message from the provided
   * `ActorRef[GetResponse]` that the the replicator will send the response message back through.
   * Use that `ActorRef[GetResponse]` as the `replyTo` parameter in the `Get` message.
   */
  def askGet(
      createRequest: JFunction[ActorRef[Replicator.GetResponse[B]], Replicator.Get[B]],
      responseAdapter: JFunction[Replicator.GetResponse[B], A]): Unit = {
    context.asScala
      .ask[Replicator.Get[B], Replicator.GetResponse[B]](replicator, askReplyTo => createRequest(askReplyTo)) {
        case Success(value) => responseAdapter(value)
        case Failure(ex)    => throw ex // unexpected ask timeout
      }
  }

  /**
   * Send a [[Replicator.Delete]] request to the replicator. The [[Replicator.DeleteResponse]]
   * message is transformed to the message protocol of the requesting actor with the given
   * `responseAdapter` function.
   *
   * Note that `createRequest` is a function that creates the `Delete` message from the provided
   * `ActorRef[DeleteResponse]` that the the replicator will send the response message back through.
   * Use that `ActorRef[DeleteResponse]` as the `replyTo` parameter in the `Delete` message.
   */
  def askDelete(
      createRequest: JFunction[ActorRef[Replicator.DeleteResponse[B]], Replicator.Delete[B]],
      responseAdapter: JFunction[Replicator.DeleteResponse[B], A]): Unit = {
    context.asScala
      .ask[Replicator.Delete[B], Replicator.DeleteResponse[B]](replicator, askReplyTo => createRequest(askReplyTo)) {
        case Success(value) => responseAdapter(value)
        case Failure(ex)    => throw ex // unexpected ask timeout
      }
  }

  /**
   * Send a [[Replicator.GetReplicaCount]] request to the replicator. The [[Replicator.ReplicaCount]]
   * message is transformed to the message protocol of the requesting actor with the given
   * `responseAdapter` function.
   *
   * Note that `createRequest` is a function that creates the `GetReplicaCount` message from the provided
   * `ActorRef[ReplicaCount]` that the the replicator will send the response message back through.
   * Use that `ActorRef[ReplicaCount]` as the `replyTo` parameter in the `GetReplicaCount` message.
   */
  def askReplicaCount(
      createRequest: JFunction[ActorRef[Replicator.ReplicaCount], Replicator.GetReplicaCount],
      responseAdapter: JFunction[Replicator.ReplicaCount, A]): Unit = {
    context.asScala
      .ask[Replicator.GetReplicaCount, Replicator.ReplicaCount](replicator, askReplyTo => createRequest(askReplyTo)) {
        case Success(value) => responseAdapter(value)
        case Failure(ex)    => throw ex // unexpected ask timeout
      }
  }

}
