/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.Optional

import scala.jdk.OptionConverters._

import akka.cluster.UniqueAddress

/**
 * Interface for implementing a state based convergent
 * replicated data type (CvRDT).
 *
 * ReplicatedData types must be serializable with an Akka
 * Serializer. It is highly recommended to implement a serializer with
 * Protobuf or similar. The built in data types are marked with
 * [[ReplicatedDataSerialization]] and serialized with
 * [[akka.cluster.ddata.protobuf.ReplicatedDataSerializer]].
 *
 * Serialization of the data types are used in remote messages and also
 * for creating message digests (SHA-1) to detect changes. Therefore it is
 * important that the serialization produce the same bytes for the same content.
 * For example sets and maps should be sorted deterministically in the serialization.
 *
 * ReplicatedData types should be immutable, i.e. "modifying" methods should return
 * a new instance.
 *
 * Implement the additional methods of [[DeltaReplicatedData]] if
 * it has support for delta-CRDT replication.
 */
trait ReplicatedData {

  /**
   * The type of the concrete implementation, e.g. `GSet[A]`.
   * To be specified by subclass.
   */
  type T <: ReplicatedData

  /**
   * Monotonic merge function.
   */
  def merge(that: T): T

}

/**
 * [[ReplicatedData]] with additional support for delta-CRDT replication.
 * delta-CRDT is a way to reduce the need for sending the full state
 * for updates. For example adding element 'c' and 'd' to set {'a', 'b'} would
 * result in sending the delta {'c', 'd'} and merge that with the state on the
 * receiving side, resulting in set {'a', 'b', 'c', 'd'}.
 *
 * Learn more about this in the paper
 * <a href="https://arxiv.org/abs/1603.01529">Delta State Replicated Data Types</a>.
 */
trait DeltaReplicatedData extends ReplicatedData {

  /**
   * The type of the delta. To be specified by subclass.
   * It may be the same type as `T` or a different type if needed.
   * For example `GSet` uses the same type and `ORSet` uses different types.
   */
  type D <: ReplicatedDelta

  /**
   * The accumulated delta of mutator operations since previous
   * [[#resetDelta]]. When the `Replicator` invokes the `modify` function
   * of the `Update` message and the user code is invoking one or more mutator
   * operations the data is collecting the delta of the operations and makes
   * it available for the `Replicator` with the [[#delta]] accessor. The
   * `modify` function shall still return the full state in the same way as
   * `ReplicatedData` without support for deltas.
   */
  def delta: Option[D]

  /**
   * When delta is merged into the full state this method is used.
   * When the type `D` of the delta is of the same type as the full state `T`
   * this method can be implemented by delegating to `merge`.
   */
  def mergeDelta(thatDelta: D): T

  /**
   * Reset collection of deltas from mutator operations. When the `Replicator`
   * invokes the `modify` function of the `Update` message the delta is always
   * "reset" and when the user code is invoking one or more mutator operations the
   * data is collecting the delta of the operations and makes it available for
   * the `Replicator` with the [[#delta]] accessor. When the `Replicator` has
   * grabbed the `delta` it will invoke this method to get a clean data instance
   * without the delta.
   */
  def resetDelta: T

}

/**
 * The delta must implement this type.
 */
trait ReplicatedDelta extends ReplicatedData {

  /**
   * The empty full state. This is used when a delta is received
   * and no existing full state exists on the receiving side. Then
   * the delta is merged into the `zero` to create the initial full state.
   */
  def zero: DeltaReplicatedData
}

/**
 * Marker that specifies that the deltas must be applied in causal order.
 * There is some overhead of managing the causal delivery so it should only
 * be used for types that need it.
 *
 * Note that if the full state type `T` is different from the delta type `D`
 * it is the delta `D` that should be marked with this.
 */
trait RequiresCausalDeliveryOfDeltas extends ReplicatedDelta

/**
 * Some complex deltas grow in size for each update and above a configured
 * threshold such deltas are discarded and sent as full state instead. This
 * interface should be implemented by such deltas to define its size.
 * This is number of elements or similar size hint, not size in bytes.
 * The threshold is defined in `akka.cluster.distributed-data.delta-crdt.max-delta-size`
 * or corresponding [[ReplicatorSettings]].
 */
trait ReplicatedDeltaSize {
  def deltaSize: Int
}

/**
 * Java API: Interface for implementing a [[ReplicatedData]] in Java.
 *
 * The type parameter `A` is a self-recursive type to be defined by the
 * concrete implementation.
 * E.g. `class TwoPhaseSet extends AbstractReplicatedData&lt;TwoPhaseSet&gt;`
 */
abstract class AbstractReplicatedData[A <: AbstractReplicatedData[A]] extends ReplicatedData {

  override type T = ReplicatedData

  /**
   * Delegates to [[#mergeData]], which must be implemented by subclass.
   */
  final override def merge(that: ReplicatedData): ReplicatedData =
    mergeData(that.asInstanceOf[A])

  /**
   * Java API: Monotonic merge function.
   */
  def mergeData(that: A): A

}

/**
 * Java API: Interface for implementing a [[DeltaReplicatedData]] in Java.
 *
 * The type parameter `A` is a self-recursive type to be defined by the
 * concrete implementation.
 * E.g. `class TwoPhaseSet extends AbstractDeltaReplicatedData&lt;TwoPhaseSet, TwoPhaseSet&gt;`
 */
abstract class AbstractDeltaReplicatedData[A <: AbstractDeltaReplicatedData[A, B], B <: ReplicatedDelta]
    extends AbstractReplicatedData[A]
    with DeltaReplicatedData {

  override type D = ReplicatedDelta

  /**
   * Delegates to [[#deltaData]], which must be implemented by subclass.
   */
  final override def delta: Option[ReplicatedDelta] =
    deltaData.toScala

  /**
   * The accumulated delta of mutator operations since previous
   * [[#resetDelta]]. When the `Replicator` invokes the `modify` function
   * of the `Update` message and the user code is invoking one or more mutator
   * operations the data is collecting the delta of the operations and makes
   * it available for the `Replicator` with the [[#deltaData]] accessor. The
   * `modify` function shall still return the full state in the same way as
   * `ReplicatedData` without support for deltas.
   */
  def deltaData: Optional[B]

  /**
   * Delegates to [[#mergeDeltaData]], which must be implemented by subclass.
   */
  final override def mergeDelta(that: ReplicatedDelta): ReplicatedData =
    mergeDeltaData(that.asInstanceOf[B])

  /**
   * When delta is merged into the full state this method is used.
   * When the type `D` of the delta is of the same type as the full state `T`
   * this method can be implemented by delegating to `mergeData`.
   */
  def mergeDeltaData(that: B): A

}

/**
 * [[ReplicatedData]] that has support for pruning of data
 * belonging to a specific node may implement this interface.
 * When a node is removed from the cluster these methods will be
 * used by the [[Replicator]] to collapse data from the removed node
 * into some other node in the cluster.
 *
 * See process description in the 'CRDT Garbage' section of the [[Replicator]]
 * documentation.
 */
trait RemovedNodePruning extends ReplicatedData {

  /**
   * The nodes that have changed the state for this data
   * and would need pruning when such node is no longer part
   * of the cluster.
   */
  def modifiedByNodes: Set[UniqueAddress]

  /**
   * Does it have any state changes from a specific node,
   * which has been removed from the cluster.
   */
  def needPruningFrom(removedNode: UniqueAddress): Boolean

  /**
   * When the `removed` node has been removed from the cluster the state
   * changes from that node will be pruned by collapsing the data entries
   * to another node.
   */
  def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T

  /**
   * Remove data entries from a node that has been removed from the cluster
   * and already been pruned.
   */
  def pruningCleanup(removedNode: UniqueAddress): T
}

/**
 * Marker trait for `ReplicatedData` serialized by
 * [[akka.cluster.ddata.protobuf.ReplicatedDataSerializer]].
 */
trait ReplicatedDataSerialization extends Serializable
