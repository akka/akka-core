/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.util.function.{ BiFunction, Supplier, ToLongBiFunction }

import scala.annotation.nowarn

import akka.NotUsed
import akka.annotation.DoNotInherit

/**
 * A MergeHub is a special streaming hub that is able to collect streamed elements from a dynamic set of
 * producers. It consists of two parts, a [[Source]] and a [[Sink]]. The [[Source]] streams the element to a consumer from
 * its merged inputs. Once the consumer has been materialized, the [[Source]] returns a materialized value which is
 * the corresponding [[Sink]]. This [[Sink]] can then be materialized arbitrary many times, where each of the new
 * materializations will feed its consumed elements to the original [[Source]].
 */
object MergeHub {

  /**
   * A DrainingControl object is created during the materialization of a MergeHub and allows to initiate the draining
   * and eventual completion of the Hub from the outside.
   *
   * Not for user extension
   */
  @DoNotInherit
  sealed trait DrainingControl {

    /**
     * Set the operation mode of the linked MergeHub to draining. In this mode the Hub will cancel any new producer and
     * will complete as soon as all the currently connected producers complete.
     */
    def drainAndComplete(): Unit
  }

  private final class DrainingControlImpl(c: akka.stream.scaladsl.MergeHub.DrainingControl) extends DrainingControl {
    override def drainAndComplete(): Unit = c.drainAndComplete()
  }

  /**
   * Creates a [[Source]] that emits elements merged from a dynamic set of producers. After the [[Source]] returned
   * by this method is materialized, it returns a [[Sink]] as a materialized value. This [[Sink]] can be materialized
   * arbitrary many times and each of the materializations will feed the elements into the original [[Source]].
   *
   * Every new materialization of the [[Source]] results in a new, independent hub, which materializes to its own
   * [[Sink]] for feeding that materialization.
   *
   * Completed or failed [[Sink]]s are simply removed. Once the [[Source]] is cancelled, the Hub is considered closed
   * and any new producers using the [[Sink]] will be cancelled.
   *
   * @param clazz Type of elements this hub emits and consumes
   * @param perProducerBufferSize Buffer space used per producer.
   */
  def of[T](@nowarn("msg=never used") clazz: Class[T], perProducerBufferSize: Int): Source[T, Sink[T, NotUsed]] = {
    akka.stream.scaladsl.MergeHub.source[T](perProducerBufferSize).mapMaterializedValue(_.asJava[T]).asJava
  }

  /**
   * Creates a [[Source]] that emits elements merged from a dynamic set of producers. After the [[Source]] returned
   * by this method is materialized, it returns a [[Sink]] as a materialized value. This [[Sink]] can be materialized
   * arbitrarily many times and each of the materializations will feed the elements into the original [[Source]].
   *
   * Every new materialization of the [[Source]] results in a new, independent hub, which materializes to its own
   * [[Sink]] for feeding that materialization.
   *
   * Completed or failed [[Sink]]s are simply removed. Once the [[Source]] is cancelled, the Hub is considered closed
   * and any new producers using the [[Sink]] will be cancelled.
   *
   * The materialized [[DrainingControl]] can be used to drain the Hub: any new produces using the [[Sink]] will be cancelled
   * and the Hub will be closed completing the [[Source]] as soon as all currently connected producers complete.
   *
   * @param clazz Type of elements this hub emits and consumes
   * @param perProducerBufferSize Buffer space used per producer. Default value is 16.
   */
  def withDraining[T](
      @nowarn("msg=never used") clazz: Class[T],
      perProducerBufferSize: Int): Source[T, akka.japi.Pair[Sink[T, NotUsed], DrainingControl]] = {
    akka.stream.scaladsl.MergeHub
      .sourceWithDraining[T](perProducerBufferSize)
      .mapMaterializedValue {
        case (sink, draining) =>
          akka.japi.Pair(sink.asJava[T], new DrainingControlImpl(draining): DrainingControl)
      }
      .asJava
  }

  /**
   * Creates a [[Source]] that emits elements merged from a dynamic set of producers. After the [[Source]] returned
   * by this method is materialized, it returns a [[Sink]] as a materialized value. This [[Sink]] can be materialized
   * arbitrary many times and each of the materializations will feed the elements into the original [[Source]].
   *
   * Every new materialization of the [[Source]] results in a new, independent hub, which materializes to its own
   * [[Sink]] for feeding that materialization.
   *
   * Completed or failed [[Sink]]s are simply removed. Once the [[Source]] is cancelled, the Hub is considered closed
   * and any new producers using the [[Sink]] will be cancelled.
   *
   * @param clazz Type of elements this hub emits and consumes
   */
  def of[T](clazz: Class[T]): Source[T, Sink[T, NotUsed]] = of(clazz, 16)

  /**
   * Creates a [[Source]] that emits elements merged from a dynamic set of producers. After the [[Source]] returned
   * by this method is materialized, it returns a [[Sink]] as a materialized value. This [[Sink]] can be materialized
   * arbitrarily many times and each of the materializations will feed the elements into the original [[Source]].
   *
   * Every new materialization of the [[Source]] results in a new, independent hub, which materializes to its own
   * [[Sink]] for feeding that materialization.
   *
   * Completed or failed [[Sink]]s are simply removed. Once the [[Source]] is cancelled, the Hub is considered closed
   * and any new producers using the [[Sink]] will be cancelled.
   *
   * The materialized [[DrainingControl]] can be used to drain the Hub: any new produces using the [[Sink]] will be cancelled
   * and the Hub will be closed completing the [[Source]] as soon as all currently connected producers complete.
   *
   * @param clazz Type of elements this hub emits and consumes
   */
  def withDraining[T](clazz: Class[T]): Source[T, akka.japi.Pair[Sink[T, NotUsed], DrainingControl]] =
    withDraining(clazz, 16)

}

/**
 * A BroadcastHub is a special streaming hub that is able to broadcast streamed elements to a dynamic set of consumers.
 * It consists of two parts, a [[Sink]] and a [[Source]]. The [[Sink]] broadcasts elements from a producer to the
 * actually live consumers it has. Once the producer has been materialized, the [[Sink]] it feeds into returns a
 * materialized value which is the corresponding [[Source]]. This [[Source]] can be materialized an arbitrary number
 * of times, where each of the new materializations will receive their elements from the original [[Sink]].
 */
object BroadcastHub {

  /**
   * Creates a [[Sink]] that receives elements from its upstream producer and broadcasts them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * broadcast elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * @param clazz Type of elements this hub emits and consumes
   * @param bufferSize Buffer size used by the producer. Gives an upper bound on how "far" from each other two
   *                   concurrent consumers can be in terms of element. If the buffer is full, the producer
   *                   is backpressured. Must be a power of two and less than 4096.
   */
  def of[T](@nowarn("msg=never used") clazz: Class[T], bufferSize: Int): Sink[T, Source[T, NotUsed]] = {
    akka.stream.scaladsl.BroadcastHub.sink[T](bufferSize).mapMaterializedValue(_.asJava).asJava
  }

  /**
   * Creates a [[Sink]] that receives elements from its upstream producer and broadcasts them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * broadcast elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * @param clazz Type of elements this hub emits and consumes
   * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
   *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
   *   been removed (canceled).
   * @param bufferSize Buffer size used by the producer. Gives an upper bound on how "far" from each other two
   *                   concurrent consumers can be in terms of element. If the buffer is full, the producer
   *                   is backpressured. Must be a power of two and less than 4096.
   */
  def of[T](
      @nowarn("msg=never used") clazz: Class[T],
      startAfterNrOfConsumers: Int,
      bufferSize: Int): Sink[T, Source[T, NotUsed]] = {
    akka.stream.scaladsl.BroadcastHub.sink[T](startAfterNrOfConsumers, bufferSize).mapMaterializedValue(_.asJava).asJava
  }

  /**
   * Creates a [[Sink]] with default buffer size 256 that receives elements from its upstream producer and broadcasts them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * broadcast elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * @param clazz Type of elements this hub emits and consumes
   */
  def of[T](clazz: Class[T]): Sink[T, Source[T, NotUsed]] =
    of(clazz, akka.stream.scaladsl.BroadcastHub.defaultBufferSize)

}

/**
 * A `PartitionHub` is a special streaming hub that is able to route streamed elements to a dynamic set of consumers.
 * It consists of two parts, a [[Sink]] and a [[Source]]. The [[Sink]] e elements from a producer to the
 * actually live consumers it has. The selection of consumer is done with a function. Each element can be routed to
 * only one consumer. Once the producer has been materialized, the [[Sink]] it feeds into returns a
 * materialized value which is the corresponding [[Source]]. This [[Source]] can be materialized an arbitrary number
 * of times, where each of the new materializations will receive their elements from the original [[Sink]].
 */
object PartitionHub {

  /**
   * Creates a [[Sink]] that receives elements from its upstream producer and routes them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * This `statefulSink` should be used when there is a need to keep mutable state in the partition function,
   * e.g. for implementing round-robin or sticky session kind of routing. If state is not needed the [[#of]] can
   * be more convenient to use.
   *
   * @param partitioner Function that decides where to route an element. It is a factory of a function to
   *   to be able to hold stateful variables that are unique for each materialization. The function
   *   takes two parameters; the first is information about active consumers, including an array of consumer
   *   identifiers and the second is the stream element. The function should return the selected consumer
   *   identifier for the given element. The function will never be called when there are no active consumers,
   *   i.e. there is always at least one element in the array of identifiers.
   * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
   *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
   *   been removed (canceled).
   * @param bufferSize Total number of elements that can be buffered. If this buffer is full, the producer
   *   is backpressured.
   */
  def ofStateful[T](
      @nowarn("msg=never used") clazz: Class[T],
      partitioner: Supplier[ToLongBiFunction[ConsumerInfo, T]],
      startAfterNrOfConsumers: Int,
      bufferSize: Int): Sink[T, Source[T, NotUsed]] = {
    val p: () => (akka.stream.scaladsl.PartitionHub.ConsumerInfo, T) => Long = () => {
      val f = partitioner.get()
      (info, elem) => f.applyAsLong(info, elem)
    }
    akka.stream.scaladsl.PartitionHub
      .statefulSink[T](p, startAfterNrOfConsumers, bufferSize)
      .mapMaterializedValue(_.asJava)
      .asJava
  }

  /**
   * Creates a [[Sink]] with default buffer size 256 that receives elements from its upstream producer and routes them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * This `statefulSink` should be used when there is a need to keep mutable state in the partition function,
   * e.g. for implementing round-robin or sticky session kind of routing. If state is not needed the [[#of]] can
   * be more convenient to use.
   *
   * @param partitioner Function that decides where to route an element. It is a factory of a function to
   *   to be able to hold stateful variables that are unique for each materialization. The function
   *   takes two parameters; the first is information about active consumers, including an array of consumer
   *   identifiers and the second is the stream element. The function should return the selected consumer
   *   identifier for the given element. The function will never be called when there are no active consumers,
   *   i.e. there is always at least one element in the array of identifiers.
   * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
   *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
   *   been removed (canceled).
   */
  def ofStateful[T](
      clazz: Class[T],
      partitioner: Supplier[ToLongBiFunction[ConsumerInfo, T]],
      startAfterNrOfConsumers: Int): Sink[T, Source[T, NotUsed]] =
    ofStateful(clazz, partitioner, startAfterNrOfConsumers, akka.stream.scaladsl.PartitionHub.defaultBufferSize)

  /**
   * Creates a [[Sink]] that receives elements from its upstream producer and routes them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * This `sink` should be used when the routing function is stateless, e.g. based on a hashed value of the
   * elements. Otherwise the [[#ofStateful]] can be used to implement more advanced routing logic.
   *
   * @param partitioner Function that decides where to route an element. The function takes two parameters;
   *   the first is the number of active consumers and the second is the stream element. The function should
   *   return the index of the selected consumer for the given element, i.e. int greater than or equal to 0
   *   and less than number of consumers. E.g. `(size, elem) -> Math.abs(elem.hashCode() % size)`. It's also
   *   possible to use `-1` to drop the element.
   * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
   *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
   *   been removed (canceled).
   * @param bufferSize Total number of elements that can be buffered. If this buffer is full, the producer
   *   is backpressured.
   */
  def of[T](
      @nowarn("msg=never used") clazz: Class[T],
      partitioner: BiFunction[Integer, T, Integer],
      startAfterNrOfConsumers: Int,
      bufferSize: Int): Sink[T, Source[T, NotUsed]] =
    akka.stream.scaladsl.PartitionHub
      .sink[T]((size, elem) => partitioner.apply(size, elem), startAfterNrOfConsumers, bufferSize)
      .mapMaterializedValue(_.asJava)
      .asJava

  /**
   * Creates a [[Sink]] with default buffer size 256 that receives elements from its upstream producer and routes them to a dynamic set
   * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
   * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
   * elements from the original [[Sink]].
   *
   * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
   * [[Source]] for consuming the [[Sink]] of that materialization.
   *
   * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
   * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
   * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
   * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
   * cancelled are simply removed from the dynamic set of consumers.
   *
   * This `sink` should be used when the routing function is stateless, e.g. based on a hashed value of the
   * elements. Otherwise the [[#ofStateful]] can be used to implement more advanced routing logic.
   *
   * @param partitioner Function that decides where to route an element. The function takes two parameters;
   *   the first is the number of active consumers and the second is the stream element. The function should
   *   return the index of the selected consumer for the given element, i.e. int greater than or equal to 0
   *   and less than number of consumers. E.g. `(size, elem) -> Math.abs(elem.hashCode() % size)`. It's also
   *   possible to use `-1` to drop the element.
   * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
   *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
   *   been removed (canceled).
   * @param bufferSize Total number of elements that can be buffered. If this buffer is full, the producer
   *   is backpressured.
   */
  def of[T](
      clazz: Class[T],
      partitioner: BiFunction[Integer, T, Integer],
      startAfterNrOfConsumers: Int): Sink[T, Source[T, NotUsed]] =
    of(clazz, partitioner, startAfterNrOfConsumers, akka.stream.scaladsl.PartitionHub.defaultBufferSize)

  @DoNotInherit trait ConsumerInfo {

    /**
     * Sequence of all identifiers of current consumers.
     *
     * Use this method only if you need to enumerate consumer existing ids.
     * When selecting a specific consumerId by its index, prefer using the dedicated [[#consumerIdByIdx]] method instead,
     * which is optimised for this use case.
     */
    def getConsumerIds: java.util.List[Long]

    /** Obtain consumer identifier by index */
    def consumerIdByIdx(idx: Int): Long

    /**
     * Approximate number of buffered elements for a consumer.
     * Larger value than other consumers could be an indication of
     * that the consumer is slow.
     *
     * Note that this is a moving target since the elements are
     * consumed concurrently.
     */
    def queueSize(consumerId: Long): Int

    /**
     * Number of attached consumers.
     */
    def size: Int
  }
}
