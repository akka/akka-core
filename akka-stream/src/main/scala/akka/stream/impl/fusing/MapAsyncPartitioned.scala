/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.{ Buffer => BufferImpl }
import akka.stream.impl.BoundedBuffer
import akka.stream.impl.ChainedBuffer
import akka.stream.impl.PartitionedBuffer
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.stage._

/**
 * Internal API
 */
@InternalApi
private[akka] object MapAsyncPartitioned {
  final class Holder[P, I, O](
      var incoming: I,
      var outgoing: Try[O],
      val partition: P,
      val cb: AsyncCallback[Holder[P, I, O]])
      extends (Try[O] => Unit) {

    def clearIncoming(): Unit = {
      incoming = null.asInstanceOf[I]
    }

    def setOutgoing(t: Try[O]): Unit = {
      outgoing = t
    }

    override def apply(t: Try[O]): Unit = {
      setOutgoing(t)
      cb.invoke(this)
    }

    override def toString: String = s"Holder($incoming, $outgoing, $partition)"
  }

  val NotYetThere = MapAsync.NotYetThere
}

/**
 * Internal API
 */
@InternalApi
private[akka] final case class MapAsyncPartitioned[In, Out, Partition](
    parallelism: Int,
    perPartition: Int,
    partitioner: In => Partition,
    f: (In, Partition) => Future[Out])
    extends GraphStage[FlowShape[In, Out]] {
  import MapAsyncPartitioned._

  if (parallelism < 1) throw new IllegalArgumentException("parallelism must be at least 1")
  if (perPartition < 1) throw new IllegalArgumentException("perPartition must be at least 1")
  if (perPartition >= parallelism)
    throw new IllegalArgumentException("perPartition must be less than parallelism: consider mapAsync instead")

  private val in = Inlet[In]("MapAsyncPartitioned.in")
  private val out = Outlet[Out]("MapAsyncPartitioned.out")

  override def initialAttributes = DefaultAttributes.mapAsync and SourceLocation.forLambda(f)
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      val buffer: PartitionedBuffer[Partition, Holder[Partition, In, Out]] =
        new PartitionedBuffer[Partition, Holder[Partition, In, Out]](size = parallelism)

      private val futureCB = getAsyncCallback[Holder[Partition, In, Out]] { holder =>
        holder.outgoing match {
          case Failure(ex) => failStage(ex)
          case _ =>
            dropCompletedThenPushIfPossible(holder.partition)
        }
      }

      override def onPull(): Unit = pushNextIfPossible()

      override def onPush(): Unit = {
        val elem = grab(in)
        val partition = partitioner(elem)
        val holder =
          new Holder[Partition, In, Out](incoming = elem, outgoing = NotYetThere, partition = partition, cb = futureCB)

        if (!buffer.containsPartition(partition)) {
          if (buffer.capacity == 0) {
            // should never happen because then we don't pull if we are out of capacity (parallelism)
            throw new IllegalStateException(s"Saw new partition [$partition] but no buffer space left")
          } else {
            // Note: each partition gets a max parallelism, same as number of partitions
            // as all elements could be for one partition
            val partitionBuffer = createNewPartitionBuffer(partition)
            buffer.addPartition(partition, partitionBuffer)
          }
        }
        buffer.enqueue(partition, holder)

        pullIfNeeded()
      }

      override def onUpstreamFinish(): Unit = if (buffer.isEmpty) completeStage()

      @tailrec
      private def dropCompletedThenPushIfPossible(partition: Partition): Unit = {
        val holderOpt = buffer.peekPartition(partition)
        holderOpt match {
          case None => pushNextIfPossible()
          case Some(holder) =>
            holder.outgoing match {
              case NotYetThere => pushNextIfPossible()

              case Success(_) =>
                // dropped from per-partition queue, to be able to execute next, but result is kept in the
                // main linear buffer for when out is ready
                buffer.dropOnlyPartitionHead(partition)

                if (perPartition > 1 && buffer.usedInPartition(partition) > perPartition) {
                  // If the next future in this partition has completed, start the next waiting future
                  dropCompletedThenPushIfPossible(partition)
                } else {
                  pushNextIfPossible()
                }

              case Failure(ex) =>
                throw ex // Could happen if this finds the failed future before the async callback runs
            }
        }
      }

      @tailrec
      private def pushNextIfPossible(): Unit =
        if (buffer.isEmpty) pullIfNeeded()
        else if (buffer.peek().outgoing eq NotYetThere) pullIfNeeded()
        else if (isAvailable(out)) {
          // We peek instead of dequeue so that we can push out an element before
          //  removing from the queue (since a dequeue or dropHead can cause re-entry)
          val holder = buffer.peek()
          val elem = holder.outgoing.get // throw if failed
          if (elem != null) {
            push(out, elem)
            buffer.dropHead()
            pullIfNeeded()
          } else {
            // elem is null
            buffer.dropHead()
            pullIfNeeded()
            pushNextIfPossible()
          }
        }

      private def pullIfNeeded(): Unit = {
        if (isClosed(in) && buffer.isEmpty) completeStage()
        else if (buffer.used < parallelism && !hasBeenPulled(in)) tryPull(in)
        // else already pulled and waiting for next element
      }

      private def createNewPartitionBuffer(partition: Partition): ChainedBuffer[Holder[Partition, In, Out]] = {
        val tailCapacity = (parallelism - perPartition).max(1)
        val headBuffer = BufferImpl[Holder[Partition, In, Out]](perPartition, inheritedAttributes)
        val tailBuffer = new BoundedBuffer.DynamicQueue[Holder[Partition, In, Out]](tailCapacity)

        new ChainedBuffer[Holder[Partition, In, Out]](
          headBuffer,
          tailBuffer,
          onEnqueueToHead = executeFutureTask(_, partition))
      }

      private def executeFutureTask(holder: Holder[Partition, In, Out], partition: Partition): Unit = {
        // This will execute, on the stream thread, when the holder moves from the tail buffer to the head buffer
        // (viz. ready to be scheduled). This might be on enqueueing into the partitioned buffer, or it might be when
        // the head of this partition is dropped after being completed.

        val future = f(holder.incoming, partition)
        // slight optimization, clear the in-element reference once we have used it so it can be gc:d
        holder.clearIncoming()

        future.value match {
          case None              => future.onComplete(holder)(ExecutionContext.parasitic)
          case Some(Failure(ex)) => throw ex
          case Some(v)           =>
            // future already completed, so don't schedule on dispatcher
            holder.setOutgoing(v)
            dropCompletedThenPushIfPossible(partition)
        }
      }

      setHandlers(in, out, this)
    }
}
