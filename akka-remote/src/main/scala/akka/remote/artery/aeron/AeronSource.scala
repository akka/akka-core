/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package aeron

import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

import io.aeron.{ Aeron, FragmentAssembler, Subscription }
import io.aeron.exceptions.DriverTimeoutException
import io.aeron.logbuffer.FragmentHandler
import io.aeron.logbuffer.Header
import org.agrona.DirectBuffer
import org.agrona.hints.ThreadHints

import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.OutHandler
import akka.stream.stage.StageLogging

/**
 * INTERNAL API
 */
private[remote] object AeronSource {

  private def pollTask(
      sub: Subscription,
      handler: MessageHandler,
      onMessage: AsyncCallback[EnvelopeBuffer]): () => Boolean = { () =>
    {
      handler.reset()
      sub.poll(handler.fragmentsHandler, 1)
      val msg = handler.messageReceived
      handler.reset() // for GC
      if (msg ne null) {
        onMessage.invoke(msg)
        true
      } else
        false
    }
  }

  class MessageHandler(pool: EnvelopeBufferPool) {
    def reset(): Unit = messageReceived = null

    private[remote] var messageReceived
        : EnvelopeBuffer = null // private to avoid scalac warning about exposing EnvelopeBuffer

    val fragmentsHandler = new Fragments(data => messageReceived = data, pool)
  }

  class Fragments(onMessage: EnvelopeBuffer => Unit, pool: EnvelopeBufferPool)
      extends FragmentAssembler(new FragmentHandler {
        override def onFragment(aeronBuffer: DirectBuffer, offset: Int, length: Int, header: Header): Unit = {
          val envelope = pool.acquire()
          aeronBuffer.getBytes(offset, envelope.byteBuffer, length)
          envelope.byteBuffer.flip()
          onMessage(envelope)
        }
      })

  trait AeronLifecycle {
    def onUnavailableImage(sessionId: Int): Unit
    // See  [io.aeron.status.ChannelEndpointStatus]
    def channelEndpointStatus(): Future[Long]
  }
}

/**
 * INTERNAL API
 * @param channel eg. "aeron:udp?endpoint=localhost:40123"
 * @param spinning the amount of busy spinning to be done synchronously before deferring to the TaskRunner
 *                 when waiting for data
 */
private[remote] class AeronSource(
    channel: String,
    streamId: Int,
    aeron: Aeron,
    taskRunner: TaskRunner,
    pool: EnvelopeBufferPool,
    spinning: Int)
    extends GraphStageWithMaterializedValue[SourceShape[EnvelopeBuffer], AeronSource.AeronLifecycle] {

  import AeronSource._
  import TaskRunner._

  val out: Outlet[EnvelopeBuffer] = Outlet("AeronSource")
  override val shape: SourceShape[EnvelopeBuffer] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new GraphStageLogic(shape) with OutHandler with AeronLifecycle with StageLogging {

      private val subscription = aeron.addSubscription(channel, streamId)
      private var backoffCount = spinning
      private var delegateTaskStartTime = 0L
      private var countBeforeDelegate = 0L

      // the fragmentHandler is called from `poll` in same thread, i.e. no async callback is needed
      private val messageHandler = new MessageHandler(pool)
      private val addPollTask: Add = Add(pollTask(subscription, messageHandler, getAsyncCallback(taskOnMessage)))

      private var delegatingToTaskRunner = false

      private var pendingUnavailableImages: List[Int] = Nil
      private val onUnavailableImageCb = getAsyncCallback[Int] { sessionId =>
        pendingUnavailableImages = sessionId :: pendingUnavailableImages
        freeSessionBuffers()
      }
      private val getStatusCb = getAsyncCallback[Promise[Long]] { promise =>
        promise.success(subscription.channelStatus())
      }

      override protected def logSource = classOf[AeronSource]

      override def preStart(): Unit = {
        RemotingFlightRecorder.aeronSourceStarted(channel, streamId)
      }

      override def postStop(): Unit = {
        taskRunner.command(Remove(addPollTask.task))
        try subscription.close()
        catch {
          case e: DriverTimeoutException =>
            // media driver was shutdown
            log.debug("DriverTimeout when closing subscription. {}", e)
        } finally RemotingFlightRecorder.aeronSourceStopped(channel, streamId)
      }

      // OutHandler
      override def onPull(): Unit = {
        backoffCount = spinning
        subscriberLoop()
      }

      @tailrec private def subscriberLoop(): Unit = {
        messageHandler.reset()
        val fragmentsRead = subscription.poll(messageHandler.fragmentsHandler, 1)
        val msg = messageHandler.messageReceived
        messageHandler.reset() // for GC
        if (fragmentsRead > 0) {
          countBeforeDelegate += 1
          if (msg ne null)
            onMessage(msg)
          else
            subscriberLoop() // recursive, read more fragments
        } else {
          backoffCount -= 1
          if (backoffCount > 0) {
            ThreadHints.onSpinWait()
            subscriberLoop() // recursive
          } else {
            // delegate backoff to shared TaskRunner
            RemotingFlightRecorder.aeronSourceDelegateToTaskRunner(countBeforeDelegate)
            delegatingToTaskRunner = true
            delegateTaskStartTime = System.nanoTime()
            taskRunner.command(addPollTask)
          }
        }
      }

      override def channelEndpointStatus(): Future[Long] = {
        val promise = Promise[Long]()
        getStatusCb.invoke(promise)
        promise.future
      }

      private def taskOnMessage(data: EnvelopeBuffer): Unit = {
        countBeforeDelegate = 0
        delegatingToTaskRunner = false
        // FIXME push calculation to JFR?
        RemotingFlightRecorder.aeronSourceReturnFromTaskRunner(System.nanoTime() - delegateTaskStartTime)
        freeSessionBuffers()
        onMessage(data)
      }

      private def onMessage(data: EnvelopeBuffer): Unit = {
        RemotingFlightRecorder.aeronSourceReceived(data.byteBuffer.limit())
        push(out, data)
      }

      private def freeSessionBuffers(): Unit =
        if (!delegatingToTaskRunner) {
          def loop(remaining: List[Int]): Unit = {
            remaining match {
              case Nil =>
              case sessionId :: tail =>
                messageHandler.fragmentsHandler.freeSessionBuffer(sessionId)
                loop(tail)
            }
          }

          loop(pendingUnavailableImages)
          pendingUnavailableImages = Nil
        }

      // External callback from ResourceLifecycle
      def onUnavailableImage(sessionId: Int): Unit =
        try {
          onUnavailableImageCb.invoke(sessionId)
        } catch {
          case NonFatal(_) => // just in case it's called before stage is initialized, ignore
        }

      setHandler(out, this)
    }

    (logic, logic)
  }
}
