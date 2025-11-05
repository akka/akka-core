/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import java.io.{ IOException, InputStream }
import java.util.concurrent.{ BlockingQueue, LinkedBlockingDeque, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi
import akka.stream.{ AbruptStageTerminationException, Attributes, Inlet, SinkShape }
import akka.stream.Attributes.InputBuffer
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.io.InputStreamSinkStage._
import akka.stream.stage._
import akka.util.ByteString

private[stream] object InputStreamSinkStage {

  sealed trait AdapterToStageMessage
  case object ReadElementAcknowledgement extends AdapterToStageMessage
  case object Close extends AdapterToStageMessage

  sealed trait StreamToAdapterMessage
  // Only non-empty ByteString is expected as Data
  case class Data(data: ByteString) extends StreamToAdapterMessage
  case object Finished extends StreamToAdapterMessage
  case object Initialized extends StreamToAdapterMessage
  case class Failed(cause: Throwable) extends StreamToAdapterMessage

  sealed trait StageWithCallback {
    def wakeUp(msg: AdapterToStageMessage): Unit
  }
}

/**
 * INTERNAL API
 */
@InternalApi final private[stream] class InputStreamSinkStage(readTimeout: FiniteDuration)
    extends GraphStageWithMaterializedValue[SinkShape[ByteString], InputStream] {

  val in = Inlet[ByteString]("InputStreamSink.in")
  override def initialAttributes: Attributes = DefaultAttributes.inputStreamSink
  override val shape: SinkShape[ByteString] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, InputStream) = {
    val maxBuffer = inheritedAttributes.get[InputBuffer](InputBuffer(16, 16)).max
    require(maxBuffer > 0, "Buffer size must be greater than 0")

    val dataQueue = new LinkedBlockingDeque[StreamToAdapterMessage](maxBuffer + 2)

    val logic = new GraphStageLogic(shape) with StageWithCallback with InHandler {

      var completionSignalled = false

      private val callback: AsyncCallback[AdapterToStageMessage] =
        getAsyncCallback {
          case ReadElementAcknowledgement => sendPullIfAllowed()
          case Close                      => completeStage()
        }

      override def wakeUp(msg: AdapterToStageMessage): Unit = callback.invoke(msg)

      private def sendPullIfAllowed(): Unit =
        if (dataQueue.remainingCapacity() > 1 && !hasBeenPulled(in))
          pull(in)

      override def preStart() = {
        dataQueue.add(Initialized)
        pull(in)
      }

      def onPush(): Unit = {
        //1 is buffer for Finished or Failed callback
        require(dataQueue.remainingCapacity() > 1)
        val bs = grab(in)
        if (bs.nonEmpty) {
          dataQueue.add(Data(bs))
        }
        if (dataQueue.remainingCapacity() > 1) sendPullIfAllowed()
      }

      override def onUpstreamFinish(): Unit = {
        dataQueue.add(Finished)
        completionSignalled = true
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        dataQueue.add(Failed(ex))
        completionSignalled = true
        failStage(ex)
      }

      override def postStop(): Unit = {
        if (!completionSignalled) dataQueue.add(Failed(new AbruptStageTerminationException(this)))
      }

      setHandler(in, this)

    }

    (logic, new InputStreamAdapter(dataQueue, logic.wakeUp, readTimeout))
  }
}

/**
 * INTERNAL API
 * InputStreamAdapter that interacts with InputStreamSinkStage
 */
@InternalApi private[akka] class InputStreamAdapter(
    sharedBuffer: BlockingQueue[StreamToAdapterMessage],
    sendToStage: (AdapterToStageMessage) => Unit,
    readTimeout: FiniteDuration)
    extends InputStream {

  var isInitialized = false
  val isActive = new AtomicBoolean(true)
  var isStageAlive = true
  def subscriberClosedException = new IOException("Reactive stream is terminated, no reads are possible")
  var detachedChunk: Option[ByteString] = None

  @scala.throws(classOf[IOException])
  private[this] def executeIfNotClosed[T](f: () => T): T =
    if (isActive.get()) {
      waitIfNotInitialized()
      f()
    } else throw subscriberClosedException

  @scala.throws(classOf[IOException])
  override def read(): Int = {
    val a = new Array[Byte](1)
    read(a, 0, 1) match {
      case 1   => a(0) & 0xff
      case -1  => -1
      case len => throw new IllegalStateException(s"Invalid length [$len]")
    }
  }

  @scala.throws(classOf[IOException])
  override def read(a: Array[Byte]): Int = read(a, 0, a.length)

  @scala.throws(classOf[IOException])
  override def read(a: Array[Byte], begin: Int, length: Int): Int = {
    require(a.length > 0, "array size must be >= 0")
    require(begin >= 0, "begin must be >= 0")
    require(length >= 0, "length must be >= 0")
    require(begin + length <= a.length, "begin + length must be smaller or equal to the array length")

    if (length == 0) 0
    else
      executeIfNotClosed(() =>
        if (isStageAlive) {
          detachedChunk match {
            case None =>
              try {
                sharedBuffer.poll(readTimeout.toMillis, TimeUnit.MILLISECONDS) match {
                  case Data(data) =>
                    detachedChunk = Some(data)
                    readBytes(a, begin, length)
                  case Finished =>
                    isStageAlive = false
                    -1
                  case Failed(ex) =>
                    isStageAlive = false
                    throw new IOException(ex)
                  case null        => throw new IOException("Timeout on waiting for new data")
                  case Initialized => throw new IllegalStateException("message 'Initialized' must come first")
                }
              } catch {
                case ex: InterruptedException => throw new IOException(ex)
              }
            case Some(_) =>
              readBytes(a, begin, length)
          }
        } else -1)
  }

  private[this] def readBytes(a: Array[Byte], begin: Int, length: Int): Int = {
    require(detachedChunk.nonEmpty, "Chunk must be pulled from shared buffer")
    val availableInChunk = detachedChunk.get.size
    val readBytes = getData(a, begin, length, 0)
    if (readBytes >= availableInChunk) sendToStage(ReadElementAcknowledgement)
    readBytes
  }

  @scala.throws(classOf[IOException])
  override def close(): Unit = {
    if (isActive.getAndSet(false)) {
      // at this point Subscriber may be already terminated
      if (isStageAlive) sendToStage(Close)
    }
  }

  @tailrec
  private[this] def getData(arr: Array[Byte], begin: Int, length: Int, gotBytes: Int): Int = {
    grabDataChunk() match {
      case Some(data) =>
        val size = data.size
        if (size <= length) {
          data.copyToArray(arr, begin, size)
          detachedChunk = None
          if (size == length)
            gotBytes + size
          else
            getData(arr, begin + size, length - size, gotBytes + size)
        } else {
          data.copyToArray(arr, begin, length)
          detachedChunk = Some(data.drop(length))
          gotBytes + length
        }
      case None => gotBytes
    }
  }

  private[this] def waitIfNotInitialized(): Unit = {
    if (!isInitialized) {
      sharedBuffer.poll(readTimeout.toMillis, TimeUnit.MILLISECONDS) match {
        case Initialized => isInitialized = true
        case null        => throw new IOException(s"Timeout after $readTimeout waiting for Initialized message from stage")
        case entry       => require(false, s"First message must be Initialized notification, got $entry")
      }
    }
  }

  private[this] def grabDataChunk(): Option[ByteString] = {
    detachedChunk match {
      case None =>
        sharedBuffer.poll() match {
          case Data(data) =>
            detachedChunk = Some(data)
            detachedChunk
          case Finished =>
            isStageAlive = false
            None
          case Failed(e) => throw new IOException(e)
          case _         => None
        }
      case Some(_) => detachedChunk
    }
  }
}
