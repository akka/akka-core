/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.nio.ByteBuffer
import java.util.Random
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.io.DirectByteBufferPool

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class DirectByteBufferPoolBenchmark {

  private val MAX_LIVE_BUFFERS = 8192

  @Param(Array("00000", "00256", "01024", "04096", "16384", "65536"))
  var size = 0

  val random = new Random

  private[akka] var arteryPool: DirectByteBufferPool = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    arteryPool = new DirectByteBufferPool(size, MAX_LIVE_BUFFERS)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    var i = 0
    while (i < MAX_LIVE_BUFFERS) {
      arteryPool.release(pooledDirectBuffers(i))
      pooledDirectBuffers(i) = null

      DirectByteBufferPool.tryCleanDirectByteBuffer(unpooledDirectBuffers(i))
      unpooledDirectBuffers(i) = null

      DirectByteBufferPool.tryCleanDirectByteBuffer(unpooledHeapBuffers(i))
      unpooledHeapBuffers(i) = null

      i += 1
    }
  }

  private val unpooledHeapBuffers = new Array[ByteBuffer](MAX_LIVE_BUFFERS)

  private val pooledDirectBuffers = new Array[ByteBuffer](MAX_LIVE_BUFFERS)
  private val unpooledDirectBuffers = new Array[ByteBuffer](MAX_LIVE_BUFFERS)

  import org.openjdk.jmh.annotations.Benchmark

  @Benchmark
  def unpooledHeapAllocAndRelease(): Unit = {
    val idx = random.nextInt(unpooledHeapBuffers.length)
    val oldBuf = unpooledHeapBuffers(idx)
    if (oldBuf != null) DirectByteBufferPool.tryCleanDirectByteBuffer(oldBuf)
    unpooledHeapBuffers(idx) = ByteBuffer.allocateDirect(size)
  }

  @Benchmark
  def unpooledDirectAllocAndRelease(): Unit = {
    val idx = random.nextInt(unpooledDirectBuffers.length)
    val oldBuf = unpooledDirectBuffers(idx)
    if (oldBuf != null) DirectByteBufferPool.tryCleanDirectByteBuffer(oldBuf)
    unpooledDirectBuffers(idx) = ByteBuffer.allocateDirect(size)
  }

  @Benchmark
  def pooledDirectAllocAndRelease(): Unit = {
    val idx = random.nextInt(pooledDirectBuffers.length)
    val oldBuf = pooledDirectBuffers(idx)
    if (oldBuf != null) arteryPool.release(oldBuf)
    pooledDirectBuffers(idx) = arteryPool.acquire()
  }

}

object DirectByteBufferPoolBenchmark {
  final val numMessages = 2000000 // messages per actor pair

  // Constants because they are used in annotations
  // update according to cpu
  final val cores = 8
  final val coresStr = "8"
  final val cores2xStr = "16"
  final val cores4xStr = "24"

  final val twoActors = 2
  final val moreThanCoresActors = cores * 2
  final val lessThanCoresActors = cores / 2
  final val sameAsCoresActors = cores

  final val totalMessagesTwoActors = numMessages
  final val totalMessagesMoreThanCores = (moreThanCoresActors * numMessages) / 2
  final val totalMessagesLessThanCores = (lessThanCoresActors * numMessages) / 2
  final val totalMessagesSameAsCores = (sameAsCoresActors * numMessages) / 2
}
