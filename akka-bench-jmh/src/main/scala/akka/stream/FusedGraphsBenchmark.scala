/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.OperationsPerInvocation

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.stage._

object FusedGraphsBenchmark {
  val ElementCount = 100 * 1000

  @volatile var blackhole: org.openjdk.jmh.infra.Blackhole = _
}

// Just to avoid allocations and still have a way to do some work in stages. The value itself does not matter
// so no issues with sharing (the result does not make any sense, but hey)
class MutableElement(var value: Int)

class TestSource(elems: Array[MutableElement]) extends GraphStage[SourceShape[MutableElement]] {
  val out = Outlet[MutableElement]("TestSource.out")
  override val shape = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private[this] var left = FusedGraphsBenchmark.ElementCount - 1

      override def onPull(): Unit = {
        if (left >= 0) {
          push(out, elems(left))
          left -= 1
        } else completeStage()
      }

      setHandler(out, this)
    }
}

class JitSafeCompletionLatch extends GraphStageWithMaterializedValue[SinkShape[MutableElement], CountDownLatch] {
  val in = Inlet[MutableElement]("JitSafeCompletionLatch.in")
  override val shape = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, CountDownLatch) = {
    val latch = new CountDownLatch(1)
    val logic = new GraphStageLogic(shape) with InHandler {
      private[this] var sum = 0

      override def preStart(): Unit = pull(in)
      override def onPush(): Unit = {
        sum += grab(in).value
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        // Do not ignore work along the chain
        FusedGraphsBenchmark.blackhole.consume(sum)
        latch.countDown()
        completeStage()
      }

      setHandler(in, this)
    }

    (logic, latch)
  }
}

class IdentityStage extends GraphStage[FlowShape[MutableElement, MutableElement]] {
  val in = Inlet[MutableElement]("Identity.in")
  val out = Outlet[MutableElement]("Identity.out")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      override def onPush(): Unit = push(out, grab(in))
      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class FusedGraphsBenchmark {
  import FusedGraphsBenchmark._

  implicit val system: ActorSystem = ActorSystem(
    "test",
    ConfigFactory.parseString(s"""
      akka.stream.materializer.sync-processing-limit = ${Int.MaxValue}
    """))
  var testElements: Array[MutableElement] = _

  var singleIdentity: RunnableGraph[CountDownLatch] = _
  var chainOfIdentities: RunnableGraph[CountDownLatch] = _
  var singleMap: RunnableGraph[CountDownLatch] = _
  var chainOfMaps: RunnableGraph[CountDownLatch] = _
  var repeatTakeMapAndFold: RunnableGraph[CountDownLatch] = _
  var singleBuffer: RunnableGraph[CountDownLatch] = _
  var chainOfBuffers: RunnableGraph[CountDownLatch] = _
  var broadcastZip: RunnableGraph[CountDownLatch] = _
  var balanceMerge: RunnableGraph[CountDownLatch] = _
  var broadcastZipBalanceMerge: RunnableGraph[CountDownLatch] = _

  @Setup
  def setup(): Unit = {
    // eager init of materializer
    SystemMaterializer(system).materializer
    testElements = Array.fill(ElementCount)(new MutableElement(0))
    val addFunc = (x: MutableElement) => { x.value += 1; x }

    val testSource = Source.fromGraph(new TestSource(testElements))
    val testSink = Sink.fromGraph(new JitSafeCompletionLatch)

    def fuse(r: RunnableGraph[CountDownLatch]): RunnableGraph[CountDownLatch] = {
      RunnableGraph.fromGraph(r)
    }

    val identityStage = new IdentityStage

    singleIdentity = fuse(testSource.via(identityStage).toMat(testSink)(Keep.right))

    chainOfIdentities = fuse(
      testSource
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .via(identityStage)
        .toMat(testSink)(Keep.right))

    singleMap = fuse(testSource.map(addFunc).toMat(testSink)(Keep.right))

    chainOfMaps = fuse(
      testSource
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .map(addFunc)
        .toMat(testSink)(Keep.right))

    repeatTakeMapAndFold = fuse(
      Source
        .repeat(new MutableElement(0))
        .take(ElementCount)
        .map(addFunc)
        .map(addFunc)
        .fold(new MutableElement(0))((acc, x) => { acc.value += x.value; acc })
        .toMat(testSink)(Keep.right))

    singleBuffer = fuse(testSource.buffer(10, OverflowStrategy.backpressure).toMat(testSink)(Keep.right))

    chainOfBuffers = fuse(
      testSource
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .buffer(10, OverflowStrategy.backpressure)
        .toMat(testSink)(Keep.right))

    val broadcastZipFlow: Flow[MutableElement, MutableElement, NotUsed] = Flow.fromGraph(GraphDSL.create() {
      implicit b =>
        import GraphDSL.Implicits._

        val bcast = b.add(Broadcast[MutableElement](2))
        val zip = b.add(Zip[MutableElement, MutableElement]())

        bcast ~> zip.in0
        bcast ~> zip.in1

        FlowShape(bcast.in, zip.out.map(_._1).outlet)
    })

    val balanceMergeFlow: Flow[MutableElement, MutableElement, NotUsed] = Flow.fromGraph(GraphDSL.create() {
      implicit b =>
        import GraphDSL.Implicits._

        val balance = b.add(Balance[MutableElement](2))
        val merge = b.add(Merge[MutableElement](2))

        balance ~> merge
        balance ~> merge

        FlowShape(balance.in, merge.out)
    })

    broadcastZip = fuse(testSource.via(broadcastZipFlow).toMat(testSink)(Keep.right))

    balanceMerge = fuse(testSource.via(balanceMergeFlow).toMat(testSink)(Keep.right))

    broadcastZipBalanceMerge = fuse(testSource.via(broadcastZipFlow).via(balanceMergeFlow).toMat(testSink)(Keep.right))
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def single_identity(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    singleIdentity.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def chain_of_identities(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    chainOfIdentities.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def single_map(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    singleMap.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def chain_of_maps(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    chainOfMaps.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def repeat_take_map_and_fold(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    repeatTakeMapAndFold.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def single_buffer(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    singleBuffer.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def chain_of_buffers(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    chainOfBuffers.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def broadcast_zip(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    broadcastZip.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def balance_merge(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    balanceMerge.run().await()
  }

  @Benchmark
  @OperationsPerInvocation(100 * 1000)
  def broadcast_zip_balance_merge(blackhole: org.openjdk.jmh.infra.Blackhole): Unit = {
    FusedGraphsBenchmark.blackhole = blackhole
    broadcastZipBalanceMerge.run().await()
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

}
