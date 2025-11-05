/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import org.openjdk.jmh.annotations._

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._

object MaterializationBenchmark {

  val flowWithMapBuilder = (numOfOperators: Int) => {
    var source = Source.single(())
    for (_ <- 1 to numOfOperators) {
      source = source.map(identity)
    }
    source.to(Sink.ignore)
  }

  val graphWithJunctionsGradualBuilder = (numOfJunctions: Int) =>
    RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val broadcast = b.add(Broadcast[Unit](numOfJunctions))
      var outlet = broadcast.out(0)
      for (i <- 1 until numOfJunctions) {
        val merge = b.add(Merge[Unit](2))
        outlet ~> merge
        broadcast.out(i) ~> merge
        outlet = merge.out
      }

      Source.single(()) ~> broadcast
      outlet ~> Sink.ignore
      ClosedShape
    })

  val graphWithJunctionsImmediateBuilder = (numOfJunctions: Int) =>
    RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val broadcast = b.add(Broadcast[Unit](numOfJunctions))
      val merge = b.add(Merge[Unit](numOfJunctions))
      for (_ <- 0 until numOfJunctions) {
        broadcast ~> merge
      }

      Source.single(()) ~> broadcast
      merge ~> Sink.ignore
      ClosedShape
    })

  val graphWithImportedFlowBuilder = (numOfFlows: Int) =>
    RunnableGraph.fromGraph(GraphDSL.createGraph(Source.single(())) { implicit b => source =>
      import GraphDSL.Implicits._
      val flow = Flow[Unit].map(identity)
      var out: Outlet[Unit] = source.out
      for (_ <- 0 until numOfFlows) {
        val flowShape = b.add(flow)
        out ~> flowShape
        out = flowShape.outlet
      }
      out ~> Sink.ignore
      ClosedShape
    })

  final val subStreamCount = 10000

  val subStreamBuilder: Int => RunnableGraph[Future[Unit]] = numOfOperators => {

    val subFlow = {
      var flow = Flow[Unit]
      for (_ <- 1 to numOfOperators) {
        flow = flow.map(identity)
      }
      flow
    }

    Source.repeat(Source.single(())).take(subStreamCount).flatMapConcat(_.via(subFlow)).toMat(Sink.last)(Keep.right)
  }
}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class MaterializationBenchmark {

  import MaterializationBenchmark._

  implicit val system: ActorSystem = ActorSystem("MaterializationBenchmark")

  var flowWithMap: RunnableGraph[NotUsed] = _
  var graphWithJunctionsGradual: RunnableGraph[NotUsed] = _
  var graphWithJunctionsImmediate: RunnableGraph[NotUsed] = _
  var graphWithImportedFlow: RunnableGraph[NotUsed] = _
  var subStream: RunnableGraph[Future[Unit]] = _

  @Param(Array("1", "10", "100"))
  var complexity = 0

  @Setup
  def setup(): Unit = {
    flowWithMap = flowWithMapBuilder(complexity)
    graphWithJunctionsGradual = graphWithJunctionsGradualBuilder(complexity)
    graphWithJunctionsImmediate = graphWithJunctionsImmediateBuilder(complexity)
    graphWithImportedFlow = graphWithImportedFlowBuilder(complexity)
    subStream = subStreamBuilder(complexity)
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  def flow_with_map(): NotUsed = flowWithMap.run()

  @Benchmark
  def graph_with_junctions_gradual(): NotUsed = graphWithJunctionsGradual.run()

  @Benchmark
  def graph_with_junctions_immediate(): NotUsed = graphWithJunctionsImmediate.run()

  @Benchmark
  def graph_with_imported_flow(): NotUsed = graphWithImportedFlow.run()

  @Benchmark
  @OperationsPerInvocation(subStreamCount)
  def sub_stream(): Done = {
    Await.result(subStream.run(), 5.seconds)
    Done
  }
}
