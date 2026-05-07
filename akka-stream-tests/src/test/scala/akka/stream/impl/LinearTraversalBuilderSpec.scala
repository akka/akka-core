/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.NotUsed
import akka.stream._
import akka.stream.impl.TraversalTestUtils._
import akka.stream.scaladsl.Keep
import akka.testkit.AkkaSpec

class LinearTraversalBuilderSpec extends AkkaSpec {

  "LinearTraversalBuilder" must {
    val source = new LinearTestSource
    val sink = new LinearTestSink
    val flow1 = new LinearTestFlow("1")
    val flow2 = new LinearTestFlow("2")

    val compositeSource = new CompositeTestSource
    val compositeSink = new CompositeTestSink
    val compositeFlow1 = new CompositeTestFlow("C1")
    val compositeFlow2 = new CompositeTestFlow("C2")

    // ADD closed shape, (and composite closed shape)

    "work with a single Source and Sink" in {
      val builder =
        source.traversalBuilder.append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)

      mat.outlets(0) should ===(source.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with two Flows" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.left)
        .append(flow2.traversalBuilder, flow2.shape, Keep.left)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow2.in)
      mat.outlets(0) should ===(flow2.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with two Flows wired in opposite order" in {
      val s1 = flow2.traversalBuilder.append(sink.traversalBuilder, sink.shape, Keep.left)
      val s2 = flow1.traversalBuilder.append(s1, SinkShape(flow2.in), Keep.left)
      val builder = source.traversalBuilder.append(s2, SinkShape(flow1.in), Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow2.in)
      mat.outlets(0) should ===(flow2.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with two Flows wired in an irregular order" in {
      val source2 = source.traversalBuilder.append(flow1.traversalBuilder, flow1.shape, Keep.left)
      val sink2 = flow2.traversalBuilder.append(sink.traversalBuilder, sink.shape, Keep.left)

      val builder = source2.append(sink2, SinkShape(flow2.in), Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow2.in)
      mat.outlets(0) should ===(flow2.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with a Flow appended to its imported self" in {
      val remappedShape = FlowShape(Inlet[Any]("Remapped.in"), Outlet[Any]("Remapped.out"))
      remappedShape.in.mappedTo = flow1.in
      remappedShape.out.mappedTo = flow1.out

      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.left)
        .append(flow1.traversalBuilder, flow1.shape, Keep.left)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow1.in)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with a nested Flow chain" in {
      val nestedFlows =
        flow1.traversalBuilder.append(flow2.traversalBuilder, flow2.shape, Keep.left)

      val builder = source.traversalBuilder
        .append(nestedFlows, FlowShape(flow1.in, flow2.out), Keep.left)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow2.in)
      mat.outlets(0) should ===(flow2.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with a nested Flow chain used twice (appended to self)" in {
      val nestedFlows =
        flow1.traversalBuilder.append(flow2.traversalBuilder, flow2.shape, Keep.left)

      val builder = source.traversalBuilder
        .append(nestedFlows, FlowShape(flow1.in, flow2.out), Keep.left)
        .append(nestedFlows, FlowShape(flow1.in, flow2.out), Keep.left)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(5)
      mat.outlets(4) should ===(source.out)
      mat.inlets(4) should ===(flow1.in)
      mat.outlets(3) should ===(flow1.out)
      mat.inlets(3) should ===(flow2.in)
      mat.outlets(2) should ===(flow2.out)
      mat.inlets(2) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow2.in)
      mat.outlets(0) should ===(flow2.out)
      mat.inlets(0) should ===(sink.in)
    }

    "work with a Flow wired to self" in {
      val builder = flow1.traversalBuilder.wire(flow1.out, flow1.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(flow1.in)
    }

    "work with a two Flows wired back to self" in {
      val builder =
        flow1.traversalBuilder.append(flow2.traversalBuilder, flow2.shape, Keep.left).wire(flow2.out, flow1.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(2)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(flow2.in)
      mat.outlets(1) should ===(flow2.out)
      mat.inlets(1) should ===(flow1.in)
    }

    "work with Flow appended to self then wired back to self" in {
      val builder =
        flow1.traversalBuilder.append(flow1.traversalBuilder, flow1.shape, Keep.left).wire(flow1.out, flow1.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(2)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(flow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow1.in)
    }

    "be able to be used with a composite source" in {
      val builder =
        compositeSource.traversalBuilder
          .add(sink.traversalBuilder, sink.shape, Keep.left)
          .wire(compositeSource.out, sink.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(compositeSource.out)
      mat.inlets(0) should ===(sink.in)
    }

    "be able to be used with a composite sink" in {
      val builder =
        compositeSink.traversalBuilder
          .add(source.traversalBuilder, source.shape, Keep.left)
          .wire(source.out, compositeSink.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(source.out)
      mat.inlets(0) should ===(compositeSink.in)
    }

    "be able to be joined with a composite flow" in {
      val embeddedFlow =
        flow1.traversalBuilder.append(flow2.traversalBuilder, flow2.shape, Keep.left)

      val builder =
        compositeFlow1.traversalBuilder
          .add(embeddedFlow, FlowShape(flow1.in, flow2.out), Keep.left)
          .wire(compositeFlow1.out, flow1.in)
          .wire(flow2.out, compositeFlow1.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(0) should ===(flow2.out)
      mat.inlets(0) should ===(compositeFlow1.in)
      mat.outlets(1) should ===(flow1.out)
      mat.inlets(1) should ===(flow2.in)
      mat.outlets(2) should ===(compositeFlow1.out)
      mat.inlets(2) should ===(flow1.in)
    }

    "be able to use a linear flow with composite source and sink" in {
      val builder =
        compositeSource.traversalBuilder
          .add(compositeSink.traversalBuilder, compositeSink.shape, Keep.left)
          .add(flow1.traversalBuilder, flow1.shape, Keep.left)
          .wire(compositeSource.out, flow1.in)
          .wire(flow1.out, compositeSink.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(2)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(compositeSink.in)
      mat.outlets(1) should ===(compositeSource.out)
      mat.inlets(1) should ===(flow1.in)
    }

    "be able to add a flow to an empty composite and join to itself" in {
      val builder =
        TraversalBuilder.empty().add(flow1.traversalBuilder, flow1.shape, Keep.left).wire(flow1.out, flow1.in)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(flow1.in)
    }

    "be able to embed a composite sink in a linear traversal" in {
      val builder =
        source.traversalBuilder.append(compositeSink.traversalBuilder, compositeSink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(source.out)
      mat.inlets(0) should ===(compositeSink.in)
    }

    "be able to start from a composite source" in pending

    "be able to start from a composite flow" in pending

    "be able to embed a composite flow in a linear traversal" in {
      val builder =
        source.traversalBuilder
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(2)
      mat.outlets(1) should ===(source.out)
      mat.inlets(1) should ===(compositeFlow1.in)
      mat.outlets(0) should ===(compositeFlow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "be able to embed a composite sink with an irregular wiring" in {
      val sinkBuilder =
        TraversalBuilder
          .empty()
          .add(compositeFlow2.traversalBuilder, compositeFlow2.shape, Keep.left)
          .add(compositeSink.traversalBuilder, compositeSink.shape, Keep.left)
          .add(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .wire(compositeFlow2.out, compositeSink.in)
          .wire(compositeFlow1.out, compositeFlow2.in)

      val builder =
        source.traversalBuilder.append(sinkBuilder, SinkShape(compositeFlow1.in), Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(compositeFlow1.in)
      mat.outlets(1) should ===(compositeFlow2.out)
      mat.inlets(1) should ===(compositeSink.in)
      mat.outlets(0) should ===(compositeFlow1.out)
      mat.inlets(0) should ===(compositeFlow2.in)
    }

    "be able to embed a composite flow multiple times appended to self" in {
      val builder =
        source.traversalBuilder
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(3)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(compositeFlow1.in)
      mat.outlets(1) should ===(compositeFlow1.out)
      mat.inlets(1) should ===(compositeFlow1.in)
      mat.outlets(0) should ===(compositeFlow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "be able to embed a composite flow multiple times appended to self alternating with linear flow" in {

      val builder =
        source.traversalBuilder
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .append(flow1.traversalBuilder, flow1.shape, Keep.left)
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .append(flow1.traversalBuilder, flow1.shape, Keep.left)
          .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(5)

      mat.outlets(4) should ===(source.out)
      mat.inlets(4) should ===(compositeFlow1.in)
      mat.outlets(3) should ===(compositeFlow1.out)
      mat.inlets(3) should ===(flow1.in)
      mat.outlets(2) should ===(flow1.out)
      mat.inlets(2) should ===(compositeFlow1.in)
      mat.outlets(1) should ===(compositeFlow1.out)
      mat.inlets(1) should ===(flow1.in)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "be able to embed a composite flow wrapped in linear, if the input port is of the second flow" in {
      val compositeBuilder =
        compositeFlow2.traversalBuilder
          .add(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .add(flow1.traversalBuilder, flow1.shape, Keep.left)
          .wire(compositeFlow1.out, compositeFlow2.in)
          .wire(compositeFlow2.out, flow1.in)

      val shape = FlowShape(compositeFlow1.in, flow1.out)

      val embeddedBuilder =
        LinearTraversalBuilder.empty().append(compositeBuilder, shape, Keep.left)

      val builder =
        source.traversalBuilder
          .append(embeddedBuilder, shape, Keep.left)
          .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.connections should ===(4)

      mat.outlets(3) should ===(compositeFlow2.out)
      mat.inlets(3) should ===(flow1.in)
      mat.outlets(2) should ===(source.out)
      mat.inlets(2) should ===(compositeFlow1.in)
      mat.outlets(1) should ===(compositeFlow1.out)
      mat.inlets(1) should ===(compositeFlow2.in)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "properly materialize empty builder" in {
      val builder = LinearTraversalBuilder.empty()

      val mat = testMaterialize(builder)
      mat.connections should ===(0)
      mat.outlets.length should ===(0)
      mat.inlets.length should ===(0)
      mat.matValue should ===(NotUsed)
    }

    "properly propagate materialized value with Keep.left" in {
      val builder =
        source.traversalBuilder.append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestSource")
    }

    "keep mapped materialized value of empty builder" in {
      val builder =
        LinearTraversalBuilder
          .empty()
          .transformMat((_: Any) => "NOTUSED")
          .append(source.traversalBuilder, source.shape, Keep.left)
          .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===("NOTUSED")
    }

    "properly propagate materialized value with Keep.right" in {
      val builder =
        source.traversalBuilder.append(sink.traversalBuilder, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestSink")
    }

    "properly propagate materialized value with Keep.both" in {
      val builder =
        source.traversalBuilder.append(sink.traversalBuilder, sink.shape, Keep.both)

      val mat = testMaterialize(builder)

      mat.matValue should ===(("TestSource", "TestSink"))
    }

    "properly propagate materialized value with Keep.left with Flow in middle" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.left)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestSource")
    }

    "properly propagate materialized value with Keep.right with Flow in middle (1)" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.right)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestFlow1")
    }

    "properly propagate materialized value with Keep.right with composite Flow in middle" in {
      val builder = source.traversalBuilder
        .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.right)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestFlowC1")
    }

    "properly propagate materialized value with Keep.right with composite Source as start" in {
      val builder =
        LinearTraversalBuilder
          .empty()
          .append(compositeSource.traversalBuilder, compositeSource.shape, Keep.right)
          .append(flow1.traversalBuilder, flow1.shape, Keep.left)
          .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestSource")
    }

    "properly propagate materialized value with Keep.right with composite Sink as end" in {
      val builder =
        source.traversalBuilder
          .append(flow1.traversalBuilder, flow1.shape, Keep.left)
          .append(compositeSink.traversalBuilder, compositeSink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestSink")
    }

    "properly propagate materialized value with Keep.both and all composite" in {
      val builder =
        LinearTraversalBuilder
          .empty()
          .append(compositeSource.traversalBuilder, compositeSource.shape, Keep.both)
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.both)
          .append(compositeSink.traversalBuilder, compositeSink.shape, Keep.both)

      val mat = testMaterialize(builder)

      mat.matValue should ===((((NotUsed, "TestSource"), "TestFlowC1"), "TestSink"))
    }

    "properly propagate materialized value with Keep.right with Flow in middle (2)" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.left)
        .append(sink.traversalBuilder, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.matValue should ===("TestSink")
    }

    "properly propagate materialized value with Keep.both with Flow in middle (1)" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.both)
        .append(sink.traversalBuilder, sink.shape, Keep.left)

      val mat = testMaterialize(builder)

      mat.matValue should ===(("TestSource", "TestFlow1"))
    }

    "properly propagate materialized value with Keep.both with Flow in middle (2)" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.both)
        .append(sink.traversalBuilder, sink.shape, Keep.both)

      val mat = testMaterialize(builder)

      mat.matValue should ===((("TestSource", "TestFlow1"), "TestSink"))
    }

    "properly map materialized value" in {
      val builder = source.traversalBuilder
        .append(flow1.traversalBuilder, flow1.shape, Keep.right)
        .append(sink.traversalBuilder, sink.shape, Keep.left)
        .transformMat("MAPPED: " + (_: String))

      val mat = testMaterialize(builder)

      mat.matValue should ===("MAPPED: TestFlow1")
    }

    "properly map materialized value (nested)" in {
      val flowBuilder =
        flow1.traversalBuilder.transformMat("M1: " + (_: String))

      val builder = source.traversalBuilder
        .append(flowBuilder, flow1.shape, Keep.right)
        .append(sink.traversalBuilder, sink.shape, Keep.left)
        .transformMat("M2: " + (_: String))

      val mat = testMaterialize(builder)

      mat.matValue should ===("M2: M1: TestFlow1")
    }

    "properly set attributes for whole chain" in {
      val builder = source.traversalBuilder
        .append(sink.traversalBuilder, sink.shape, Keep.left)
        .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test") and Attributes.name("testSink")),
          source -> (Attributes.name("test") and Attributes.name("testSource"))))
    }

    "properly accumulate attributes in chain" in {
      val builder = source.traversalBuilder
        .setAttributes(Attributes.name("source"))
        .append(sink.traversalBuilder, sink.shape, Keep.left)
        .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test") and Attributes.name("testSink")),
          source -> (Attributes.name("test") and Attributes.name("source"))))
    }

    "overwrite last attributes until a new module is added" in {
      val builder = source.traversalBuilder
        .setAttributes(Attributes.name("source"))
        .setAttributes(Attributes.name("source2"))
        .append(sink.traversalBuilder, sink.shape, Keep.left)
        .setAttributes(Attributes.name("test"))
        .setAttributes(Attributes.name("test2"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test2") and Attributes.name("testSink")),
          source -> (Attributes.name("test2") and Attributes.name("source2"))))
    }

    "propagate attributes to embedded linear sink and source" in {
      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("source"))
          .append(sink.traversalBuilder.setAttributes(Attributes.name("sink")), sink.shape, Keep.left)
          .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test") and Attributes.name("sink")),
          source -> (Attributes.name("test") and Attributes.name("source"))))
    }

    "propagate attributes to embedded linear flow" in {
      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("source"))
          .append(flow1.traversalBuilder.setAttributes(Attributes.name("flow")), flow1.shape, Keep.left)
          .setAttributes(Attributes.name("compositeSource"))
          .append(sink.traversalBuilder.setAttributes(Attributes.name("sink")), sink.shape, Keep.left)
          .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test") and Attributes.name("sink")),
          flow1 -> (Attributes.name("test") and Attributes.name("compositeSource") and Attributes.name("flow")),
          source -> (Attributes.name("test") and Attributes.name("compositeSource") and Attributes.name("source"))))
    }

    "propagate attributes to embedded composite sink" in {
      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("source"))
          .append(flow1.traversalBuilder.setAttributes(Attributes.name("flow")), flow1.shape, Keep.left)
          .append(compositeSink.traversalBuilder.setAttributes(Attributes.name("sink")), compositeSink.shape, Keep.left)
          .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          compositeSink -> (Attributes.name("test") and Attributes.name("sink")),
          flow1 -> (Attributes.name("test") and Attributes.name("flow")),
          source -> (Attributes.name("test") and Attributes.name("source"))))
    }

    "propagate attributes to embedded composite source" in {
      val builder =
        LinearTraversalBuilder
          .empty()
          .append(
            compositeSource.traversalBuilder.setAttributes(Attributes.name("source")),
            compositeSource.shape,
            Keep.left)
          .setAttributes(Attributes.name("source-outer"))
          .append(flow1.traversalBuilder.setAttributes(Attributes.name("flow")), flow1.shape, Keep.left)
          .append(sink.traversalBuilder.setAttributes(Attributes.name("sink")), compositeSink.shape, Keep.left)
          .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test") and Attributes.name("sink")),
          flow1 -> (Attributes.name("test") and Attributes.name("flow")),
          compositeSource -> (Attributes.name("test") and Attributes.name("source-outer") and Attributes.name(
            "source"))))
    }

    "propagate attributes to embedded composite flow" in {
      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("source"))
          .append(
            compositeFlow1.traversalBuilder.setAttributes(Attributes.name("flow")),
            compositeFlow1.shape,
            Keep.left)
          .append(sink.traversalBuilder.setAttributes(Attributes.name("sink")), compositeSink.shape, Keep.left)
          .setAttributes(Attributes.name("test"))

      val mat = testMaterialize(builder)

      mat.attributesAssignments should ===(
        List(
          sink -> (Attributes.name("test") and Attributes.name("sink")),
          compositeFlow1 -> (Attributes.name("test") and Attributes.name("flow")),
          source -> (Attributes.name("test") and Attributes.name("source"))))
    }

    "properly append a Source to empty linear" in {
      val builder =
        LinearTraversalBuilder
          .empty()
          .append(source.traversalBuilder, source.shape, Keep.right)
          .append(sink.traversalBuilder, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(source.out)
      mat.inlets(0) should ===(sink.in)
    }

    "properly append a Sink to empty linear" in {
      val nestedSink =
        LinearTraversalBuilder.empty().append(sink.traversalBuilder, sink.shape, Keep.right)

      val builder =
        source.traversalBuilder.append(nestedSink, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(source.out)
      mat.inlets(0) should ===(sink.in)
    }

    "properly append a Flow to empty linear" in {
      val nestedFlow =
        LinearTraversalBuilder.empty().append(flow1.traversalBuilder, flow1.shape, Keep.right)

      val builder =
        source.traversalBuilder
          .append(nestedFlow, flow1.shape, Keep.right)
          .append(sink.traversalBuilder, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.connections should ===(2)
      mat.outlets(1) should ===(source.out)
      mat.inlets(1) should ===(flow1.in)
      mat.outlets(0) should ===(flow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "properly append a composite Source to empty linear" in {
      val builder =
        LinearTraversalBuilder
          .empty()
          .append(compositeSource.traversalBuilder, compositeSource.shape, Keep.right)
          .append(sink.traversalBuilder, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(compositeSource.out)
      mat.inlets(0) should ===(sink.in)
    }

    "properly append a composite Sink to empty linear" in {
      val nestedSink =
        LinearTraversalBuilder.empty().append(compositeSink.traversalBuilder, compositeSink.shape, Keep.right)

      val builder =
        source.traversalBuilder.append(nestedSink, compositeSink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.connections should ===(1)
      mat.outlets(0) should ===(source.out)
      mat.inlets(0) should ===(compositeSink.in)
    }

    "properly append a composite Flow to empty linear" in {
      val nestedFlow =
        LinearTraversalBuilder.empty().append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.right)

      val builder =
        source.traversalBuilder
          .append(nestedFlow, compositeFlow1.shape, Keep.right)
          .append(sink.traversalBuilder, sink.shape, Keep.right)

      val mat = testMaterialize(builder)

      mat.connections should ===(2)
      mat.outlets(1) should ===(source.out)
      mat.inlets(1) should ===(compositeFlow1.in)
      mat.outlets(0) should ===(compositeFlow1.out)
      mat.inlets(0) should ===(sink.in)
    }

    "properly nest islands and attributes" in {
      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("island1"))
          .makeIsland(TestIsland1)
          .append(flow1.traversalBuilder, Keep.left)
          .makeIsland(TestIsland2)
          .setAttributes(Attributes.name("island2"))
          .append(flow2.traversalBuilder, Keep.left)
          .append(sink.traversalBuilder, Keep.left)

      val mat = testMaterialize(builder)

      mat.islandAssignments should ===(
        List(
          (sink, Attributes.none, TestDefaultIsland),
          (flow2, Attributes.none, TestDefaultIsland),
          (flow1, Attributes.name("island2"), TestIsland2),
          (source, Attributes.name("island2") and Attributes.name("island1"), TestIsland1)))
    }

    "properly nest flow with islands" in {
      val nestedFlow =
        flow1.traversalBuilder
          .append(flow2.traversalBuilder, Keep.left)
          .setAttributes(Attributes.name("nestedFlow"))
          .makeIsland(TestIsland1)

      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("source"))
          .append(nestedFlow, Keep.left)
          .append(sink.traversalBuilder, Keep.left)
          .setAttributes(Attributes.name("wholeThing")) // This will not be applied to the enclosing default island

      val mat = testMaterialize(builder)

      mat.islandAssignments should ===(
        List(
          (sink, Attributes.none, TestDefaultIsland),
          (flow2, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (flow1, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (source, Attributes.none, TestDefaultIsland)))
    }

    "properly nest flow with island inside another island" in {
      val nestedFlow =
        flow1.traversalBuilder
          .append(flow2.traversalBuilder, Keep.left)
          .setAttributes(Attributes.name("nestedFlow"))
          .makeIsland(TestIsland1)

      val builder =
        source.traversalBuilder
          .setAttributes(Attributes.name("source"))
          .append(nestedFlow, Keep.left)
          .makeIsland(TestIsland2)
          .append(sink.traversalBuilder, Keep.left)
          .setAttributes(Attributes.name("wholeThing")) // This will not be applied to the enclosing default island

      val mat = testMaterialize(builder)

      mat.islandAssignments should ===(
        List(
          (sink, Attributes.none, TestDefaultIsland),
          (flow2, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (flow1, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (source, Attributes.name("wholeThing"), TestIsland2)))
    }

    "properly nest flow with islands starting from linear enclosing a composite" in {
      val nestedFlow =
        flow2.traversalBuilder.setAttributes(Attributes.name("nestedFlow")).makeIsland(TestIsland1)

      val builder =
        source.traversalBuilder
          .append(compositeFlow1.traversalBuilder, compositeFlow1.shape, Keep.left)
          .makeIsland(TestIsland2)
          .append(nestedFlow, Keep.left)
          .append(sink.traversalBuilder, Keep.left)
          .setAttributes(Attributes.name("wholeThing")) // This will not be applied to the enclosing default island

      val mat = testMaterialize(builder)

      mat.islandAssignments should ===(
        List(
          (sink, Attributes.none, TestDefaultIsland),
          (flow2, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (compositeFlow1, Attributes.name("wholeThing"), TestIsland2),
          (source, Attributes.name("wholeThing"), TestIsland2)))
    }

    "properly nest flow containing composite with islands" in {
      val nestedFlow =
        flow1.traversalBuilder
          .append(compositeFlow2.traversalBuilder, compositeFlow1.shape, Keep.left)
          .setAttributes(Attributes.name("nestedFlow"))
          .makeIsland(TestIsland1)

      val builder =
        source.traversalBuilder
          .append(nestedFlow, Keep.left)
          .append(sink.traversalBuilder, Keep.left)
          .setAttributes(Attributes.name("wholeThing")) // This will not be applied to the enclosing default island

      val mat = testMaterialize(builder)

      mat.islandAssignments should ===(
        List(
          (sink, Attributes.none, TestDefaultIsland),
          (compositeFlow2, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (flow1, Attributes.name("wholeThing") and Attributes.name("nestedFlow"), TestIsland1),
          (source, Attributes.none, TestDefaultIsland)))
    }

  }

}
