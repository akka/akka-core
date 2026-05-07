/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.NotUsed
import akka.stream._
import akka.stream.impl.StreamLayout.AtomicModule

object TraversalTestUtils {

  // --- These test classes do not use the optimized linear builder, for testing the composite builder instead
  class CompositeTestSource extends AtomicModule[SourceShape[Any], Any] {
    val out = Outlet[Any]("testSourceC.out")
    override val shape: Shape = SourceShape(out)
    override val traversalBuilder = TraversalBuilder.atomic(this, Attributes.name("testSource"))

    override def withAttributes(attributes: Attributes): AtomicModule[SourceShape[Any], Any] = ???
    override def toString = "TestSource"
  }

  class CompositeTestSink extends AtomicModule[SinkShape[Any], Any] {
    val in = Inlet[Any]("testSinkC.in")
    override val shape: Shape = SinkShape(in)
    override val traversalBuilder = TraversalBuilder.atomic(this, Attributes.name("testSink"))

    override def withAttributes(attributes: Attributes): AtomicModule[SinkShape[Any], Any] = ???
    override def toString = "TestSink"
  }

  class CompositeTestFlow(tag: String) extends AtomicModule[FlowShape[Any, Any], Any] {
    val in = Inlet[Any](s"testFlowC$tag.in")
    val out = Outlet[Any](s"testFlowC$tag.out")
    override val shape: Shape = FlowShape(in, out)
    override val traversalBuilder = TraversalBuilder.atomic(this, Attributes.name(s"testFlow$tag"))

    override def withAttributes(attributes: Attributes): AtomicModule[FlowShape[Any, Any], Any] = ???
    override def toString = s"TestFlow$tag"
  }

  // --- These test classes DO use the optimized linear builder, for testing the composite builder instead
  class LinearTestSource extends AtomicModule[SourceShape[Any], Any] {
    val out = Outlet[Any]("testSource.out")
    override val shape: Shape = SourceShape(out)
    override val traversalBuilder: LinearTraversalBuilder =
      LinearTraversalBuilder.fromModule(this, Attributes.name("testSource"))

    override def withAttributes(attributes: Attributes): AtomicModule[SourceShape[Any], Any] = ???
    override def toString = "TestSource"
  }

  class LinearTestSink extends AtomicModule[SinkShape[Any], Any] {
    val in = Inlet[Any]("testSink.in")
    override val shape: Shape = SinkShape(in)
    override val traversalBuilder: LinearTraversalBuilder =
      LinearTraversalBuilder.fromModule(this, Attributes.name("testSink"))

    override def withAttributes(attributes: Attributes): AtomicModule[SinkShape[Any], Any] = ???
    override def toString = "TestSink"
  }

  class LinearTestFlow(tag: String) extends AtomicModule[FlowShape[Any, Any], Any] {
    val in = Inlet[Any](s"testFlow$tag.in")
    val out = Outlet[Any](s"testFlow$tag.out")
    override val shape: Shape = FlowShape(in, out)
    override val traversalBuilder: LinearTraversalBuilder =
      LinearTraversalBuilder.fromModule(this, Attributes.name(s"testFlow$tag"))

    override def withAttributes(attributes: Attributes): AtomicModule[FlowShape[Any, Any], Any] = ???
    override def toString = s"TestFlow$tag"
  }

  class MaterializationResult(
      val connections: Int,
      val inlets: Array[InPort],
      val outlets: Array[OutPort],
      val matValue: Any,
      val attributesAssignments: List[(AtomicModule[Shape, Any], Attributes)],
      val islandAssignments: List[(AtomicModule[Shape, Any], Attributes, IslandTag)]) {

    override def toString = {
      outlets.iterator.zip(inlets.iterator).mkString("connections: ", ", ", "")
    }
  }

  case object TestDefaultIsland extends IslandTag
  case object TestIsland1 extends IslandTag
  case object TestIsland2 extends IslandTag
  case object TestIsland3 extends IslandTag

  /**
   * This test method emulates a materialization run. It simply puts input and output ports into slots of an Array.
   * After running this method, it can be tested that ports that are meant to be wired together have been put into
   * corresponding slots of the [[MaterializationResult]].
   */
  def testMaterialize(b: TraversalBuilder): MaterializationResult = {
    require(b.isTraversalComplete, "Traversal builder must be complete")

    var attributesResult: List[(AtomicModule[Shape, Any], Attributes)] = Nil
    var islandsResult: List[(AtomicModule[Shape, Any], Attributes, IslandTag)] = Nil
    var islandStack: List[(IslandTag, Attributes)] = (TestDefaultIsland, Attributes.none) :: Nil

    val connections = b.inSlots
    val inlets = new Array[InPort](connections)
    val outlets = new Array[OutPort](connections)

    // Track next assignable number for input ports
    var inOffs = 0

    var current: Traversal = b.traversal
    val traversalStack = new java.util.ArrayList[Traversal](16)
    traversalStack.add(current)

    val matValueStack = new java.util.ArrayDeque[Any](8)
    val attributesStack = new java.util.ArrayDeque[Attributes](8)
    attributesStack.addLast(Attributes.none)

    // Due to how Concat works, we need a stack. This probably can be optimized for the most common cases.
    while (!traversalStack.isEmpty) {
      current = traversalStack.remove(traversalStack.size() - 1)

      while (current ne EmptyTraversal) {
        var nextStep: Traversal = EmptyTraversal

        current match {
          case MaterializeAtomic(mod, outToSlot) =>
            var i = 0
            val inletsIter = mod.shape.inlets.iterator
            while (inletsIter.hasNext) {
              val in = inletsIter.next()
              inlets(inOffs + i) = in
              i += 1
            }

            val outletsIter = mod.shape.outlets.iterator
            while (outletsIter.hasNext) {
              val out = outletsIter.next()
              outlets(inOffs + outToSlot(out.id)) = out
            }

            // Recording attributes assignment results for testing purposes
            attributesResult = (mod -> attributesStack.getLast) :: attributesResult
            islandsResult = (mod, islandStack.head._2, islandStack.head._1) :: islandsResult

            // Dummy materialized value is simply the name of the module
            matValueStack.addLast(mod.toString)
            inOffs += mod.shape.inlets.size
          case Pop =>
            matValueStack.removeLast()
          case PushNotUsed =>
            matValueStack.addLast(NotUsed)
          case transform: Transform =>
            val prev = matValueStack.removeLast()
            val result = transform(prev)
            matValueStack.addLast(result)
          case compose: Compose =>
            val second = matValueStack.removeLast()
            val first = matValueStack.removeLast()
            val result = compose(first, second)
            matValueStack.addLast(result)
          case PushAttributes(attr) =>
            attributesStack.addLast(attributesStack.getLast and attr)
          case PopAttributes =>
            attributesStack.removeLast()
          case Concat(first, next) =>
            if (next ne EmptyTraversal) traversalStack.add(next)
            nextStep = first
          case EnterIsland(tag) =>
            islandStack = (tag, attributesStack.getLast) :: islandStack
          case ExitIsland =>
            islandStack = islandStack.tail
          case _ =>
        }

        current = nextStep
      }
    }

    new MaterializationResult(
      connections,
      inlets,
      outlets,
      matValueStack.peekLast(),
      attributesResult.reverse,
      islandsResult.reverse)
  }

}
