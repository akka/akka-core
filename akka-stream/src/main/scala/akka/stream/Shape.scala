/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable

import akka.annotation.InternalApi
import akka.util.Collections.EmptyImmutableSeq
import scala.jdk.CollectionConverters._

/**
 * An input port of a StreamLayout.Module. This type logically belongs
 * into the impl package but must live here due to how `sealed` works.
 * It is also used in the Java DSL for “classic Inlets” as a work-around
 * for otherwise unreasonable existential types.
 */
sealed abstract class InPort { self: Inlet[_] =>
  final override def hashCode: Int = super.hashCode
  final override def equals(that: Any): Boolean = this eq that.asInstanceOf[AnyRef]

  /**
   * INTERNAL API
   */
  @volatile private[stream] var id: Int = -1

  /**
   * INTERNAL API
   */
  @volatile private[stream] var mappedTo: InPort = this

  /**
   * INTERNAL API
   */
  private[stream] def inlet: Inlet[_] = this
}

/**
 * An output port of a StreamLayout.Module. This type logically belongs
 * into the impl package but must live here due to how `sealed` works.
 * It is also used in the Java DSL for “classic Outlets” as a work-around
 * for otherwise unreasonable existential types.
 */
sealed abstract class OutPort { self: Outlet[_] =>
  final override def hashCode: Int = super.hashCode
  final override def equals(that: Any): Boolean = this eq that.asInstanceOf[AnyRef]

  /**
   * INTERNAL API
   */
  @volatile private[stream] var id: Int = -1

  /**
   * INTERNAL API
   */
  @volatile private[stream] var mappedTo: OutPort = this

  /**
   * INTERNAL API
   */
  private[stream] def outlet: Outlet[_] = this
}

/**
 * An Inlet is a typed input to a Shape. Its partner in the Module view
 * is the InPort (which does not bear an element type because Modules only
 * express the internal structural hierarchy of stream topologies).
 */
object Inlet {

  /**
   * Scala API
   *
   * Creates a new Inlet with the given name. The name will be used when
   * displaying debug information or error messages involving the port.
   */
  def apply[T](name: String): Inlet[T] = new Inlet[T](name)

  /**
   * JAVA API
   *
   * Creates a new Inlet with the given name. The name will be used when
   * displaying debug information or error messages involving the port.
   */
  def create[T](name: String): Inlet[T] = Inlet(name)
}

final class Inlet[T] private (val s: String) extends InPort {
  def carbonCopy(): Inlet[T] = {
    val in = Inlet[T](s)
    in.mappedTo = this
    in
  }

  /**
   * INTERNAL API.
   */
  def as[U]: Inlet[U] = this.asInstanceOf[Inlet[U]]

  override def toString: String =
    s + "(" + this.hashCode + ")" +
    (if (mappedTo eq this) ""
     else s" mapped to $mappedTo")
}

/**
 * An Outlet is a typed output to a Shape. Its partner in the Module view
 * is the OutPort (which does not bear an element type because Modules only
 * express the internal structural hierarchy of stream topologies).
 */
object Outlet {

  /**
   * Scala API
   *
   * Creates a new Outlet with the given name. The name will be used when
   * displaying debug information or error messages involving the port.
   */
  def apply[T](name: String): Outlet[T] = new Outlet[T](name)

  /**
   * JAVA API
   *
   * Creates a new Outlet with the given name. The name will be used when
   * displaying debug information or error messages involving the port.
   */
  def create[T](name: String): Outlet[T] = Outlet(name)
}

final class Outlet[T] private (val s: String) extends OutPort {
  def carbonCopy(): Outlet[T] = {
    val out = Outlet[T](s)
    out.mappedTo = this
    out
  }

  /**
   * INTERNAL API.
   */
  def as[U]: Outlet[U] = this.asInstanceOf[Outlet[U]]

  override def toString: String =
    s + "(" + this.hashCode + ")" +
    (if (mappedTo eq this) ""
     else s" mapped to $mappedTo")
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Shape {

  /**
   * `inlets` and `outlets` can be `Vector` or `List` so this method
   * checks the size of 1 in an optimized way.
   */
  def hasOnePort(ports: immutable.Seq[_]): Boolean = {
    ports.nonEmpty && (ports match {
      case l: List[_] => l.tail.isEmpty // assuming List is most common
      case _          => ports.size == 1 // e.g. Vector
    })
  }
}

/**
 * A Shape describes the inlets and outlets of a [[Graph]]. In keeping with the
 * philosophy that a Graph is a freely reusable blueprint, everything that
 * matters from the outside are the connections that can be made with it,
 * otherwise it is just a black box.
 */
abstract class Shape {

  /**
   * Scala API: get a list of all input ports
   */
  def inlets: immutable.Seq[Inlet[_]]

  /**
   * Scala API: get a list of all output ports
   */
  def outlets: immutable.Seq[Outlet[_]]

  /**
   * Create a copy of this Shape object, returning the same type as the
   * original; this constraint can unfortunately not be expressed in the
   * type system.
   */
  def deepCopy(): Shape

  /**
   * Java API: get a list of all input ports
   */
  def getInlets: java.util.List[Inlet[_]] = inlets.asJava

  /**
   * Java API: get a list of all output ports
   */
  def getOutlets: java.util.List[Outlet[_]] = outlets.asJava

  /**
   * Compare this to another shape and determine whether the set of ports is the same (ignoring their ordering).
   */
  def hasSamePortsAs(s: Shape): Boolean =
    inlets.toSet == s.inlets.toSet && outlets.toSet == s.outlets.toSet

  /**
   * Compare this to another shape and determine whether the arrangement of ports is the same (including their ordering).
   */
  def hasSamePortsAndShapeAs(s: Shape): Boolean =
    inlets == s.inlets && outlets == s.outlets

  /**
   * Asserting version of [[#hasSamePortsAs]].
   */
  def requireSamePortsAs(s: Shape): Unit = require(hasSamePortsAs(s), nonCorrespondingMessage(s))

  /**
   * Asserting version of [[#hasSamePortsAndShapeAs]].
   */
  def requireSamePortsAndShapeAs(s: Shape): Unit = require(hasSamePortsAndShapeAs(s), nonCorrespondingMessage(s))

  private def nonCorrespondingMessage(s: Shape) =
    s"The inlets [${s.inlets.mkString(", ")}] and outlets [${s.outlets.mkString(", ")}] must correspond to the inlets [${inlets
      .mkString(", ")}] and outlets [${outlets.mkString(", ")}]"
}

/**
 * Java API for creating custom [[Shape]] types.
 */
abstract class AbstractShape extends Shape {

  /**
   * Provide the list of all input ports of this shape.
   */
  def allInlets: java.util.List[Inlet[_]]

  /**
   * Provide the list of all output ports of this shape.
   */
  def allOutlets: java.util.List[Outlet[_]]

  final override lazy val inlets: immutable.Seq[Inlet[_]] = allInlets.asScala.toList
  final override lazy val outlets: immutable.Seq[Outlet[_]] = allOutlets.asScala.toList

  final override def getInlets = allInlets
  final override def getOutlets = allOutlets
}

/**
 * This [[Shape]] is used for graphs that have neither open inputs nor open
 * outputs. Only such a [[Graph]] can be materialized by a [[Materializer]].
 */
sealed abstract class ClosedShape extends Shape
object ClosedShape extends ClosedShape {
  override val inlets: immutable.Seq[Inlet[_]] = EmptyImmutableSeq
  override val outlets: immutable.Seq[Outlet[_]] = EmptyImmutableSeq
  override def deepCopy() = this

  /**
   * Java API: obtain ClosedShape instance
   */
  def getInstance: ClosedShape = this

  override def toString: String = "ClosedShape"
}

/**
 * This type of [[Shape]] can express any number of inputs and outputs at the
 * expense of forgetting about their specific types. It is used mainly in the
 * implementation of the [[Graph]] builders and typically replaced by a more
 * meaningful type of Shape when the building is finished.
 */
case class AmorphousShape(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]) extends Shape {
  override def deepCopy() = AmorphousShape(inlets.map(_.carbonCopy()), outlets.map(_.carbonCopy()))
}

/**
 * A Source [[Shape]] has exactly one output and no inputs, it models a source
 * of data.
 */
final case class SourceShape[+T](out: Outlet[T @uncheckedVariance]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = EmptyImmutableSeq
  override val outlets: immutable.Seq[Outlet[_]] = out :: Nil

  override def deepCopy(): SourceShape[T] = SourceShape(out.carbonCopy())
}
object SourceShape {

  /** Java API */
  def of[T](outlet: Outlet[T @uncheckedVariance]): SourceShape[T] =
    SourceShape(outlet)
}

/**
 * A Flow [[Shape]] has exactly one input and one output, it looks from the
 * outside like a pipe (but it can be a complex topology of streams within of
 * course).
 */
final case class FlowShape[-I, +O](in: Inlet[I @uncheckedVariance], out: Outlet[O @uncheckedVariance]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = in :: Nil
  override val outlets: immutable.Seq[Outlet[_]] = out :: Nil

  override def deepCopy(): FlowShape[I, O] = FlowShape(in.carbonCopy(), out.carbonCopy())
}
object FlowShape {

  /** Java API */
  def of[I, O](inlet: Inlet[I @uncheckedVariance], outlet: Outlet[O @uncheckedVariance]): FlowShape[I, O] =
    FlowShape(inlet, outlet)
}

/**
 * A Sink [[Shape]] has exactly one input and no outputs, it models a data sink.
 */
final case class SinkShape[-T](in: Inlet[T @uncheckedVariance]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = in :: Nil
  override val outlets: immutable.Seq[Outlet[_]] = EmptyImmutableSeq

  override def deepCopy(): SinkShape[T] = SinkShape(in.carbonCopy())
}
object SinkShape {

  /** Java API */
  def of[T](inlet: Inlet[T @uncheckedVariance]): SinkShape[T] =
    SinkShape(inlet)
}

//#bidi-shape
/**
 * A bidirectional flow of elements that consequently has two inputs and two
 * outputs, arranged like this:
 *
 * {{{
 *        +------+
 *  In1 ~>|      |~> Out1
 *        | bidi |
 * Out2 <~|      |<~ In2
 *        +------+
 * }}}
 */
final case class BidiShape[-In1, +Out1, -In2, +Out2](
    in1: Inlet[In1 @uncheckedVariance],
    out1: Outlet[Out1 @uncheckedVariance],
    in2: Inlet[In2 @uncheckedVariance],
    out2: Outlet[Out2 @uncheckedVariance])
    extends Shape {
  //#implementation-details-elided
  override val inlets: immutable.Seq[Inlet[_]] = in1 :: in2 :: Nil
  override val outlets: immutable.Seq[Outlet[_]] = out1 :: out2 :: Nil

  /**
   * Java API for creating from a pair of unidirectional flows.
   */
  def this(top: FlowShape[In1, Out1], bottom: FlowShape[In2, Out2]) = this(top.in, top.out, bottom.in, bottom.out)

  override def deepCopy(): BidiShape[In1, Out1, In2, Out2] =
    BidiShape(in1.carbonCopy(), out1.carbonCopy(), in2.carbonCopy(), out2.carbonCopy())

  //#implementation-details-elided
}
//#bidi-shape
object BidiShape {
  def fromFlows[I1, O1, I2, O2](top: FlowShape[I1, O1], bottom: FlowShape[I2, O2]): BidiShape[I1, O1, I2, O2] =
    BidiShape(top.in, top.out, bottom.in, bottom.out)

  /** Java API */
  def of[In1, Out1, In2, Out2](
      in1: Inlet[In1 @uncheckedVariance],
      out1: Outlet[Out1 @uncheckedVariance],
      in2: Inlet[In2 @uncheckedVariance],
      out2: Outlet[Out2 @uncheckedVariance]): BidiShape[In1, Out1, In2, Out2] =
    BidiShape(in1, out1, in2, out2)

}
