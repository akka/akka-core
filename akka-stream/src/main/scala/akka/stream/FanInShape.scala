/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable

object FanInShape {
  sealed trait Init[O] {
    def outlet: Outlet[O]
    def inlets: immutable.Seq[Inlet[_]]
    def name: String
  }
  final case class Name[O](override val name: String) extends Init[O] {
    override def outlet: Outlet[O] = Outlet(s"$name.out")
    override def inlets: immutable.Seq[Inlet[_]] = Nil
  }
  final case class Ports[O](override val outlet: Outlet[O], override val inlets: immutable.Seq[Inlet[_]])
      extends Init[O] {
    override def name: String = "FanIn"
  }
}

abstract class FanInShape[+O] private (
    _out: Outlet[O @uncheckedVariance],
    _registered: Iterator[Inlet[_]],
    _name: String)
    extends Shape {
  import FanInShape._

  def this(init: FanInShape.Init[O]) = this(init.outlet, init.inlets.iterator, init.name)

  final def out: Outlet[O @uncheckedVariance] = _out
  final override def outlets: immutable.Seq[Outlet[O @uncheckedVariance]] = _out :: Nil

  /**
   * Not meant for overriding outside of Akka.
   */
  override def inlets: immutable.Seq[Inlet[_]] = _inlets

  /**
   * Performance of subclass `UniformFanInShape` relies on `_inlets` being a `Vector`, not a `List`.
   */
  private var _inlets: Vector[Inlet[_]] = Vector.empty
  protected def newInlet[T](name: String): Inlet[T] = {
    val p = if (_registered.hasNext) _registered.next().asInstanceOf[Inlet[T]] else Inlet[T](s"${_name}.$name")
    _inlets :+= p
    p
  }

  protected def construct(init: Init[O @uncheckedVariance]): FanInShape[O]

  def deepCopy(): FanInShape[O] = construct(Ports[O](_out.carbonCopy(), inlets.map(_.carbonCopy())))
}
