/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable

object FanOutShape {
  sealed trait Init[I] {
    def inlet: Inlet[I]
    def outlets: immutable.Seq[Outlet[_]]
    def name: String
  }
  final case class Name[I](override val name: String) extends Init[I] {
    override def inlet: Inlet[I] = Inlet(s"$name.in")
    override def outlets: immutable.Seq[Outlet[_]] = Nil
  }
  final case class Ports[I](override val inlet: Inlet[I], override val outlets: immutable.Seq[Outlet[_]])
      extends Init[I] {
    override def name: String = "FanOut"
  }
}

abstract class FanOutShape[-I] private (
    _in: Inlet[I @uncheckedVariance],
    _registered: Iterator[Outlet[_]],
    _name: String)
    extends Shape {
  import FanOutShape._

  def this(init: FanOutShape.Init[I]) = this(init.inlet, init.outlets.iterator, init.name)

  final def in: Inlet[I @uncheckedVariance] = _in

  /**
   * Not meant for overriding outside of Akka.
   */
  override def outlets: immutable.Seq[Outlet[_]] = _outlets
  final override def inlets: immutable.Seq[Inlet[I @uncheckedVariance]] = in :: Nil

  /**
   * Performance of subclass `UniformFanOutShape` relies on `_outlets` being a `Vector`, not a `List`.
   */
  private var _outlets: Vector[Outlet[_]] = Vector.empty
  protected def newOutlet[T](name: String): Outlet[T] = {
    val p = if (_registered.hasNext) _registered.next().asInstanceOf[Outlet[T]] else Outlet[T](s"${_name}.$name")
    _outlets :+= p
    p
  }

  protected def construct(init: Init[I @uncheckedVariance]): FanOutShape[I]

  def deepCopy(): FanOutShape[I] = construct(Ports[I](_in.carbonCopy(), outlets.map(_.carbonCopy())))
}
