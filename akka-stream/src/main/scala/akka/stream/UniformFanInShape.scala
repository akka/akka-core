/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable

object UniformFanInShape {
  def apply[I, O](outlet: Outlet[O], inlets: Inlet[I]*): UniformFanInShape[I, O] =
    new UniformFanInShape(inlets.size, FanInShape.Ports(outlet, inlets.toList))

  /** Java API */
  def create[I, O](outlet: Outlet[O], inlets: java.util.List[Inlet[I]]): UniformFanInShape[I, O] = {
    import scala.jdk.CollectionConverters._
    new UniformFanInShape(inlets.size, FanInShape.Ports(outlet, inlets.asScala.toList))
  }
}

class UniformFanInShape[-T, +O](val n: Int, _init: FanInShape.Init[O]) extends FanInShape[O](_init) {

  //ports get added to `FanInShape.inlets` as a side-effect of calling `newInlet`
  for (i <- 0 until n) newInlet[T](s"in$i")

  def this(n: Int) = this(n, FanInShape.Name[O]("UniformFanIn"))
  def this(n: Int, name: String) = this(n, FanInShape.Name[O](name))
  def this(outlet: Outlet[O], inlets: Array[Inlet[T]]) = this(inlets.length, FanInShape.Ports(outlet, inlets.toList))
  override protected def construct(init: FanInShape.Init[O @uncheckedVariance]): FanInShape[O] =
    new UniformFanInShape(n, init)
  override def deepCopy(): UniformFanInShape[T, O] = super.deepCopy().asInstanceOf[UniformFanInShape[T, O]]

  final override def inlets: immutable.Seq[Inlet[T @uncheckedVariance]] =
    super.inlets.asInstanceOf[immutable.Seq[Inlet[T]]]

  def in(n: Int): Inlet[T @uncheckedVariance] = inlets(n)
}
