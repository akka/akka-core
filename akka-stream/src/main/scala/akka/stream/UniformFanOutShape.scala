/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable

object UniformFanOutShape {
  def apply[I, O](inlet: Inlet[I], outlets: Outlet[O]*): UniformFanOutShape[I, O] =
    new UniformFanOutShape(outlets.size, FanOutShape.Ports(inlet, outlets.toList))
}

class UniformFanOutShape[-I, +O](n: Int, _init: FanOutShape.Init[I @uncheckedVariance]) extends FanOutShape[I](_init) {

  //initialize by side-effect
  for (i <- 0 until n) newOutlet[O](s"out$i")

  def this(n: Int) = this(n, FanOutShape.Name[I]("UniformFanOut"))
  def this(n: Int, name: String) = this(n, FanOutShape.Name[I](name))
  def this(inlet: Inlet[I], outlets: Array[Outlet[O]]) = this(outlets.length, FanOutShape.Ports(inlet, outlets.toList))
  override protected def construct(init: FanOutShape.Init[I @uncheckedVariance]): FanOutShape[I] =
    new UniformFanOutShape(n, init)
  override def deepCopy(): UniformFanOutShape[I, O] = super.deepCopy().asInstanceOf[UniformFanOutShape[I, O]]

  final override def outlets: immutable.Seq[Outlet[O @uncheckedVariance]] =
    super.outlets.asInstanceOf[immutable.Seq[Outlet[O]]]

  def out(n: Int): Outlet[O @uncheckedVariance] = outlets(n)
}
