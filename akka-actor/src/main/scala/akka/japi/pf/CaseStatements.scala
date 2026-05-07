/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi.pf

import FI.{ Apply, Predicate, UnitApply }

private[pf] object CaseStatement {
  def empty[F, T](): PartialFunction[F, T] = PartialFunction.empty
}

private[pf] class CaseStatement[-F, +P, T](predicate: Predicate, apply: Apply[P, T]) extends PartialFunction[F, T] {

  override def isDefinedAt(o: F) = predicate.defined(o)

  override def apply(o: F) = apply.apply(o.asInstanceOf[P])
}

private[pf] class UnitCaseStatement[F, P](predicate: Predicate, apply: UnitApply[P]) extends PartialFunction[F, Unit] {

  override def isDefinedAt(o: F) = predicate.defined(o)

  override def apply(o: F) = apply.apply(o.asInstanceOf[P])
}
