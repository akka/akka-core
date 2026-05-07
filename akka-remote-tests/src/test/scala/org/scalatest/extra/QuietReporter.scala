/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package org.scalatest.extra

import java.lang.Boolean.getBoolean

import org.scalatest.events._
import org.scalatest.tools.StandardOutReporter

class QuietReporter(inColor: Boolean, withDurations: Boolean = false)
    extends StandardOutReporter(withDurations, inColor, false, true, false, false, false, false, false, false, false) {

  def this() = this(!getBoolean("akka.test.nocolor"), !getBoolean("akka.test.nodurations"))

  override def apply(event: Event): Unit = event match {
    case _: RunStarting => ()
    case _              => super.apply(event)
  }
}
