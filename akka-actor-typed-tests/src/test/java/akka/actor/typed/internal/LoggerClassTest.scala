/*
 * Copyright (C) 2025-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import org.junit.Test
import org.scalatestplus.junit.JUnitSuite

class LoggerClassTest extends JUnitSuite {
  @Test
  def testLoggerClass(): Unit = {
    // LoggerClass.detectLoggerClassFromStack(Void.TYPE) frames prefixed with
    // akka.actor.typed.internal, so we can either move this test, or indirect the call
    // through a lambda-based method:
    val value = Some(()).map(_ => LoggerClass.detectLoggerClassFromStack(Void.TYPE))
    assert(value.get == classOf[Option[_]])
  }
}
