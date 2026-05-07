/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.scalatest

import org.scalatest.Informing

import akka.persistence.CapabilityFlag

trait OptionalTests {
  this: Informing =>

  def optional(flag: CapabilityFlag)(test: => Unit) = {
    val msg = s"CapabilityFlag `${flag.name}` was turned `" +
      (if (flag.value) "on" else "off") +
      "`. " + (if (!flag.value) "To enable the related tests override it with `CapabilityFlag.on` (or `true` in Scala)."
               else "")
    info(msg)
    if (flag.value)
      try test
      catch {
        case _: Exception =>
          throw new AssertionError(
            "Implementation did not pass this spec. " +
            "If your journal will be (by definition) unable to abide the here tested rule, you can disable this test," +
            s"by overriding [${flag.name}] with CapabilityFlag.off in your test class.")
      }
  }

}
