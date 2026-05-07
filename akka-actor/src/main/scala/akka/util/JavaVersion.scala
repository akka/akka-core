/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import akka.annotation.InternalApi

import scala.annotation.nowarn

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JavaVersion {

  @nowarn("msg=deprecated")
  val majorVersion: Int =
    // FIXME replace with feature() when we no longer support Java 9 (added in JDK 10)
    Runtime.version().major()

}
