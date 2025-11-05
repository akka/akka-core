/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JavaVersion {

  val majorVersion: Int = {
    // FIXME replace with Runtime.version() when we no longer support Java 8
    // See Oracle section 1.5.3 at:
    // https://docs.oracle.com/javase/8/docs/technotes/guides/versioning/spec/versioning2.html
    val version = System.getProperty("java.specification.version").split('.')

    val majorString =
      if (version(0) == "1") version(1) // Java 8 will be 1.8
      else version(0) // later will be 9, 10, 11 etc

    majorString.toInt
  }
}
