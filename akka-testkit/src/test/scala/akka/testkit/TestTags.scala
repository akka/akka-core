/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import org.scalatest.Tag

object TimingTest extends Tag("timing")
object LongRunningTest extends Tag("long-running")
object PerformanceTest extends Tag("performance")

object GHExcludeTest extends Tag("gh-exclude")
object GHExcludeAeronTest extends Tag("gh-exclude-aeron")
