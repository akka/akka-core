/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

// we could migrate AkkaSpec to extend this
abstract class AbstractSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterEach
