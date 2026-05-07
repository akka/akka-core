/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit.javadsl

import akka.actor.ClassicActorSystemProvider
import akka.stream.{ Materializer, SystemMaterializer }
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.testkit.scaladsl

object StreamTestKit {

  /**
   * Assert that there are no stages running under a given materializer.
   * Usually this assertion is run after a test-case to check that all of the
   * stages have terminated successfully.
   */
  def assertAllStagesStopped(mat: Materializer): Unit =
    mat match {
      case impl: PhasedFusingActorMaterializer =>
        scaladsl.StreamTestKit.assertNoChildren(impl.system, impl.supervisor)
      case _ =>
    }

  /**
   * Assert that there are no stages running under a given system's materializer.
   * Usually this assertion is run after a test-case to check that all of the
   * stages have terminated successfully.
   */
  def assertAllStagesStopped(system: ClassicActorSystemProvider): Unit = {
    assertAllStagesStopped(SystemMaterializer(system).materializer)
  }
}
