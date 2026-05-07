/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.Failed

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.stream.testkit.scaladsl.StreamTestKit.assertNoChildren
import akka.stream.testkit.scaladsl.StreamTestKit.printDebugDump
import akka.stream.testkit.scaladsl.StreamTestKit.stopAllChildren
import akka.testkit.AkkaSpec
import akka.testkit.TestKitUtils

abstract class StreamSpec(_system: ActorSystem) extends AkkaSpec(_system) {

  def this(config: Config) =
    this(
      ActorSystem(
        TestKitUtils.testNameFromCallStack(classOf[StreamSpec], "".r),
        ConfigFactory.load(config.withFallback(AkkaSpec.testConf))))

  def this(s: String) = this(ConfigFactory.parseString(s))

  def this(configMap: Map[String, _]) = this(AkkaSpec.mapToConfig(configMap))

  def this() = this(ActorSystem(TestKitUtils.testNameFromCallStack(classOf[StreamSpec], "".r), AkkaSpec.testConf))

  override def withFixture(test: NoArgTest) = {
    super.withFixture(test) match {
      case failed: Failed =>
        implicit val ec = system.dispatcher
        Materializer(_system) match {
          case impl: PhasedFusingActorMaterializer =>
            println("--- Stream actors debug dump (only works for tests using system materializer) ---")
            printDebugDump(impl.supervisor)
            println("--- Stream actors debug dump end ---")
            // make sure not to leak running streams from failed to next test case
            stopAllChildren(impl.system, impl.supervisor)
          case _ =>
        }
        failed
      case other =>
        Materializer(_system) match {
          case impl: PhasedFusingActorMaterializer =>
            // Note that this is different from assertAllStages stopped since it tries to
            // *kill* all streams first, before checking if any is stuck. It also does not
            // work for tests starting their own materializers.
            stopAllChildren(impl.system, impl.supervisor)
            val result = test.apply()
            assertNoChildren(impl.system, impl.supervisor)
            result
          case _ => other
        }
    }
  }
}
