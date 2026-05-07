/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import akka.testkit.internal.NativeImageUtils
import com.typesafe.config.Config
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // akka.cluster.metrics.supervisor.strategy.provider
    ReflectConfigEntry(
      "akka.cluster.metrics.ClusterMetricsStrategy",
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName)))))

  val nativeImageUtils = new NativeImageUtils("akka-cluster-metrics", additionalEntries, Seq("akka.cluster.metrics"))

  // run this to regenerate metadata 'akka-cluster-metrics/Test/runMain akka.cluster.metrics.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-cluster-metrics" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
