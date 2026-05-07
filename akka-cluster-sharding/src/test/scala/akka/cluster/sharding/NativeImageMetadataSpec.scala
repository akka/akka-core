/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import akka.actor.ActorSystem
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import akka.testkit.internal.NativeImageUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // akka.management.health-checks.readiness-checks.sharding
    ReflectConfigEntry(
      "akka.cluster.sharding.ClusterShardingHealthCheck",
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[ActorSystem].getName)))))

  val nativeImageUtils = new NativeImageUtils("akka-cluster-sharding", additionalEntries, Seq("akka.cluster.sharding"))

  // run this to regenerate metadata 'akka-cluster-sharding/Test/runMain akka.cluster.sharding.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-cluster-sharding" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing.trim should ===(current.trim)
    }
  }

}
