/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import akka.testkit.internal.NativeImageUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val nativeImageUtils = new NativeImageUtils("akka-cluster-sharding-typed", Seq(), Seq("akka.cluster.sharding.typed"))

  // run this to regenerate metadata 'akka-cluster-sharding-typed/Test/runMain akka.cluster.sharding.typed.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-cluster-sharding-typed" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
