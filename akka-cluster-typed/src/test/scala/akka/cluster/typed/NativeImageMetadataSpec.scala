/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import akka.cluster.typed.internal.receptionist.ClusterReceptionist
import akka.testkit.internal.NativeImageUtils
import akka.testkit.internal.NativeImageUtils.ModuleField
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // trixery around auto-selecting local or cluster receptionist impl
    ReflectConfigEntry(classOf[ClusterReceptionist.type].getName, fields = Seq(ModuleField)))

  val modulePackages = Seq("akka.cluster.typed", "akka.cluster.ddata.typed")

  val nativeImageUtils = new NativeImageUtils("akka-cluster-typed", additionalEntries, modulePackages)

  // run this to regenerate metadata 'akka-cluster-typed/Test/runMain akka.cluster.typed.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-cluster-typed" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
