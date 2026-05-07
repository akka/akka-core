/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event.slf4j

import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import akka.testkit.internal.NativeImageUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    ReflectConfigEntry(
      classOf[akka.event.slf4j.Slf4jLogger].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor))))

  val nativeImageUtils = new NativeImageUtils("akka-slf4j", additionalEntries, Seq("akka.event.slf4j"))

  // run this to regenerate metadata 'akka-slf4j/Test/runMain akka.event.slf4j.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-slf4j" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
