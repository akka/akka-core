/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.discovery

import akka.actor.ExtendedActorSystem
import akka.discovery.aggregate.AggregateServiceDiscovery
import akka.discovery.config.ConfigServiceDiscovery
import akka.discovery.dns.DnsServiceDiscovery
import akka.testkit.internal.NativeImageUtils
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // akka.discovery.config.class
    ReflectConfigEntry(
      classOf[ConfigServiceDiscovery].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[ExtendedActorSystem].getName)))),
    // akka.discovery.aggregate.class
    ReflectConfigEntry(
      classOf[AggregateServiceDiscovery].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[ExtendedActorSystem].getName)))),
    // akka.discovery.akka-dns.class
    ReflectConfigEntry(
      classOf[DnsServiceDiscovery].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[ExtendedActorSystem].getName)))))

  val nativeImageUtils = new NativeImageUtils("akka-discovery", additionalEntries, Seq("akka.discovery"))

  // run this to regenerate metadata 'akka-discovery/Test/runMain akka.discovery.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-discovery" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
