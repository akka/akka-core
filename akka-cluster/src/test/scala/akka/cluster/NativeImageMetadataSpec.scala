/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.actor.ActorSystem
import akka.actor.DynamicAccess
import akka.cluster.sbr.SplitBrainResolverProvider
import akka.event.EventStream
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import akka.testkit.internal.NativeImageUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // akka.cluster.downing-provider-class
    ReflectConfigEntry(
      classOf[SplitBrainResolverProvider].getName,
      methods = Seq(ReflectMethod(Constructor, Seq(classOf[ActorSystem].getName)))),
    ReflectConfigEntry(
      classOf[ClusterActorRefProvider].getName,
      methods = Seq(
        ReflectMethod(
          Constructor,
          Seq(
            classOf[java.lang.String].getName,
            classOf[ActorSystem.Settings].getName,
            classOf[EventStream].getName,
            classOf[DynamicAccess].getName)))),
    // default downing-provider
    ReflectConfigEntry(
      classOf[akka.cluster.NoDowning].getName,
      methods = Seq(ReflectMethod(Constructor, Seq(classOf[ActorSystem].getName)))))

  val nativeImageUtils = new NativeImageUtils("akka-cluster", additionalEntries, Seq("akka.cluster"))

  // run this to regenerate metadata 'akka-cluster/Test/runMain akka.cluster.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-cluster" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
