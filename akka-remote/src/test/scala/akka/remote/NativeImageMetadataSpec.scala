/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import akka.actor.ActorSystem
import akka.actor.DynamicAccess
import akka.event.EventStream
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import akka.testkit.internal.NativeImageUtils
import com.typesafe.config.Config
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // akka.remote.watch-failure-detector.implementation-class (and then also in akka-cluster)
    ReflectConfigEntry(
      classOf[PhiAccrualFailureDetector].getName,
      methods = Seq(ReflectMethod(Constructor, Seq(classOf[Config].getName, classOf[EventStream].getName)))),
    // ssl-engine-provider
    ReflectConfigEntry(
      classOf[akka.remote.artery.tcp.ConfigSSLEngineProvider].getName,
      methods = Seq(ReflectMethod(Constructor, Seq(classOf[ActorSystem].getName)))),
    // used by akka-cluster but defined here
    ReflectConfigEntry(
      classOf[akka.remote.DeadlineFailureDetector].getName,
      methods =
        Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName, classOf[EventStream].getName)))),
    ReflectConfigEntry(
      classOf[RemoteActorRefProvider].getName,
      methods = Seq(
        ReflectMethod(
          Constructor,
          Seq(
            classOf[java.lang.String].getName,
            classOf[ActorSystem.Settings].getName,
            classOf[EventStream].getName,
            classOf[DynamicAccess].getName)))))

  val nativeImageUtils = new NativeImageUtils("akka-remote", additionalEntries, Seq("akka.remote"))

  // run this to regenerate metadata 'akka-remote/Test/runMain akka.remote.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-remote" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
