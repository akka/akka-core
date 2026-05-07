/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

import akka.testkit.internal.NativeImageUtils
import akka.testkit.internal.NativeImageUtils.Condition
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ModuleField
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.CollectionHasAsScala

object NativeImageMetadataSpec {

  def additionalEntries = {
    val config = ConfigFactory.load()
    val jacksonModules = config
      .getStringList("akka.serialization.jackson.jackson-modules")
      .asScala
      .map { className =>
        ReflectConfigEntry(className, methods = Seq(ReflectMethod(Constructor)))
      }
      .toVector

    val manualAdditions = Seq(
      ReflectConfigEntry("akka.serialization.jackson.CborSerializable", fields = Seq(ModuleField)),
      ReflectConfigEntry("akka.serialization.jackson.JsonSerializable", fields = Seq(ModuleField)),
      ReflectConfigEntry("akka.serialization.jackson.JacksonObjectMapperProvider$", fields = Seq(ModuleField)),
      // specific jackson serializers/de-serializers
      ReflectConfigEntry("akka.serialization.jackson.ActorRefSerializer", methods = Seq(ReflectMethod(Constructor))),
      ReflectConfigEntry("akka.serialization.jackson.ActorRefDeserializer", methods = Seq(ReflectMethod(Constructor))),
      ReflectConfigEntry("akka.serialization.jackson.AddressSerializer", methods = Seq(ReflectMethod(Constructor))),
      ReflectConfigEntry("akka.serialization.jackson.AddressDeserializer", methods = Seq(ReflectMethod(Constructor))),
      ReflectConfigEntry(
        "akka.serialization.jackson.FiniteDurationSerializer",
        methods = Seq(ReflectMethod(Constructor))),
      ReflectConfigEntry(
        "akka.serialization.jackson.FiniteDurationDeserializer",
        methods = Seq(ReflectMethod(Constructor))),
      ReflectConfigEntry(
        "akka.serialization.jackson.TypedActorRefSerializer",
        methods = Seq(ReflectMethod(Constructor)),
        condition = Some(Condition(typeReachable = "akka.actor.typed.ActorRef"))),
      ReflectConfigEntry(
        "akka.serialization.jackson.TypedActorRefDeserializer",
        methods = Seq(ReflectMethod(Constructor)),
        condition = Some(Condition(typeReachable = "akka.actor.typed.ActorRef"))),
      ReflectConfigEntry(
        "akka.serialization.jackson.SourceRefSerializer",
        methods = Seq(ReflectMethod(Constructor)),
        condition = Some(Condition(typeReachable = "akka.stream.SourceRef"))),
      ReflectConfigEntry(
        "akka.serialization.jackson.SourceRefDeserializer",
        methods = Seq(ReflectMethod(Constructor)),
        condition = Some(Condition(typeReachable = "akka.stream.SourceRef"))),
      ReflectConfigEntry(
        "akka.serialization.jackson.SinkRefSerializer",
        methods = Seq(ReflectMethod(Constructor)),
        condition = Some(Condition(typeReachable = "akka.stream.SinkRef"))),
      ReflectConfigEntry(
        "akka.serialization.jackson.SinkRefDeserializer",
        methods = Seq(ReflectMethod(Constructor)),
        condition = Some(Condition(typeReachable = "akka.stream.SinkRef"))))

    jacksonModules ++ manualAdditions
  }

  val nativeImageUtils =
    new NativeImageUtils("akka-serialization-jackson", additionalEntries, Seq("akka.serialization.jackson"))

  // run this to regenerate metadata 'akka-serialization-jackson/Test/runMain akka.serialization.jackson.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-serialization-jackson" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
