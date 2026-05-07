/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import akka.actor.ActorSystem
import akka.actor.DynamicAccess
import akka.actor.LocalActorRefProvider
import akka.actor.Props
import akka.event.EventStream
import akka.routing.RouterConfig
import akka.testkit.internal.NativeImageUtils
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectField
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // dungeon or dungeon worthy unsafe trixery
    // FIXME ugh, maybe we could reflect generate it instead of manually listing?
    ReflectConfigEntry(
      classOf[sun.misc.Unsafe].getName,
      methods = Seq(
        ReflectMethod("arrayBaseOff", parameterTypes = Seq("java.lang.Class")),
        ReflectMethod("arrayIndexScale", parameterTypes = Seq("java.lang.Class")),
        ReflectMethod("copyMemory", parameterTypes = Seq("long", "long", "long")),
        ReflectMethod(
          "copyMemory",
          parameterTypes = Seq("java.lang.Object", "long", "java.lang.Object", "long", "long")),
        ReflectMethod("getAndAddLong", parameterTypes = Seq("java.lang.Object", "long", "long")),
        ReflectMethod("getAndSetObject", parameterTypes = Seq("java.lang.Object", "long", "java.lang.Object")),
        ReflectMethod("getBoolean", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("getByte", parameterTypes = Seq("long")),
        ReflectMethod("getByte", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("getDouble", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("getFloat", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("getInt", parameterTypes = Seq("long")),
        ReflectMethod("getInt", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("getLong", parameterTypes = Seq("long")),
        ReflectMethod("getLong", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("getObject", parameterTypes = Seq("java.lang.Object", "long")),
        ReflectMethod("invokeCleaner", parameterTypes = Seq("java.nio.ByteBuffer")),
        ReflectMethod("objectFieldOffset", parameterTypes = Seq("java.lang.reflect.Field")),
        ReflectMethod("putBoolean", parameterTypes = Seq("java.lang.Object", "long", "boolean")),
        ReflectMethod("putByte", parameterTypes = Seq("long", "byte")),
        ReflectMethod("putByte", parameterTypes = Seq("java.lang.Object", "long", "byte")),
        ReflectMethod("putDouble", parameterTypes = Seq("java.lang.Object", "long", "double")),
        ReflectMethod("putFloat", parameterTypes = Seq("java.lang.Object", "long", "float")),
        ReflectMethod("putInt", parameterTypes = Seq("long", "int")),
        ReflectMethod("putInt", parameterTypes = Seq("java.lang.Object", "long", "int")),
        ReflectMethod("putLong", parameterTypes = Seq("long", "long")),
        ReflectMethod("putLong", parameterTypes = Seq("java.lang.Object", "long", "long")),
        ReflectMethod("putObject", parameterTypes = Seq("java.lang.Object", "long", "java.lang.Object")),
        ReflectMethod("storeFence", parameterTypes = Seq())),
      allDeclaredFields = true),
    // FIXME these are mostly "static" trixery, I wonder if graal can't figure them out itself without explicit listing
    ReflectConfigEntry(
      classOf[akka.actor.ActorCell].getName,
      fields = Seq(
        ReflectField("akka$actor$dungeon$Children$$_childrenRefsDoNotCallMeDirectly"),
        ReflectField("akka$actor$dungeon$Children$$_functionRefsDoNotCallMeDirectly"),
        ReflectField("akka$actor$dungeon$Children$$_nextNameDoNotCallMeDirectly"),
        ReflectField("akka$actor$dungeon$Dispatch$$_mailboxDoNotCallMeDirectly"))),
    ReflectConfigEntry(
      classOf[akka.actor.RepointableRef].getName,
      fields = Seq(ReflectField("_cellDoNotCallMeDirectly"), ReflectField("_lookupDoNotCallMeDirectly"))),
    ReflectConfigEntry(
      classOf[akka.pattern.CircuitBreaker].getName,
      fields =
        Seq(ReflectField("_currentResetTimeoutDoNotCallMeDirectly"), ReflectField("_currentStateDoNotCallMeDirectly"))),
    ReflectConfigEntry(
      classOf[akka.pattern.PromiseActorRef].getName,
      fields = Seq(ReflectField("_stateDoNotCallMeDirectly"), ReflectField("_watchedByDoNotCallMeDirectly"))),
    ReflectConfigEntry("akka.actor.LightArrayRevolverScheduler$TaskHolder", fields = Seq(ReflectField("task"))),
    // loaded via config
    ReflectConfigEntry(
      "akka.actor.LocalActorRefProvider$Guardian",
      queryAllDeclaredConstructors = true,
      methods = Seq(ReflectMethod(Constructor, Seq("akka.actor.SupervisorStrategy")))),
    ReflectConfigEntry(
      "akka.actor.LocalActorRefProvider$SystemGuardian",
      queryAllDeclaredConstructors = true,
      methods = Seq(ReflectMethod(Constructor, Seq("akka.actor.SupervisorStrategy", "akka.actor.ActorRef")))),
    ReflectConfigEntry(
      classOf[LocalActorRefProvider].getName,
      methods = Seq(
        ReflectMethod(
          Constructor,
          Seq(
            classOf[java.lang.String].getName,
            classOf[ActorSystem.Settings].getName,
            classOf[EventStream].getName,
            classOf[DynamicAccess].getName)))),
    // left as reflection based to not break akka-remote remote deploy test
    ReflectConfigEntry(classOf[Props.EmptyActor].getName, methods = Seq(ReflectMethod(Constructor))),
    // affinity pool pluggable things
    ReflectConfigEntry(
      "akka.dispatch.affinity.ThrowOnOverflowRejectionHandler",
      methods = Seq(ReflectMethod(Constructor, Seq.empty))),
    ReflectConfigEntry(
      "akka.dispatch.affinity.FairDistributionHashCache",
      methods = Seq(ReflectMethod(Constructor, Seq("com.typesafe.config.Config")))),
    // logging infra
    ReflectConfigEntry(
      classOf[akka.event.Logging.DefaultLogger].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor))),
    ReflectConfigEntry(
      classOf[akka.event.DefaultLoggingFilter].getName,
      methods = Seq(
        ReflectMethod(
          NativeImageUtils.Constructor,
          parameterTypes = Seq("akka.actor.ActorSystem$Settings", "akka.event.EventStream")))),
    // akka io stuff
    ReflectConfigEntry(
      classOf[akka.io.InetAddressDnsProvider].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor))),
    // FIXME remove these DNS related entries when removing the deprecated pluggable DNS setup and switch to non-reflective construction
    // pluggable through deprecated InetAddressDnsProvider
    ReflectConfigEntry(
      classOf[akka.io.SimpleDnsManager].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor, parameterTypes = Seq("akka.io.DnsExt"))),
      queryAllDeclaredConstructors = true),
    ReflectConfigEntry(
      classOf[akka.io.dns.internal.AsyncDnsProvider].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor, parameterTypes = Seq("akka.io.DnsExt"))),
      queryAllDeclaredConstructors = true),
    // pluggable through deprecated InetAddressDnsProvider.actorClass
    ReflectConfigEntry(
      classOf[akka.io.InetAddressDnsResolver].getName,
      methods = Seq(
        ReflectMethod(
          NativeImageUtils.Constructor,
          parameterTypes = Seq("akka.io.SimpleDnsCache", "com.typesafe.config.Config"))),
      queryAllDeclaredConstructors = true),
    ReflectConfigEntry(
      "akka.io.dns.internal.AsyncDnsResolver",
      methods = Seq(
        ReflectMethod(
          NativeImageUtils.Constructor,
          parameterTypes = Seq(
            "akka.io.SimpleDnsCache",
            "com.typesafe.config.Config",
            "scala.Function2<akka.actor.ActorRefFactory,scala.collection.immutable.List<java.net.InetSocketAddress>,scala.collection.immutable.List<akka.actor.ActorRef>>"))),
      queryAllDeclaredConstructors = true),
    // pluggable through deprecated InetAddressDnsProvider.managerClass
    ReflectConfigEntry(
      classOf[akka.io.SimpleDnsManager].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor, parameterTypes = Seq("akka.io.DnsExt"))),
      queryAllDeclaredConstructors = true),
    ReflectConfigEntry(
      "akka.io.dns.internal.AsyncDnsManager",
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor, parameterTypes = Seq("akka.io.DnsExt"))),
      queryAllDeclaredConstructors = true),
    // Internal Routing infra
    ReflectConfigEntry(
      classOf[akka.routing.RoutedActorCell.RouterActorCreator].getName,
      methods = Seq(ReflectMethod(NativeImageUtils.Constructor, parameterTypes = Seq(classOf[RouterConfig].getName)))),
    // we use reflective access to this to trigger our own JFR reporting
    ReflectConfigEntry("jdk.jfr.Recording", methods = Seq(ReflectMethod(NativeImageUtils.Constructor))))

  val modulePackages = Seq(
    "akka.actor",
    "akka.dispatch",
    "akka.event",
    "akka.io",
    "akka.japi",
    "akka.pattern",
    "akka.routing",
    "akka.serialization",
    "akka.util")

  val nativeImageUtils = new NativeImageUtils("akka-actor", additionalEntries, modulePackages)

  // run this to regenerate metadata 'akka-actor-tests/Test/runMain akka.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-actor" should {

    "be up to date" in {
      val (existing, current) =
        nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
