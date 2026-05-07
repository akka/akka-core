/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.persistence.journal.PersistencePluginProxy
import akka.persistence.journal.inmem.InmemJournal
import akka.persistence.journal.leveldb.LeveldbStore
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.persistence.snapshot.NoSnapshotStore
import akka.persistence.snapshot.local.LocalSnapshotStore
import akka.testkit.internal.NativeImageUtils
import akka.testkit.internal.NativeImageUtils.Constructor
import akka.testkit.internal.NativeImageUtils.ReflectConfigEntry
import akka.testkit.internal.NativeImageUtils.ReflectMethod
import com.typesafe.config.Config
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object NativeImageMetadataSpec {

  val additionalEntries = Seq(
    // akka.persistence.internal-stash-overflow-strategy possible "configurators"
    ReflectConfigEntry(classOf[ThrowExceptionConfigurator].getName, methods = Seq(ReflectMethod(Constructor))),
    ReflectConfigEntry(classOf[DiscardConfigurator].getName, methods = Seq(ReflectMethod(Constructor))),
    // akka.persistence.no-snapshot-store.class
    ReflectConfigEntry(classOf[NoSnapshotStore].getName, methods = Seq(ReflectMethod(Constructor))),
    // akka.persistence.journal.inmem.class
    ReflectConfigEntry(
      classOf[InmemJournal].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName)))),
    // akka.persistence.snapshot-store.local.class
    ReflectConfigEntry(
      classOf[LocalSnapshotStore].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName)))),
    // akka.persistence.journal.leveldb.class
    ReflectConfigEntry(
      classOf[LeveldbStore].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName)))),
    // akka.persistence.journal.leveldb-shared.class
    ReflectConfigEntry(
      classOf[SharedLeveldbJournal].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName)))),
    // akka.persistence.journal.proxy.class and akka.persistence.snapshot-store.proxy.class
    ReflectConfigEntry(
      classOf[PersistencePluginProxy].getName,
      methods = Seq(ReflectMethod(Constructor, parameterTypes = Seq(classOf[Config].getName)))))

  val nativeImageUtils = new NativeImageUtils("akka-persistence", additionalEntries, Seq("akka.persistence"))

  // run this to regenerate metadata 'akka-persistence/Test/runMain akka.persistence.NativeImageMetadataSpec'
  def main(args: Array[String]): Unit = {
    nativeImageUtils.writeMetadata()
  }
}

class NativeImageMetadataSpec extends AnyWordSpec with Matchers {
  import NativeImageMetadataSpec._

  "Native-image metadata for akka-persistence" should {

    "be up to date" in {
      val (existing, current) = nativeImageUtils.verifyMetadata()
      existing should ===(current)
    }
  }

}
