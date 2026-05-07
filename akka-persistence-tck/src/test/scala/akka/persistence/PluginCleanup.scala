/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

trait PluginCleanup extends BeforeAndAfterAll { self: PluginSpec =>
  val storageLocations =
    List("akka.persistence.journal.leveldb.dir", "akka.persistence.snapshot-store.local.dir").map(s =>
      new File(system.settings.config.getString(s)))

  override def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    storageLocations.foreach(FileUtils.deleteDirectory)
  }
}
