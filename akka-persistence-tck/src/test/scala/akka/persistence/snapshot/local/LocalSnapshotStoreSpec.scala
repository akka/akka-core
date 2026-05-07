/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.snapshot.local

import com.typesafe.config.ConfigFactory

import akka.persistence.CapabilityFlag
import akka.persistence.PluginCleanup
import akka.persistence.snapshot.SnapshotStoreSpec

class LocalSnapshotStoreSpec
    extends SnapshotStoreSpec(
      config =
        ConfigFactory.parseString("""
    akka.test.timefactor = 3
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/snapshots"
    """))
    with PluginCleanup {

  override protected def supportsSerialization: CapabilityFlag = CapabilityFlag.on()
}
