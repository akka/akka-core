/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.testkit

import akka.actor.typed.ActorSystem
import akka.persistence.testkit.{ PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit, SnapshotTestKit }
import com.typesafe.config.ConfigFactory

object TestKitTypedConf {

  //#testkit-typed-conf

  val yourConfiguration = ConfigFactory.defaultApplication()

  val system =
    ActorSystem(??? /*some behavior*/, "test-system", PersistenceTestKitPlugin.config.withFallback(yourConfiguration))

  val testKit = PersistenceTestKit(system)

  //#testkit-typed-conf

}

object SnapshotTypedConf {

  //#snapshot-typed-conf

  val yourConfiguration = ConfigFactory.defaultApplication()

  val system = ActorSystem(
    ??? /*some behavior*/,
    "test-system",
    PersistenceTestKitSnapshotPlugin.config.withFallback(yourConfiguration))

  val testKit = SnapshotTestKit(system)

  //#snapshot-typed-conf

}
