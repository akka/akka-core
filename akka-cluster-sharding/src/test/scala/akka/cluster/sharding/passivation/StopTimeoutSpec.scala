/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object StopTimeoutSpec {
  val config: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        stop-timeout = 100ms
        default-idle-strategy.idle-entity.timeout = 1s
      }
    }
    """).withFallback(EntityPassivationSpec.config)
}

class StopTimeoutSpec extends AbstractEntityPassivationSpec(StopTimeoutSpec.config, expectedEntities = 1) {

  import akka.cluster.sharding.passivation.EntityPassivationSpec.Entity
  import EntityPassivationSpec.Entity.Envelope

  "Passivation timeout" must {
    "stop entity if it doesn't terminate from Stop message" in {
      val region = start(Entity.IgnoreStop)

      region ! Envelope(shard = 1, id = 1, message = "A")
      expectReceived(id = 1, message = "A")
      expectReceived(id = 1, message = Entity.IgnoreStop, within = configuredIdleTimeout * 2)
      region ! Envelope(shard = 1, id = 1, message = "B")
      expectReceived(id = 1, message = "B")

      // stop it, otherwise TestKit termination will time out
      region ! Envelope(shard = 1, id = 1, message = Entity.Stop)
    }
  }
}
