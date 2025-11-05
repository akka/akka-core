/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object IdleSpec {
  val config: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        default-idle-strategy.idle-entity.timeout = 1s
      }
    }
    """).withFallback(EntityPassivationSpec.config)
}

class IdleSpec extends AbstractEntityPassivationSpec(IdleSpec.config, expectedEntities = 2) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of idle entities" must {
    "passivate entities when they haven't seen messages for the configured duration" in {
      val region = start()

      val lastSendNanoTime1 = clock.currentTime()
      region ! Envelope(shard = 1, id = 1, message = "A")
      region ! Envelope(shard = 2, id = 2, message = "B")
      Thread.sleep((configuredIdleTimeout / 2).toMillis)
      region ! Envelope(shard = 2, id = 2, message = "C")
      Thread.sleep((configuredIdleTimeout / 2).toMillis)
      region ! Envelope(shard = 2, id = 2, message = "D")
      Thread.sleep((configuredIdleTimeout / 2).toMillis)
      val lastSendNanoTime2 = clock.currentTime()
      region ! Envelope(shard = 2, id = 2, message = "E")

      expectReceived(id = 1, message = "A")
      expectReceived(id = 2, message = "B")
      expectReceived(id = 2, message = "C")
      expectReceived(id = 2, message = "D")
      expectReceived(id = 2, message = "E")
      val passivate1 = expectReceived(id = 1, message = Stop)
      val passivate2 = expectReceived(id = 2, message = Stop, within = configuredIdleTimeout * 2)

      // note: touched timestamps are when the shard receives the message, not the entity itself
      // so look at the time from before sending the last message until receiving the passivate message
      (passivate1.nanoTime - lastSendNanoTime1).nanos should be > configuredIdleTimeout
      (passivate2.nanoTime - lastSendNanoTime2).nanos should be > configuredIdleTimeout

      // entities can be re-activated
      region ! Envelope(shard = 1, id = 1, message = "X")
      region ! Envelope(shard = 2, id = 2, message = "Y")
      region ! Envelope(shard = 1, id = 1, message = "Z")

      expectReceived(id = 1, message = "X")
      expectReceived(id = 2, message = "Y")
      expectReceived(id = 1, message = "Z")
      expectReceived(id = 1, message = Stop, within = configuredIdleTimeout * 2)
      expectReceived(id = 2, message = Stop, within = configuredIdleTimeout * 2)
    }
  }
}
