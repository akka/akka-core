/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.security.provider

/**
 * INTERNAL API
 * From AESCounterRNG API docs:
 * Valid values are 16 (128 bits), 24 (192 bits) and 32 (256 bits).
 * Any other values will result in an exception from the AES implementation.
 *
 * INTERNAL API
 */
private[provider] object SeedSize {
  val Seed128 = 16
  val Seed192 = 24
  val Seed256 = 32
}
