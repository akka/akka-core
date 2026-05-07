/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization.jackson

/**
 * Predefined marker trait for serialization with Jackson JSON.
 * Enabled in reference.conf `akka.actor.serialization-bindings` (via application.conf).
 */
trait JsonSerializable

/**
 * Predefined marker trait for serialization with Jackson CBOR.
 * Enabled in reference.conf `akka.actor.serialization-bindings` (via application.conf).
 */
trait CborSerializable
