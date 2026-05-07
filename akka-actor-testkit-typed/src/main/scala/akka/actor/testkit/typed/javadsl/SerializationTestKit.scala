/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import akka.actor.testkit.typed.scaladsl
import akka.actor.typed.ActorSystem

/**
 * Utilities to test serialization.
 */
class SerializationTestKit(system: ActorSystem[_]) {

  private val delegate = new scaladsl.SerializationTestKit(system)

  /**
   * Verify serialization roundtrip.
   * Throws exception from serializer if `obj` can't be serialized and deserialized.
   *
   * @param obj the object to verify
   * @param assertEquality if `true` the deserialized  object is verified to be equal to `obj`,
   *                       and if not an `AssertionError` is thrown
   * @return the deserialized object
   */
  def verifySerialization[M](obj: M, assertEquality: Boolean): M =
    delegate.verifySerialization(obj, assertEquality)
}
