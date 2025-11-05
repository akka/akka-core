/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.scaladsl

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializationExtension
import akka.serialization.Serializers

/**
 * Utilities to test serialization.
 */
class SerializationTestKit(system: ActorSystem[_]) {

  private val serialization = SerializationExtension(system.toClassic)

  /**
   * Verify serialization roundtrip.
   * Throws exception from serializer if `obj` can't be serialized and deserialized.
   * Also tests that the deserialized  object is equal to `obj`, and if not an
   * `AssertionError` is thrown.
   *
   * @param obj the object to verify
   * @return the deserialized object
   */
  def verifySerialization[M](obj: M): M =
    verifySerialization(obj, assertEquality = true)

  /**
   * Verify serialization roundtrip.
   * Throws exception from serializer if `obj` can't be serialized and deserialized.
   *
   * @param obj the object to verify
   * @param assertEquality if `true` the deserialized  object is verified to be equal to `obj`,
   *                       and if not an `AssertionError` is thrown
   * @return the deserialized object
   */
  def verifySerialization[M](obj: M, assertEquality: Boolean): M = {
    val result = roundtrip(obj)
    if (assertEquality && result != obj)
      throw new AssertionError(s"Serialization verification expected $obj, but was $result")
    result
  }

  /**
   * Verify serialization roundtrip.
   * Throws exception from serializer if `obj` can't be serialized and deserialized.
   *
   * @return the deserialized object
   */
  private def roundtrip[M](obj: M): M = {
    val objAnyRef = obj.asInstanceOf[AnyRef]
    val bytes = serialization.serialize(objAnyRef).get
    val serializer = serialization.findSerializerFor(objAnyRef)
    val manifest = Serializers.manifestFor(serializer, objAnyRef)
    serialization.deserialize(bytes, serializer.identifier, manifest).get.asInstanceOf[M]
  }
}
