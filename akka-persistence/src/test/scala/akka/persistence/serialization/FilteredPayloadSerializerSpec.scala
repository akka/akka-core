/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.serialization

import akka.persistence.FilteredPayload
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.testkit.AkkaSpec

class FilteredPayloadSerializerSpec extends AkkaSpec {

  "FilteredPayload serializer" should {
    "serialize FilteredPayload to zero-byte array" in {
      val serialization = SerializationExtension(system)
      val serializer = serialization.findSerializerFor(FilteredPayload).asInstanceOf[SerializerWithStringManifest]
      val manifest = serializer.manifest(FilteredPayload)
      val serialized = serializer.toBinary(FilteredPayload)
      serialized should have(size(0))
      serializer.fromBinary(serialized, manifest) should be(FilteredPayload)
    }
  }

}
