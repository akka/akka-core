/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.serialization

import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec

class MessageSerializerSpec extends AkkaSpec {

  "Message serializer" should {
    "serialize metadata for persistent repr" in {
      val pr = PersistentRepr("payload", 1L, "pid1").withMetadata("meta")
      val serialization = SerializationExtension(system)
      val deserialzied = serialization.deserialize(serialization.serialize(pr).get, classOf[PersistentRepr]).get
      deserialzied.metadata shouldEqual Some("meta")
    }
  }

}
