/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.temporal.ChronoUnit

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.internal.adapter.ActorSystemAdapter
import akka.cluster.sharding.typed.internal.ShardedDaemonProcessCoordinator
import akka.cluster.sharding.typed.internal.ShardedDaemonProcessState
import akka.cluster.sharding.typed.internal.ShardingSerializer
import akka.serialization.SerializationExtension

class ShardingSerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "The typed ShardingSerializer" must {

    val serialization = SerializationExtension(ActorSystemAdapter.toClassic(system))
    val probe = createTestProbe[AnyRef]()

    def checkSerialization(obj: AnyRef): Unit = {
      serialization.findSerializerFor(obj) match {
        case serializer: ShardingSerializer =>
          val blob = serializer.toBinary(obj)
          val ref = serializer.fromBinary(blob, serializer.manifest(obj))
          ref should ===(obj)

          val buffer = ByteBuffer.allocate(128)
          buffer.order(ByteOrder.LITTLE_ENDIAN)
          serializer.toBinary(obj, buffer)
          buffer.flip()
          val refFromBuf = serializer.fromBinary(buffer, serializer.manifest(obj))
          refFromBuf should ===(obj)
          buffer.clear()

        case s =>
          throw new IllegalStateException(s"Wrong serializer ${s.getClass} for ${obj.getClass}")
      }
    }

    "must serialize and deserialize ShardingEnvelope" in {
      checkSerialization(ShardingEnvelope("abc", 42))
    }

    "must serialize and deserialize StartEntity" in {
      checkSerialization(scaladsl.StartEntity[Int]("abc"))
      checkSerialization(javadsl.StartEntity.create(classOf[java.lang.Integer], "def"))
    }

    "must serialize and deserialize ShardedDaemonProcessCoordinator.ScaleState" in {
      checkSerialization(ShardedDaemonProcessState(2, 3, true, Instant.now().truncatedTo(ChronoUnit.MILLIS)))
    }

    "must serialize and deserialize ChangeNumberOfProcesses" in {
      checkSerialization(ChangeNumberOfProcesses(7, probe.ref))
    }

    "must serialize and deserialize GetNumberOfProcesses" in {
      checkSerialization(GetNumberOfProcesses(probe.ref))
    }

    "must serialize and deserialize GetNumberOfProcessesReply" in {
      checkSerialization(
        ShardedDaemonProcessCoordinator
          .GetNumberOfProcessesReply(8, Instant.now().truncatedTo(ChronoUnit.MILLIS), false, 4L))
    }

  }
}
