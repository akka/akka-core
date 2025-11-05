/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.pubsub.protobuf

import scala.collection.immutable.TreeMap

import akka.actor.{ Address, ExtendedActorSystem }
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator._
import akka.cluster.pubsub.DistributedPubSubMediator.Internal._
import akka.testkit.AkkaSpec

class DistributedPubSubMessageSerializerSpec extends AkkaSpec {

  val serializer = new DistributedPubSubMessageSerializer(system.asInstanceOf[ExtendedActorSystem])

  def checkSerialization(obj: AnyRef): Unit = {
    val blob = serializer.toBinary(obj)
    val ref = serializer.fromBinary(blob, serializer.manifest(obj))
    ref should ===(obj)
  }

  " DistributedPubSubMessages" must {

    "be serializable" in {
      val address1 = Address("akka", "system", "some.host.org", 4711)
      val address2 = Address("akka", "system", "other.host.org", 4711)
      val address3 = Address("akka", "system", "some.host.org", 4712)
      val u1 = system.actorOf(Props.empty, "u1")
      val u2 = system.actorOf(Props.empty, "u2")
      val u3 = system.actorOf(Props.empty, "u3")
      val u4 = system.actorOf(Props.empty, "u4")
      checkSerialization(Status(Map(address1 -> 3, address2 -> 17, address3 -> 5), isReplyToStatus = true))
      checkSerialization(
        Delta(List(
          Bucket(address1, 3, TreeMap("/user/u1" -> ValueHolder(2, Some(u1)), "/user/u2" -> ValueHolder(3, Some(u2)))),
          Bucket(address2, 17, TreeMap("/user/u3" -> ValueHolder(17, Some(u3)))),
          Bucket(address3, 5, TreeMap("/user/u4" -> ValueHolder(4, Some(u4)), "/user/u5" -> ValueHolder(5, None))))))
      checkSerialization(Send("/user/u3", "hello", localAffinity = true))
      checkSerialization(SendToAll("/user/u3", "hello", allButSelf = true))
      checkSerialization(Publish("mytopic", "hello"))
      checkSerialization(SendToOneSubscriber("hello"))
    }
  }
}
