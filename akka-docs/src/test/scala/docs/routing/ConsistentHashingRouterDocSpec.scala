/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.routing

import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.actor.ActorRef

object ConsistentHashingRouterDocSpec {

  //#cache-actor
  import akka.actor.Actor
  import akka.routing.ConsistentHashingRouter.ConsistentHashable

  class Cache extends Actor {
    var cache = Map.empty[String, String]

    def receive = {
      case Entry(key, value) => cache += (key -> value)
      case Get(key)          => sender() ! cache.get(key)
      case Evict(key)        => cache -= key
    }
  }

  final case class Evict(key: String)

  final case class Get(key: String) extends ConsistentHashable {
    override def consistentHashKey: Any = key
  }

  final case class Entry(key: String, value: String)
  //#cache-actor

}

class ConsistentHashingRouterDocSpec extends AkkaSpec with ImplicitSender {

  import ConsistentHashingRouterDocSpec._

  "demonstrate usage of ConsistentHashableRouter" in {

    def context = system

    //#consistent-hashing-router
    import akka.actor.Props
    import akka.routing.ConsistentHashingPool
    import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
    import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope

    def hashMapping: ConsistentHashMapping = {
      case Evict(key) => key
    }

    val cache: ActorRef =
      context.actorOf(ConsistentHashingPool(10, hashMapping = hashMapping).props(Props[Cache]()), name = "cache")

    cache ! ConsistentHashableEnvelope(message = Entry("hello", "HELLO"), hashKey = "hello")
    cache ! ConsistentHashableEnvelope(message = Entry("hi", "HI"), hashKey = "hi")

    cache ! Get("hello")
    expectMsg(Some("HELLO"))

    cache ! Get("hi")
    expectMsg(Some("HI"))

    cache ! Evict("hi")
    cache ! Get("hi")
    expectMsg(None)

    //#consistent-hashing-router

  }

}
