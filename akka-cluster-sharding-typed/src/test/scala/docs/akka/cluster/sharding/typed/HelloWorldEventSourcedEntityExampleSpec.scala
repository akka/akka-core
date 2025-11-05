/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.sharding.typed

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object HelloWorldEventSourcedEntityExampleSpec {
  val config = ConfigFactory.parseString("""
      akka.actor.provider = cluster

      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1

      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.journal.inmem.test-serialization = on
    """)
}

class HelloWorldEventSourcedEntityExampleSpec
    extends ScalaTestWithActorTestKit(HelloWorldEventSourcedEntityExampleSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import HelloWorldPersistentEntityExample.HelloWorld
  import HelloWorldPersistentEntityExample.HelloWorld._

  val sharding = ClusterSharding(system)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).manager ! Join(Cluster(system).selfMember.address)

    sharding.init(Entity(HelloWorld.TypeKey) { entityContext =>
      HelloWorld(entityContext.entityId, PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId))
    })
  }

  "HelloWorld example" must {

    "sayHello" in {
      val probe = createTestProbe[Greeting]()
      val ref = ClusterSharding(system).entityRefFor(HelloWorld.TypeKey, "1")
      ref ! Greet("Alice")(probe.ref)
      probe.expectMessage(Greeting("Alice", 1))
      ref ! Greet("Bob")(probe.ref)
      probe.expectMessage(Greeting("Bob", 2))
    }

    "verifySerialization" in {
      val probe = createTestProbe[Greeting]()
      serializationTestKit.verifySerialization(Greet("Alice")(probe.ref))
      serializationTestKit.verifySerialization(Greeting("Alice", 1))
      serializationTestKit.verifySerialization(KnownPeople(Set.empty).add("Alice").add("Bob"))
    }

  }
}
