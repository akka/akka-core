package sample.distributeddata

import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.GetReplicaCount
import akka.cluster.ddata.typed.scaladsl.Replicator.ReplicaCount
import akka.cluster.typed.{ Cluster, Join }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import com.typesafe.config.ConfigFactory

object ShoppingCartSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    akka.actor.serialization-bindings {
      "sample.distributeddata.ShoppingCart$LineItem" = jackson-cbor
    }
    """))

}

class ShoppingCartSpecMultiJvmNode1 extends ShoppingCartSpec
class ShoppingCartSpecMultiJvmNode2 extends ShoppingCartSpec
class ShoppingCartSpecMultiJvmNode3 extends ShoppingCartSpec

class ShoppingCartSpec extends MultiNodeSpec(ShoppingCartSpec) with STMultiNodeSpec {
  import ShoppingCartSpec._
  import ShoppingCart._

  override def initialParticipants = roles.size

  implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
  val cluster = Cluster(typedSystem)
  val shoppingCart = system.spawnAnonymous(ShoppingCart("user-1"))

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.manager ! Join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Demo of a replicated shopping cart" must {
    "join cluster" in within(20.seconds) {
      join(node1, node1)
      join(node2, node1)
      join(node3, node1)

      awaitAssert {
        val probe = TestProbe[ReplicaCount]()
        DistributedData(typedSystem).replicator ! GetReplicaCount(probe.ref)
        probe.expectMessage(Replicator.ReplicaCount(roles.size))
      }
      enterBarrier("after-1")
    }

    "handle updates directly after start" in within(15.seconds) {
      runOn(node2) {
        shoppingCart ! ShoppingCart.AddItem(LineItem("1", "Apples", quantity = 2))
        shoppingCart ! ShoppingCart.AddItem(LineItem("2", "Oranges", quantity = 3))
      }
      enterBarrier("updates-done")

      awaitAssert {
        val probe = TestProbe[Cart]()
        shoppingCart ! ShoppingCart.GetCart(probe.ref)
        val cart = probe.expectMessageType[Cart]
        cart.items should be(Set(LineItem("1", "Apples", quantity = 2), LineItem("2", "Oranges", quantity = 3)))
      }

      enterBarrier("after-2")
    }

    "handle updates from different nodes" in within(5.seconds) {
      runOn(node2) {
        shoppingCart ! ShoppingCart.AddItem(LineItem("1", "Apples", quantity = 5))
        shoppingCart ! ShoppingCart.RemoveItem("2")
      }
      runOn(node3) {
        shoppingCart ! ShoppingCart.AddItem(LineItem("3", "Bananas", quantity = 4))
      }
      enterBarrier("updates-done")

      awaitAssert {
        val probe = TestProbe[Cart]()
        shoppingCart ! ShoppingCart.GetCart(probe.ref)
        val cart = probe.expectMessageType[Cart]
        cart.items should be(Set(LineItem("1", "Apples", quantity = 7), LineItem("3", "Bananas", quantity = 4)))
      }

      enterBarrier("after-3")
    }

  }

}

