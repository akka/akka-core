/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.persistence.typed

import java.util.UUID

import docs.akka.persistence.typed.ReplicatedShoppingCartExampleSpec.ShoppingCart.CartItems
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.query.scaladsl.PersistenceTestKitReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.crdt.Counter
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.serialization.jackson.CborSerializable

object ReplicatedShoppingCartExampleSpec {

  //#shopping-cart
  object ShoppingCart {

    type ProductId = String

    sealed trait Command extends CborSerializable
    final case class AddItem(id: ProductId, count: Int) extends Command
    final case class RemoveItem(id: ProductId, count: Int) extends Command
    final case class GetCartItems(replyTo: ActorRef[CartItems]) extends Command
    final case class CartItems(items: Map[ProductId, Int]) extends CborSerializable

    sealed trait Event extends CborSerializable
    final case class ItemUpdated(id: ProductId, update: Counter.Updated) extends Event

    final case class State(items: Map[ProductId, Counter])

    def apply(entityId: String, replicaId: ReplicaId, allReplicaIds: Set[ReplicaId]): Behavior[Command] = {
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("cart", entityId, replicaId),
        allReplicaIds,
        PersistenceTestKitReadJournal.Identifier) { replicationContext =>
        EventSourcedBehavior[Command, Event, State](
          replicationContext.persistenceId,
          State(Map.empty),
          (state, cmd) => commandHandler(state, cmd),
          (state, event) => eventHandler(state, event))
      }
    }

    private def commandHandler(state: State, cmd: Command): Effect[Event, State] = {
      cmd match {
        case AddItem(productId, count) =>
          Effect.persist(ItemUpdated(productId, Counter.Updated(count)))
        case RemoveItem(productId, count) =>
          Effect.persist(ItemUpdated(productId, Counter.Updated(-count)))
        case GetCartItems(replyTo) =>
          val items = state.items.collect {
            case (id, counter) if counter.value > 0 => id -> counter.value.toInt
          }
          replyTo ! CartItems(items)
          Effect.none
      }
    }

    private def eventHandler(state: State, event: Event): State = {
      event match {
        case ItemUpdated(id, update) =>
          val newItems = state.items.get(id) match {
            case Some(counter) => state.items + (id -> counter.applyOperation(update))
            case None          => state.items + (id -> Counter.empty.applyOperation(update))
          }
          State(newItems)
      }
    }
  }
  //#shopping-cart
}

class ReplicatedShoppingCartExampleSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ReplicatedShoppingCartExampleSpec.ShoppingCart

  "Replicated shopping cart" should {
    "work" in {
      val cartId = UUID.randomUUID().toString

      val refDcA: ActorRef[ShoppingCart.Command] =
        spawn(ShoppingCart(cartId, ReplicaId("DC-A"), Set(ReplicaId("DC-A"), ReplicaId("DC-B"))))

      val refDcB: ActorRef[ShoppingCart.Command] =
        spawn(ShoppingCart(cartId, ReplicaId("DC-B"), Set(ReplicaId("DC-A"), ReplicaId("DC-B"))))

      val fidgetSpinnerId = "T2912"
      val rubicsCubeId = "T1302"

      refDcA ! ShoppingCart.AddItem(fidgetSpinnerId, 10)
      refDcB ! ShoppingCart.AddItem(rubicsCubeId, 10)
      refDcA ! ShoppingCart.AddItem(rubicsCubeId, 10)
      refDcA ! ShoppingCart.AddItem(fidgetSpinnerId, 10)
      refDcB ! ShoppingCart.AddItem(fidgetSpinnerId, 10)
      refDcA ! ShoppingCart.RemoveItem(fidgetSpinnerId, 10)
      refDcA ! ShoppingCart.AddItem(rubicsCubeId, 10)
      refDcB ! ShoppingCart.RemoveItem(rubicsCubeId, 10)

      val replyProbe = createTestProbe[CartItems]()

      eventually {
        refDcA ! ShoppingCart.GetCartItems(replyProbe.ref)
        replyProbe.expectMessage(CartItems(Map(fidgetSpinnerId -> 20, rubicsCubeId -> 20)))
      }
    }
  }
}
