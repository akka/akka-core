/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package scala.docs.ddata

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.SelfUniqueAddress

object ShoppingCart {
  import akka.cluster.ddata.Replicator._

  def props(userId: String): Props = Props(new ShoppingCart(userId))

  case object GetCart
  final case class AddItem(item: LineItem)
  final case class RemoveItem(productId: String)

  final case class Cart(items: Set[LineItem])
  final case class LineItem(productId: String, title: String, quantity: Int)

  //#read-write-majority
  private val timeout = 3.seconds
  private val readMajority = ReadMajority(timeout)
  private val writeMajority = WriteMajority(timeout)
  //#read-write-majority

}

class ShoppingCart(userId: String) extends Actor {
  import ShoppingCart._
  import akka.cluster.ddata.Replicator._

  val replicator = DistributedData(context.system).replicator
  implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

  val DataKey = LWWMapKey[String, LineItem]("cart-" + userId)

  def receive =
    receiveGetCart
      .orElse[Any, Unit](receiveAddItem)
      .orElse[Any, Unit](receiveRemoveItem)
      .orElse[Any, Unit](receiveOther)

  //#get-cart
  def receiveGetCart: Receive = {
    case GetCart =>
      replicator ! Get(DataKey, readMajority, Some(sender()))

    case g @ GetSuccess(DataKey, Some(replyTo: ActorRef)) =>
      val data = g.get(DataKey)
      val cart = Cart(data.entries.values.toSet)
      replyTo ! cart

    case NotFound(DataKey, Some(replyTo: ActorRef)) =>
      replyTo ! Cart(Set.empty)

    case GetFailure(DataKey, Some(replyTo: ActorRef)) =>
      // ReadMajority failure, try again with local read
      replicator ! Get(DataKey, ReadLocal, Some(replyTo))
  }
  //#get-cart

  //#add-item
  def receiveAddItem: Receive = {
    case cmd @ AddItem(item) =>
      val update = Update(DataKey, LWWMap.empty[String, LineItem], writeMajority, Some(cmd)) { cart =>
        updateCart(cart, item)
      }
      replicator ! update
  }
  //#add-item

  def updateCart(data: LWWMap[String, LineItem], item: LineItem): LWWMap[String, LineItem] =
    data.get(item.productId) match {
      case Some(LineItem(_, _, existingQuantity)) =>
        data :+ (item.productId -> item.copy(quantity = existingQuantity + item.quantity))
      case None => data :+ (item.productId -> item)
    }

  //#remove-item
  def receiveRemoveItem: Receive = {
    case cmd @ RemoveItem(productId) =>
      // Try to fetch latest from a majority of nodes first, since ORMap
      // remove must have seen the item to be able to remove it.
      replicator ! Get(DataKey, readMajority, Some(cmd))

    case GetSuccess(DataKey, Some(RemoveItem(productId))) =>
      replicator ! Update(DataKey, LWWMap(), writeMajority, None) {
        _.remove(node, productId)
      }

    case GetFailure(DataKey, Some(RemoveItem(productId))) =>
      // ReadMajority failed, fall back to best effort local value
      replicator ! Update(DataKey, LWWMap(), writeMajority, None) {
        _.remove(node, productId)
      }

    case NotFound(DataKey, Some(RemoveItem(productId))) =>
    // nothing to remove
  }
  //#remove-item

  def receiveOther: Receive = {
    case _: UpdateSuccess[_] | _: UpdateTimeout[_] =>
    // UpdateTimeout, will eventually be replicated
    case e: UpdateFailure[_] => throw new IllegalStateException("Unexpected failure: " + e)
  }

}
