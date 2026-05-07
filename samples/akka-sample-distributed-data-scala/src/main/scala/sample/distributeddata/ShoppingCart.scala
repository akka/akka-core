package sample.distributeddata

import scala.concurrent.duration._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.{ Update, Get }

object ShoppingCart {
  sealed trait Command
  final case class GetCart(replyTo: ActorRef[Cart]) extends Command
  final case class AddItem(item: LineItem) extends Command
  final case class RemoveItem(productId: String) extends Command

  final case class Cart(items: Set[LineItem])
  final case class LineItem(productId: String, title: String, quantity: Int)

  private sealed trait InternalCommand extends Command
  private case class InternalGetResponse(replyTo: ActorRef[Cart], rsp: GetResponse[LWWMap[String, LineItem]]) extends InternalCommand
  private case class InternalUpdateResponse[A <: ReplicatedData](rsp: UpdateResponse[A]) extends InternalCommand
  private case class InternalRemoveItem(productId: String, getResponse: GetResponse[LWWMap[String, LineItem]]) extends InternalCommand

  private val timeout = 3.seconds
  private val readMajority = ReadMajority(timeout)
  private val writeMajority = WriteMajority(timeout)

  def apply(userId: String): Behavior[Command] = Behaviors.setup { context =>
    DistributedData.withReplicatorMessageAdapter[Command, LWWMap[String, LineItem]] { replicator =>
      implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

      val DataKey = LWWMapKey[String, LineItem]("cart-" + userId)

      def behavior = Behaviors.receiveMessagePartial(
        receiveGetCart
            .orElse(receiveAddItem)
            .orElse(receiveRemoveItem)
            .orElse(receiveOther)
      )

      def receiveGetCart: PartialFunction[Command, Behavior[Command]] = {
        case GetCart(replyTo) =>
          replicator.askGet(
            askReplyTo => Get(DataKey, readMajority, askReplyTo),
            rsp => InternalGetResponse(replyTo, rsp))

          Behaviors.same

        case InternalGetResponse(replyTo, g @ GetSuccess(DataKey, _)) =>
          val data = g.get(DataKey)
          val cart = Cart(data.entries.values.toSet)
          replyTo ! cart
          Behaviors.same

        case InternalGetResponse(replyTo, NotFound(DataKey, _)) =>
          replyTo ! Cart(Set.empty)
          Behaviors.same

        case InternalGetResponse(replyTo, GetFailure(DataKey, _)) =>
          // ReadMajority failure, try again with local read
          replicator.askGet(
            askReplyTo => Get(DataKey, ReadLocal, askReplyTo),
            rsp => InternalGetResponse(replyTo, rsp))

          Behaviors.same
      }

      def receiveAddItem: PartialFunction[Command, Behavior[Command]] = {
        case AddItem(item) =>
          replicator.askUpdate(
            askReplyTo => Update(DataKey, LWWMap.empty[String, LineItem], writeMajority, askReplyTo) {
              cart => updateCart(cart, item)
            },
            InternalUpdateResponse.apply)

          Behaviors.same
      }

      def updateCart(data: LWWMap[String, LineItem], item: LineItem): LWWMap[String, LineItem] = {
        data.get(item.productId) match {
          case Some(LineItem(_, _, existingQuantity)) =>
            data :+ (item.productId -> item.copy(quantity = existingQuantity + item.quantity))
          case None => data :+ (item.productId -> item)
        }
      }

      def receiveRemoveItem: PartialFunction[Command, Behavior[Command]] = {
        case RemoveItem(productId) =>
          // Try to fetch latest from a majority of nodes first, since ORMap
          // remove must have seen the item to be able to remove it.
          replicator.askGet(
            askReplyTo => Get(DataKey, readMajority, askReplyTo),
            rsp => InternalRemoveItem(productId, rsp))

          Behaviors.same

        case InternalRemoveItem(productId, GetSuccess(DataKey, _)) =>
          removeItem(productId)
          Behaviors.same

        case InternalRemoveItem(productId, GetFailure(DataKey, _)) =>
          // ReadMajority failed, fall back to best effort local value
          removeItem(productId)
          Behaviors.same

        case InternalRemoveItem(_, NotFound(DataKey, _)) =>
          // nothing to remove
          Behaviors.same
      }

      def removeItem(productId: String): Unit = {
        replicator.askUpdate(
          askReplyTo => Update(DataKey, LWWMap.empty[String, LineItem], writeMajority, askReplyTo) {
            _.remove(node, productId)
          },
          InternalUpdateResponse.apply)
      }

      def receiveOther: PartialFunction[Command, Behavior[Command]] = {
        case InternalUpdateResponse(_: UpdateSuccess[_]) => Behaviors.same
        case InternalUpdateResponse(_: UpdateTimeout[_]) => Behaviors.same
        // UpdateTimeout, will eventually be replicated
        case InternalUpdateResponse(e: UpdateFailure[_]) => throw new IllegalStateException("Unexpected failure: " + e)
      }

      behavior
    }
  }
}
