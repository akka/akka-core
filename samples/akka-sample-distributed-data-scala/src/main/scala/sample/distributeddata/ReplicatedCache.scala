package sample.distributeddata

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.{ Update, Get }

object ReplicatedCache {
  sealed trait Command
  final case class PutInCache(key: String, value: String) extends Command
  final case class GetFromCache(key: String, replyTo: ActorRef[Cached]) extends Command
  final case class Cached(key: String, value: Option[String])
  final case class Evict(key: String) extends Command
  private sealed trait InternalCommand extends Command
  private case class InternalGetResponse(key: String, replyTo: ActorRef[Cached], rsp: GetResponse[LWWMap[String, String]])
      extends InternalCommand
  private case class InternalUpdateResponse(rsp: UpdateResponse[LWWMap[String, String]]) extends InternalCommand

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    DistributedData.withReplicatorMessageAdapter[Command, LWWMap[String, String]] { replicator =>
      implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

      def dataKey(entryKey: String): LWWMapKey[String, String] =
        LWWMapKey("cache-" + math.abs(entryKey.hashCode % 100))

      Behaviors.receiveMessage[Command] {
        case PutInCache(key, value) =>
          replicator.askUpdate(
            askReplyTo => Update(dataKey(key), LWWMap.empty[String, String], WriteLocal, askReplyTo)(_ :+ (key -> value)),
            InternalUpdateResponse.apply)

          Behaviors.same

        case Evict(key) =>
          replicator.askUpdate(
            askReplyTo => Update(dataKey(key), LWWMap.empty[String, String], WriteLocal, askReplyTo)(_.remove(node, key)),
            InternalUpdateResponse.apply)

          Behaviors.same

        case GetFromCache(key, replyTo) =>
          replicator.askGet(
            askReplyTo => Get(dataKey(key), ReadLocal, askReplyTo),
            rsp => InternalGetResponse(key, replyTo, rsp))

          Behaviors.same

        case InternalGetResponse(key, replyTo, g @ GetSuccess(_, _)) =>
          replyTo ! Cached(key, g.dataValue.get(key))
          Behaviors.same

        case InternalGetResponse(key, replyTo, _: NotFound[_]) =>
          replyTo ! Cached(key, None)
          Behaviors.same

        case _: InternalGetResponse    => Behaviors.same // ok
        case _: InternalUpdateResponse => Behaviors.same // ok
      }
    }
  }
}
