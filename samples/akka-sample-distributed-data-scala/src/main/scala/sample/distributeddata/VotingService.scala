package sample.distributeddata

import scala.concurrent.duration._
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.Flag
import akka.cluster.ddata.FlagKey
import akka.cluster.ddata.PNCounterMap
import akka.cluster.ddata.PNCounterMapKey
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.{ Update, Get }

object VotingService {
  sealed trait Command
  case object Open extends Command
  case object Close extends Command
  final case class Vote(participant: String) extends Command
  final case class GetVotes(replyTo: ActorRef[Votes]) extends Command

  final case class Votes(result: Map[String, BigInt], open: Boolean)

  private sealed trait InternalCommand extends Command
  private case class InternalSubscribeResponse(chg: SubscribeResponse[Flag]) extends InternalCommand
  private case class InternalUpdateResponse[A <: ReplicatedData](rsp: UpdateResponse[A]) extends InternalCommand
  private case class InternalGetResponse(replyTo: ActorRef[Votes], rsp: GetResponse[PNCounterMap[String]]) extends InternalCommand

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    DistributedData.withReplicatorMessageAdapter[Command, Flag] { replicatorFlag =>
      DistributedData.withReplicatorMessageAdapter[Command, PNCounterMap[String]] { replicatorCounters =>

        implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

        val OpenedKey = FlagKey("contestOpened")
        val ClosedKey = FlagKey("contestClosed")
        val CountersKey = PNCounterMapKey[String]("contestCounters")

        replicatorFlag.subscribe(OpenedKey, InternalSubscribeResponse.apply)

        def start = Behaviors.receiveMessagePartial[Command] {
          case Open =>
            replicatorFlag.askUpdate(
              askReplyTo => Update(OpenedKey, Flag(), WriteAll(5.seconds), askReplyTo)(_.switchOn),
              InternalUpdateResponse.apply)

            becomeOpen()

          case InternalSubscribeResponse(c @ Changed(OpenedKey)) if c.get(OpenedKey).enabled =>
            becomeOpen()

          case GetVotes(replyTo) =>
            replyTo ! Votes(Map.empty, open = false)
            Behaviors.same
        }

        def becomeOpen() = {
          replicatorFlag.unsubscribe(OpenedKey)
          replicatorFlag.subscribe(ClosedKey, InternalSubscribeResponse.apply)
          Behaviors.receiveMessagePartial(open orElse getVotes(open = true))
        }

        def open: PartialFunction[Command, Behavior[Command]] = {
          case Vote(participant) =>
            replicatorCounters.askUpdate(
              askReplyTo => Update(CountersKey, PNCounterMap[String](), WriteLocal, askReplyTo)(_.incrementBy(participant, 1)),
              InternalUpdateResponse.apply)

            Behaviors.same

          case InternalUpdateResponse(_: UpdateSuccess[_]) => Behaviors.same

          case Close =>
            replicatorFlag.askUpdate(
              askReplyTo => Update(ClosedKey, Flag(), WriteAll(5.seconds), askReplyTo)(_.switchOn),
              InternalUpdateResponse.apply)

            Behaviors.receiveMessagePartial(getVotes(open = false))

          case InternalSubscribeResponse(c @ Changed(ClosedKey)) if c.get(ClosedKey).enabled =>
            Behaviors.receiveMessagePartial(getVotes(open = false))

          case InternalSubscribeResponse(Changed(OpenedKey)) => Behaviors.same
        }

        def getVotes(open: Boolean): PartialFunction[Command, Behavior[Command]] = {
          case GetVotes(replyTo) =>
            replicatorCounters.askGet(
              askReplyTo => Get(CountersKey, ReadAll(3.seconds), askReplyTo),
              rsp => InternalGetResponse(replyTo, rsp))

            Behaviors.same

          case InternalGetResponse(replyTo, g @ GetSuccess(CountersKey, _)) =>
            val data = g.get(CountersKey)
            replyTo ! Votes(data.entries, open)
            Behaviors.same

          case InternalGetResponse(replyTo, NotFound(CountersKey, _)) =>
            replyTo ! Votes(Map.empty, open)
            Behaviors.same

          case InternalGetResponse(_, _: GetFailure[_])    => Behaviors.same
          case InternalUpdateResponse(_: UpdateSuccess[_]) => Behaviors.same
        }

        start
      }
    }
  }
}
