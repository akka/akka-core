/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.internal

import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Terminated
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.cluster.{ ddata => dd }
import akka.cluster.ddata.ReplicatedData
import akka.pattern.ask
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ReplicatorBehavior {
  import akka.cluster.ddata.typed.javadsl.{ Replicator => JReplicator }
  import akka.cluster.ddata.typed.scaladsl.{ Replicator => SReplicator }

  private case class InternalSubscribeResponse[A <: ReplicatedData](
      chg: dd.Replicator.SubscribeResponse[A],
      subscriber: ActorRef[JReplicator.SubscribeResponse[A]])
      extends JReplicator.Command

  val localAskTimeout = 60.seconds // ReadLocal, WriteLocal shouldn't timeout
  val additionalAskTimeout = 1.second

  def apply(
      settings: dd.ReplicatorSettings,
      underlyingReplicator: Option[akka.actor.ActorRef]): Behavior[SReplicator.Command] = {

    Behaviors.setup { ctx =>
      val classicReplicator = underlyingReplicator match {
        case Some(ref) => ref
        case None      =>
          // FIXME perhaps add supervisor for restarting, see PR https://github.com/akka/akka-core/pull/25988
          val classicReplicatorProps = dd.Replicator.props(settings)
          ctx.actorOf(classicReplicatorProps, name = "underlying")
      }

      def withState(
          subscribeAdapters: Map[
            ActorRef[JReplicator.SubscribeResponse[ReplicatedData]],
            ActorRef[dd.Replicator.SubscribeResponse[ReplicatedData]]]): Behavior[SReplicator.Command] = {

        def stopSubscribeAdapter(
            subscriber: ActorRef[JReplicator.SubscribeResponse[ReplicatedData]]): Behavior[SReplicator.Command] = {
          subscribeAdapters.get(subscriber) match {
            case Some(adapter) =>
              // will be unsubscribed from classicReplicator via Terminated
              ctx.stop(adapter)
              withState(subscribeAdapters - subscriber)
            case None => // already unsubscribed or terminated
              Behaviors.same
          }
        }

        Behaviors
          .receive[SReplicator.Command] { (ctx, msg) =>
            msg match {
              case cmd: SReplicator.Get[_] =>
                classicReplicator.tell(dd.Replicator.Get(cmd.key, cmd.consistency), sender = cmd.replyTo.toClassic)
                Behaviors.same

              case cmd: JReplicator.Get[d] =>
                implicit val timeout: Timeout = Timeout(cmd.consistency.timeout match {
                  case java.time.Duration.ZERO => localAskTimeout
                  case t                       => t.toScala + additionalAskTimeout
                })
                import ctx.executionContext
                val reply =
                  (classicReplicator ? dd.Replicator.Get(cmd.key, cmd.consistency.toClassic))
                    .mapTo[dd.Replicator.GetResponse[d]]
                    .map {
                      case rsp: dd.Replicator.GetSuccess[d] =>
                        JReplicator.GetSuccess(rsp.key)(rsp.dataValue)
                      case rsp: dd.Replicator.NotFound[d]       => JReplicator.NotFound(rsp.key)
                      case rsp: dd.Replicator.GetFailure[d]     => JReplicator.GetFailure(rsp.key)
                      case rsp: dd.Replicator.GetDataDeleted[d] => JReplicator.GetDataDeleted(rsp.key)
                    }
                    .recover {
                      case _ => JReplicator.GetFailure(cmd.key)
                    }
                reply.foreach { cmd.replyTo ! _ }
                Behaviors.same

              case cmd: SReplicator.Update[_] =>
                classicReplicator.tell(
                  dd.Replicator.Update(cmd.key, cmd.writeConsistency, None)(cmd.modify),
                  sender = cmd.replyTo.toClassic)
                Behaviors.same

              case cmd: JReplicator.Update[d] =>
                implicit val timeout: Timeout = Timeout(cmd.writeConsistency.timeout match {
                  case java.time.Duration.ZERO => localAskTimeout
                  case t                       => t.toScala + additionalAskTimeout
                })
                import ctx.executionContext
                val reply =
                  (classicReplicator ? dd.Replicator.Update(cmd.key, cmd.writeConsistency.toClassic, None)(cmd.modify))
                    .mapTo[dd.Replicator.UpdateResponse[d]]
                    .map {
                      case rsp: dd.Replicator.UpdateSuccess[d] => JReplicator.UpdateSuccess(rsp.key)
                      case rsp: dd.Replicator.UpdateTimeout[d] => JReplicator.UpdateTimeout(rsp.key)
                      case rsp: dd.Replicator.ModifyFailure[d] =>
                        JReplicator.ModifyFailure(rsp.key, rsp.errorMessage, rsp.cause)
                      case rsp: dd.Replicator.StoreFailure[d]      => JReplicator.StoreFailure(rsp.key)
                      case rsp: dd.Replicator.UpdateDataDeleted[d] => JReplicator.UpdateDataDeleted(rsp.key)
                    }
                    .recover {
                      case _ => JReplicator.UpdateTimeout(cmd.key)
                    }
                reply.foreach { cmd.replyTo ! _ }
                Behaviors.same

              case cmd: SReplicator.Subscribe[_] =>
                // For the Scala API the SubscribeResponse messages can be sent directly to the subscriber
                classicReplicator.tell(
                  dd.Replicator.Subscribe(cmd.key, cmd.subscriber.toClassic),
                  sender = cmd.subscriber.toClassic)
                Behaviors.same

              case cmd: JReplicator.Subscribe[ReplicatedData] @unchecked =>
                // For the Java API the Changed/Deleted messages must be mapped to the JReplicator.Changed/Deleted class.
                // That is done with an adapter, and we have to keep track of the lifecycle of the original
                // subscriber and stop the adapter when the original subscriber is stopped.
                val adapter: ActorRef[dd.Replicator.SubscribeResponse[ReplicatedData]] = ctx.spawnMessageAdapter {
                  rsp =>
                    InternalSubscribeResponse(rsp, cmd.subscriber)
                }

                classicReplicator.tell(
                  dd.Replicator.Subscribe(cmd.key, adapter.toClassic),
                  sender = akka.actor.ActorRef.noSender)

                ctx.watch(cmd.subscriber)

                withState(subscribeAdapters.updated(cmd.subscriber, adapter))

              case InternalSubscribeResponse(rsp, subscriber) =>
                rsp match {
                  case chg: dd.Replicator.Changed[_] => subscriber ! JReplicator.Changed(chg.key)(chg.dataValue)
                  case del: dd.Replicator.Deleted[_] => subscriber ! JReplicator.Deleted(del.key)
                  case exp: dd.Replicator.Expired[_] => subscriber ! JReplicator.Expired(exp.key)
                }
                Behaviors.same

              case cmd: SReplicator.Unsubscribe[_] =>
                classicReplicator.tell(
                  dd.Replicator.Unsubscribe(cmd.key, cmd.subscriber.toClassic),
                  sender = cmd.subscriber.toClassic)
                Behaviors.same

              case cmd: JReplicator.Unsubscribe[ReplicatedData] @unchecked =>
                stopSubscribeAdapter(cmd.subscriber)

              case cmd: SReplicator.Delete[_] =>
                classicReplicator.tell(dd.Replicator.Delete(cmd.key, cmd.consistency), sender = cmd.replyTo.toClassic)
                Behaviors.same

              case cmd: JReplicator.Delete[d] =>
                implicit val timeout: Timeout = Timeout(cmd.consistency.timeout match {
                  case java.time.Duration.ZERO => localAskTimeout
                  case t                       => t.toScala + additionalAskTimeout
                })
                import ctx.executionContext
                val reply =
                  (classicReplicator ? dd.Replicator.Delete(cmd.key, cmd.consistency.toClassic))
                    .mapTo[dd.Replicator.DeleteResponse[d]]
                    .map {
                      case rsp: dd.Replicator.DeleteSuccess[d] => JReplicator.DeleteSuccess(rsp.key)
                      case rsp: dd.Replicator.ReplicationDeleteFailure[d] =>
                        JReplicator.DeleteFailure(rsp.key)
                      case rsp: dd.Replicator.DataDeleted[d]  => JReplicator.DataDeleted(rsp.key)
                      case rsp: dd.Replicator.StoreFailure[d] => JReplicator.StoreFailure(rsp.key)
                    }
                    .recover {
                      case _ => JReplicator.DeleteFailure(cmd.key)
                    }
                reply.foreach { cmd.replyTo ! _ }
                Behaviors.same

              case SReplicator.GetReplicaCount(replyTo) =>
                classicReplicator.tell(dd.Replicator.GetReplicaCount, sender = replyTo.toClassic)
                Behaviors.same

              case JReplicator.GetReplicaCount(replyTo) =>
                implicit val timeout = Timeout(localAskTimeout)
                import ctx.executionContext
                val reply =
                  (classicReplicator ? dd.Replicator.GetReplicaCount)
                    .mapTo[dd.Replicator.ReplicaCount]
                    .map(rsp => JReplicator.ReplicaCount(rsp.n))
                reply.foreach { replyTo ! _ }
                Behaviors.same

              case SReplicator.FlushChanges | JReplicator.FlushChanges =>
                classicReplicator.tell(dd.Replicator.FlushChanges, sender = akka.actor.ActorRef.noSender)
                Behaviors.same

              case unexpected =>
                throw new RuntimeException(s"Unexpected message: ${unexpected.getClass}") // compiler exhaustiveness check pleaser
            }
          }
          .receiveSignal {
            case (_, Terminated(ref: ActorRef[JReplicator.SubscribeResponse[ReplicatedData]] @unchecked)) =>
              stopSubscribeAdapter(ref)
          }
      }

      withState(Map.empty)

    }
  }
}
