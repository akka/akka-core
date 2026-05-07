/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal.testkit

import java.time.Duration
import java.util.concurrent.CompletionStage

import scala.concurrent.Future
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._

import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.Scheduler
import akka.actor.typed.internal.InternalRecipientRef
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.javadsl
import akka.cluster.sharding.typed.javadsl.EntityRef
import akka.cluster.sharding.typed.scaladsl
import akka.japi.function.{ Function => JFunction }
import akka.pattern.StatusReply
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class TestEntityRefImpl[M](
    override val entityId: String,
    probe: ActorRef[M],
    override val typeKey: scaladsl.EntityTypeKey[M])
    extends javadsl.EntityRef[M]
    with scaladsl.EntityRef[M]
    with InternalRecipientRef[M] {

  import akka.actor.typed.scaladsl.adapter._

  override def dataCenter: Option[String] = None

  override def tell(msg: M): Unit =
    probe ! msg

  override def ask[U](message: ActorRef[U] => M)(implicit timeout: Timeout): Future[U] = {
    import akka.actor.typed.scaladsl.AskPattern._
    implicit val scheduler: Scheduler = provider.guardian.underlying.system.scheduler.toTyped
    probe.ask(message)
  }

  def ask[U](message: JFunction[ActorRef[U], M], timeout: Duration): CompletionStage[U] =
    ask[U](replyTo => message.apply(replyTo))(timeout.toScala).asJava

  override def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M, timeout: Duration): CompletionStage[Res] =
    askWithStatus(f)(timeout.toScala).asJava

  override def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M)(implicit timeout: Timeout): Future[Res] =
    StatusReply.flattenStatusFuture(ask(f))

  // impl InternalRecipientRef
  override def provider: ActorRefProvider = {
    probe.asInstanceOf[InternalRecipientRef[_]].provider
  }

  // impl InternalRecipientRef
  def isTerminated: Boolean = {
    probe.asInstanceOf[InternalRecipientRef[_]].isTerminated
  }

  override def toString: String = s"TestEntityRef($entityId)"

  override private[akka] def asJava: EntityRef[M] = this
}
