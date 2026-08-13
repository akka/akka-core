/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease.internal

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Consumer

import scala.jdk.FutureConverters.CompletionStageOps
import scala.jdk.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.annotation.InternalApi
import akka.coordination.lease.LeaseSettings
import akka.coordination.lease.javadsl.{ Lease => JavaLease }
import akka.coordination.lease.scaladsl.{ Lease => ScalaLease }
import akka.util.DispatcherFutureConverters.ScalaFutureOpsWithShiftTo

/**
 * INTERNAL API
 */
@InternalApi
final private[akka] class LeaseAdapter(delegate: ScalaLease)(implicit val ec: ExecutionContext) extends JavaLease {

  override def acquire(): CompletionStage[java.lang.Boolean] =
    delegate.acquire().map(Boolean.box)(ExecutionContext.parasitic).asJava(ec)

  override def acquire(leaseLostCallback: Consumer[Optional[Throwable]]): CompletionStage[java.lang.Boolean] = {
    delegate.acquire(o => leaseLostCallback.accept(o.toJava)).map(Boolean.box)(ExecutionContext.parasitic).asJava(ec)
  }

  override def release(): CompletionStage[java.lang.Boolean] =
    delegate.release().map(Boolean.box)(ExecutionContext.parasitic).asJava(ec)

  override def checkLease(): Boolean = delegate.checkLease()
  override def getSettings(): LeaseSettings = delegate.settings
}

/**
 * INTERNAL API
 */
@InternalApi
final private[akka] class LeaseAdapterToScala(val delegate: JavaLease)(implicit val ec: ExecutionContext)
    extends ScalaLease(delegate.getSettings()) {

  override def acquire(): Future[Boolean] =
    delegate.acquire().asScala.map(Boolean.unbox)(ExecutionContext.parasitic)

  override def acquire(leaseLostCallback: Option[Throwable] => Unit): Future[Boolean] =
    delegate.acquire(o => leaseLostCallback(o.toScala)).asScala.map(Boolean.unbox)(ExecutionContext.parasitic)

  override def release(): Future[Boolean] =
    delegate.release().asScala.map(Boolean.unbox)(ExecutionContext.parasitic)

  override def checkLease(): Boolean =
    delegate.checkLease()
}
