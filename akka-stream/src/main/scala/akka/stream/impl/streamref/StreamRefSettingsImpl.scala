/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.streamref

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.StreamRefSettings

/** INTERNAL API */
@InternalApi
private[akka] final case class StreamRefSettingsImpl(
    override val bufferCapacity: Int,
    override val demandRedeliveryInterval: FiniteDuration,
    override val subscriptionTimeout: FiniteDuration,
    override val finalTerminationSignalDeadline: FiniteDuration)
    extends StreamRefSettings {

  override def withBufferCapacity(value: Int): StreamRefSettings = copy(bufferCapacity = value)
  override def withDemandRedeliveryInterval(value: FiniteDuration): StreamRefSettings =
    copy(demandRedeliveryInterval = value)
  override def withSubscriptionTimeout(value: FiniteDuration): StreamRefSettings = copy(subscriptionTimeout = value)
  override def withTerminationReceivedBeforeCompletionLeeway(value: FiniteDuration): StreamRefSettings =
    copy(finalTerminationSignalDeadline = value)

  override def productPrefix: String = Logging.simpleName(classOf[StreamRefSettings])

}

/** INTERNAL API */
@InternalApi
private[akka] object StreamRefSettingsImpl {
  def apply(c: Config): StreamRefSettingsImpl =
    StreamRefSettingsImpl(
      bufferCapacity = c.getInt("buffer-capacity"),
      demandRedeliveryInterval = c.getDuration("demand-redelivery-interval", TimeUnit.MILLISECONDS).millis,
      subscriptionTimeout = c.getDuration("subscription-timeout", TimeUnit.MILLISECONDS).millis,
      finalTerminationSignalDeadline = c.getDuration("final-termination-signal-deadline", TimeUnit.MILLISECONDS).millis)
}
