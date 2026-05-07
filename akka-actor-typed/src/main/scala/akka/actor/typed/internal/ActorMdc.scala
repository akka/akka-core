/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import org.slf4j.MDC

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ActorMdc {
  val SourceActorSystemKey = "sourceActorSystem"
  val AkkaSourceKey = "akkaSource"
  val AkkaTagsKey = "akkaTags"
  val AkkaAddressKey = "akkaAddress"

  def setMdc(context: ActorContextImpl.LoggingContext): Unit = {
    // avoid access to MDC ThreadLocal if not needed, see details in LoggingContext
    context.mdcUsed = true
    MDC.put(AkkaSourceKey, context.akkaSource)
    MDC.put(SourceActorSystemKey, context.sourceActorSystem)
    MDC.put(AkkaAddressKey, context.akkaAddress)
    // empty string for no tags, a single tag or a comma separated list of tags
    if (context.tagsString.nonEmpty)
      MDC.put(AkkaTagsKey, context.tagsString)
  }

  // MDC is cleared (if used) from aroundReceive in ActorAdapter after processing each message,
  // via ActorContextImpl.clearMdc()
  def clearMdc(): Unit =
    MDC.clear()

}
