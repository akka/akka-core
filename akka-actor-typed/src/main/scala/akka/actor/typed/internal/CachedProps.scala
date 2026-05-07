/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.typed.Props
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
final private[akka] case class CachedProps(
    typedProps: Props,
    adaptedProps: akka.actor.Props,
    rethrowTypedFailure: Boolean)
