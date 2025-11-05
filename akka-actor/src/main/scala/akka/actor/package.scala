/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka

import language.implicitConversions

package object actor {
  @deprecated("implicit conversion is obsolete", "2.6.13")
  @inline implicit final def actorRef2Scala(ref: ActorRef): ScalaActorRef = ref.asInstanceOf[ScalaActorRef]
  @deprecated("implicit conversion is obsolete", "2.6.13")
  @inline implicit final def scala2ActorRef(ref: ScalaActorRef): ActorRef = ref.asInstanceOf[ActorRef]
}
