/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import akka.actor._

/**
 * Entry point to Akka’s IO layer.
 *
 * @see <a href="https://akka.io/docs/">the Akka online documentation</a>
 */
object IO {

  trait Extension extends akka.actor.Extension {
    def manager: ActorRef
  }

  /**
   * Scala API: obtain a reference to the manager actor for the given IO extension,
   * for example [[Tcp]] or [[Udp]].
   *
   * For the Java API please refer to the individual extensions directly.
   */
  def apply[T <: Extension](key: ExtensionId[T])(implicit system: ActorSystem): ActorRef = key(system).manager

}
