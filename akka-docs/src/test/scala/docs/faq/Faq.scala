/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.faq

import akka.actor.Actor

//#exhaustiveness-check
object MyActor {
  // these are the messages we accept
  sealed abstract trait Message
  final case class FooMessage(foo: String) extends Message
  final case class BarMessage(bar: Int) extends Message

  // these are the replies we send
  sealed abstract trait Reply
  final case class BazMessage(baz: String) extends Reply
}

class MyActor extends Actor {
  import MyActor._
  def receive = {
    case message: Message =>
      message match {
        case BarMessage(bar) => sender() ! BazMessage("Got " + bar)
        // warning here:
        // "match may not be exhaustive. It would fail on the following input: FooMessage(_)"
        //#exhaustiveness-check
        case FooMessage(_) => // avoid the warning in our build logs
        //#exhaustiveness-check
      }
  }
}
//#exhaustiveness-check
