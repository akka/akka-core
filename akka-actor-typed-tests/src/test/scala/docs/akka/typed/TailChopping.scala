/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

//#behavior
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object TailChopping {

  sealed trait Command
  private case object RequestTimeout extends Command
  private case object FinalTimeout extends Command
  private case class WrappedReply[R](reply: R) extends Command

  def apply[Reply: ClassTag](
      sendRequest: (Int, ActorRef[Reply]) => Boolean,
      nextRequestAfter: FiniteDuration,
      replyTo: ActorRef[Reply],
      finalTimeout: FiniteDuration,
      timeoutReply: Reply): Behavior[Command] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        val replyAdapter = context.messageAdapter[Reply](WrappedReply(_))

        def waiting(requestCount: Int): Behavior[Command] = {
          Behaviors.receiveMessage {
            case WrappedReply(reply) =>
              replyTo ! reply.asInstanceOf[Reply]
              Behaviors.stopped

            case RequestTimeout =>
              sendNextRequest(requestCount + 1)

            case FinalTimeout =>
              replyTo ! timeoutReply
              Behaviors.stopped
          }
        }

        def sendNextRequest(requestCount: Int): Behavior[Command] = {
          if (sendRequest(requestCount, replyAdapter)) {
            timers.startSingleTimer(RequestTimeout, nextRequestAfter)
          } else {
            timers.startSingleTimer(FinalTimeout, finalTimeout)
          }
          waiting(requestCount)
        }

        sendNextRequest(1)
      }
    }
  }

}
//#behavior
