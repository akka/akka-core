/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

object TypedBenchmarkActors {

  // to avoid benchmark to be dominated by allocations of message
  // we pass the respondTo actor ref into the behavior
  case object Message

  private def echoBehavior(respondTo: ActorRef[Message.type]): Behavior[Message.type] = Behaviors.receive { (_, _) =>
    respondTo ! Message
    Behaviors.same
  }

  private def echoSender(
      messagesPerPair: Int,
      onDone: ActorRef[Done],
      batchSize: Int,
      childProps: Props): Behavior[Message.type] =
    Behaviors.setup { ctx =>
      val echo = ctx.spawn(echoBehavior(ctx.self), "echo", childProps)
      var left = messagesPerPair / 2
      var batch = 0

      def sendBatch(): Boolean = {
        if (left > 0) {
          var i = 0
          while (i < batchSize) {
            echo ! Message
            i += 1
          }
          left -= batchSize
          batch = batchSize
          true
        } else
          false
      }

      Behaviors.receiveMessage { _ =>
        batch -= 1
        if (batch <= 0 && !sendBatch()) {
          onDone ! Done
          Behaviors.stopped
        } else {
          Behaviors.same
        }
      }
    }

  case class Start(respondTo: ActorRef[Completed])
  case class Completed(startNanoTime: Long)

  def echoActorsSupervisor(
      numMessagesPerActorPair: Int,
      numActors: Int,
      dispatcher: String,
      batchSize: Int): Behavior[Start] =
    Behaviors.receive { (ctx, msg) =>
      msg match {
        case Start(respondTo) =>
          // note: no protection against accidentally running bench sessions in parallel
          val sessionBehavior =
            startEchoBenchSession(numMessagesPerActorPair, numActors, dispatcher, batchSize, respondTo)
          ctx.spawnAnonymous(sessionBehavior)
          Behaviors.same
      }
    }

  private def startEchoBenchSession(
      messagesPerPair: Int,
      numActors: Int,
      dispatcher: String,
      batchSize: Int,
      respondTo: ActorRef[Completed]): Behavior[Unit] = {

    val numPairs = numActors / 2

    Behaviors
      .setup[Any] { ctx =>
        val props = Props.empty.withDispatcherFromConfig("akka.actor." + dispatcher)
        val pairs = (1 to numPairs).map { _ =>
          ctx.spawnAnonymous(echoSender(messagesPerPair, ctx.self.narrow[Done], batchSize, props), props)
        }
        val startNanoTime = System.nanoTime()
        pairs.foreach(_ ! Message)
        var interactionsLeft = numPairs
        Behaviors.receiveMessagePartial {
          case Done =>
            interactionsLeft -= 1
            if (interactionsLeft == 0) {
              val totalNumMessages = numPairs * messagesPerPair
              printProgress(totalNumMessages, numActors, startNanoTime)
              respondTo ! Completed(startNanoTime)
              Behaviors.stopped
            } else {
              Behaviors.same
            }

        }
      }
      .narrow[Unit]
  }

  sealed trait PingPongCommand
  case class StartPingPong(
      messagesPerPair: Int,
      numActors: Int,
      dispatcher: String,
      throughPut: Int,
      shutdownTimeout: Duration,
      replyTo: ActorRef[PingPongStarted])
      extends PingPongCommand
  case class PingPongStarted(completedLatch: CountDownLatch, startNanoTime: Long, totalNumMessages: Int)
  case object Stop extends PingPongCommand
  def benchmarkPingPongSupervisor(): Behavior[PingPongCommand] = {
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case StartPingPong(numMessagesPerActorPair, numActors, dispatcher, throughput, _, replyTo) =>
          val numPairs = numActors / 2
          val totalNumMessages = numPairs * numMessagesPerActorPair
          val (actors, latch) = startPingPongActorPairs(ctx, numMessagesPerActorPair, numPairs, dispatcher)
          val startNanoTime = System.nanoTime()
          replyTo ! PingPongStarted(latch, startNanoTime, totalNumMessages)
          initiatePingPongForPairs(actors, inFlight = throughput * 2)
          Behaviors.same

        case Stop =>
          ctx.children.foreach(ctx.stop _)
          Behaviors.same
      }
    }
  }

  private def initiatePingPongForPairs(refs: Vector[(ActorRef[Message], ActorRef[Message])], inFlight: Int): Unit = {
    for {
      (ping, pong) <- refs
      message = Message(pong) // just allocate once
      _ <- 1 to inFlight
    } ping ! message
  }

  private def startPingPongActorPairs(
      ctx: ActorContext[_],
      messagesPerPair: Int,
      numPairs: Int,
      dispatcher: String): (Vector[(ActorRef[Message], ActorRef[Message])], CountDownLatch) = {
    val fullPathToDispatcher = "akka.actor." + dispatcher
    val latch = new CountDownLatch(numPairs * 2)
    val pingPongBehavior = newPingPongBehavior(messagesPerPair, latch)
    val pingPongProps = Props.empty.withDispatcherFromConfig(fullPathToDispatcher)
    val actors = for {
      _ <- (1 to numPairs).toVector
    } yield {
      val ping = ctx.spawnAnonymous(pingPongBehavior, pingPongProps)
      val pong = ctx.spawnAnonymous(pingPongBehavior, pingPongProps)
      (ping, pong)
    }
    (actors, latch)
  }

  case class Message(replyTo: ActorRef[Message])
  private def newPingPongBehavior(messagesPerPair: Int, latch: CountDownLatch): Behavior[Message] =
    Behaviors.setup { ctx =>
      var left = messagesPerPair / 2
      val pong = Message(ctx.self) // we re-use a single pong to avoid alloc on each msg
      Behaviors.receiveMessage[Message] {
        case Message(replyTo) =>
          replyTo ! pong
          if (left == 0) {
            latch.countDown()
            Behaviors.stopped // note that this will likely lead to dead letters
          } else {
            left -= 1
            Behaviors.same
          }
      }
    }

  def printProgress(totalMessages: Long, numActors: Int, startNanoTime: Long) = {
    val durationMicros = (System.nanoTime() - startNanoTime) / 1000
    println(
      f"  $totalMessages messages by $numActors actors took ${durationMicros / 1000} ms, " +
      f"${totalMessages.toDouble / durationMicros}%,.2f M msg/s")
  }
}
