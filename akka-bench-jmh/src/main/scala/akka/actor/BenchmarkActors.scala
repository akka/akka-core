/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration

object BenchmarkActors {

  val timeout = 30.seconds

  case object Message
  case object Stop

  class PingPong(val messagesPerPair: Int, latch: CountDownLatch) extends Actor {
    var left = messagesPerPair / 2
    def receive = {
      case Message =>
        if (left == 0) {
          latch.countDown()
          context.stop(self)
        }

        sender() ! Message
        left -= 1
    }
  }

  object PingPong {
    def props(messagesPerPair: Int, latch: CountDownLatch) = Props(new PingPong(messagesPerPair, latch))
  }

  class Echo extends Actor {
    def receive = {
      case Message =>
        sender() ! Message
    }
  }

  object EchoSender {
    def props(messagesPerPair: Int, latch: CountDownLatch, batchSize: Int): Props =
      Props(new EchoSender(messagesPerPair, latch, batchSize))
  }

  class EchoSender(messagesPerPair: Int, latch: CountDownLatch, batchSize: Int) extends Actor {
    private val echo = context.actorOf(Props[Echo]().withDispatcher(context.props.dispatcher), "echo")

    private var left = messagesPerPair / 2
    private var batch = 0

    def receive = {
      case Message =>
        batch -= 1
        if (batch <= 0) {
          if (!sendBatch()) {
            latch.countDown()
            context.stop(self)
          }
        }
    }

    private def sendBatch(): Boolean = {
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
  }

  class Pipe(next: Option[ActorRef]) extends Actor {
    def receive = {
      case Message =>
        if (next.isDefined) next.get.forward(Message)
      case Stop =>
        context.stop(self)
        if (next.isDefined) next.get.forward(Stop)
    }
  }

  object Pipe {
    def props(next: Option[ActorRef]) = Props(new Pipe(next))
  }

  private def startPingPongActorPairs(messagesPerPair: Int, numPairs: Int, dispatcher: String)(
      implicit system: ActorSystem): (Vector[(ActorRef, ActorRef)], CountDownLatch) = {
    val fullPathToDispatcher = "akka.actor." + dispatcher
    val latch = new CountDownLatch(numPairs * 2)
    val actors = List
      .fill(numPairs) {
        val ping = system.actorOf(PingPong.props(messagesPerPair, latch).withDispatcher(fullPathToDispatcher))
        val pong = system.actorOf(PingPong.props(messagesPerPair, latch).withDispatcher(fullPathToDispatcher))
        (ping, pong)
      }
      .toVector
    (actors, latch)
  }

  private def initiatePingPongForPairs(refs: Vector[(ActorRef, ActorRef)], inFlight: Int): Unit = {
    for {
      (ping, pong) <- refs
      _ <- 1 to inFlight
    } {
      ping.tell(Message, pong)
    }
  }

  private def startEchoActorPairs(messagesPerPair: Int, numPairs: Int, dispatcher: String, batchSize: Int)(
      implicit system: ActorSystem): (Vector[ActorRef], CountDownLatch) = {

    val fullPathToDispatcher = "akka.actor." + dispatcher
    val latch = new CountDownLatch(numPairs)
    val actors = (1 to numPairs).map { _ =>
      system.actorOf(EchoSender.props(messagesPerPair, latch, batchSize).withDispatcher(fullPathToDispatcher))
    }.toVector
    (actors, latch)
  }

  private def initiateEchoPairs(refs: Vector[ActorRef]): Unit = {
    refs.foreach(_ ! Message)
  }

  def printProgress(totalMessages: Long, numActors: Int, startNanoTime: Long): Unit = {
    val durationMicros = (System.nanoTime() - startNanoTime) / 1000
    println(
      f"  $totalMessages messages by $numActors actors took ${durationMicros / 1000} ms, " +
      f"${totalMessages.toDouble / durationMicros}%,.2f M msg/s")
  }

  def requireRightNumberOfCores(numCores: Int): Unit =
    require(
      Runtime.getRuntime.availableProcessors == numCores,
      s"Update the cores constant to ${Runtime.getRuntime.availableProcessors}")

  def benchmarkPingPongActors(
      numMessagesPerActorPair: Int,
      numActors: Int,
      dispatcher: String,
      throughPut: Int,
      shutdownTimeout: Duration)(implicit system: ActorSystem): Unit = {
    val numPairs = numActors / 2
    val totalNumMessages = numPairs * numMessagesPerActorPair
    val (actors, latch) = startPingPongActorPairs(numMessagesPerActorPair, numPairs, dispatcher)
    val startNanoTime = System.nanoTime()
    initiatePingPongForPairs(actors, inFlight = throughPut * 2)
    latch.await(shutdownTimeout.toSeconds, TimeUnit.SECONDS)
    printProgress(totalNumMessages, numActors, startNanoTime)
  }

  def benchmarkEchoActors(
      numMessagesPerActorPair: Int,
      numActors: Int,
      dispatcher: String,
      batchSize: Int,
      shutdownTimeout: Duration)(implicit system: ActorSystem): Unit = {
    val numPairs = numActors / 2
    val totalNumMessages = numPairs * numMessagesPerActorPair
    val (actors, latch) = startEchoActorPairs(numMessagesPerActorPair, numPairs, dispatcher, batchSize)
    val startNanoTime = System.nanoTime()
    initiateEchoPairs(actors)
    latch.await(shutdownTimeout.toSeconds, TimeUnit.SECONDS)
    printProgress(totalNumMessages, numActors, startNanoTime)
  }

  def tearDownSystem()(implicit system: ActorSystem): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, timeout)
  }

}
