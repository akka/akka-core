/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ KillSwitches, RestartSettings, UniqueKillSwitch }
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RestartSource
import akka.stream.scaladsl.Sink

import scala.concurrent.duration._
import scala.util.control.NoStackTrace
// #imports
import akka.stream.scaladsl.Source
// #imports

object Restart extends App {
  implicit val system: ActorSystem = ActorSystem()

  onRestartWitFailureKillSwitch()

  case class CantConnectToDatabase(msg: String) extends RuntimeException(msg) with NoStackTrace

  def onRestartWithBackoffInnerFailure(): Unit = {
    //#restart-failure-inner-failure
    // could throw if for example it used a database connection to get rows
    val flakySource: Source[() => Int, NotUsed] =
      Source(List(() => 1, () => 2, () => 3, () => throw CantConnectToDatabase("darn")))
    val forever =
      RestartSource.onFailuresWithBackoff(
        RestartSettings(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.1))(() => flakySource)
    forever.runWith(Sink.foreach(nr => system.log.info("{}", nr())))
    // logs
    //[INFO] [12/10/2019 13:51:58.300] [default-akka.test.stream-dispatcher-7] [akka.actor.ActorSystemImpl(default)] 1
    //[INFO] [12/10/2019 13:51:58.301] [default-akka.test.stream-dispatcher-7] [akka.actor.ActorSystemImpl(default)] 2
    //[INFO] [12/10/2019 13:51:58.302] [default-akka.test.stream-dispatcher-7] [akka.actor.ActorSystemImpl(default)] 3
    //[WARN] [12/10/2019 13:51:58.310] [default-akka.test.stream-dispatcher-7] [RestartWithBackoffSource(akka://default)] Restarting graph due to failure. stack_trace:  (docs.stream.operators.source.Restart$CantConnectToDatabase: darn)
    // --> 1 second gap
    //[INFO] [12/10/2019 13:51:59.379] [default-akka.test.stream-dispatcher-8] [akka.actor.ActorSystemImpl(default)] 1
    //[INFO] [12/10/2019 13:51:59.382] [default-akka.test.stream-dispatcher-8] [akka.actor.ActorSystemImpl(default)] 2
    //[INFO] [12/10/2019 13:51:59.383] [default-akka.test.stream-dispatcher-8] [akka.actor.ActorSystemImpl(default)] 3
    //[WARN] [12/10/2019 13:51:59.386] [default-akka.test.stream-dispatcher-8] [RestartWithBackoffSource(akka://default)] Restarting graph due to failure. stack_trace:  (docs.stream.operators.source.Restart$CantConnectToDatabase: darn)
    //--> 2 second gap
    //[INFO] [12/10/2019 13:52:01.594] [default-akka.test.stream-dispatcher-8] [akka.actor.ActorSystemImpl(default)] 1
    //[INFO] [12/10/2019 13:52:01.595] [default-akka.test.stream-dispatcher-8] [akka.actor.ActorSystemImpl(default)] 2
    //[INFO] [12/10/2019 13:52:01.595] [default-akka.test.stream-dispatcher-8] [akka.actor.ActorSystemImpl(default)] 3
    //[WARN] [12/10/2019 13:52:01.596] [default-akka.test.stream-dispatcher-8] [RestartWithBackoffSource(akka://default)] Restarting graph due to failure. stack_trace:  (docs.stream.operators.source.Restart$CantConnectToDatabase: darn)
    //#restart-failure-inner-failure

  }

  def onRestartWithBackoffInnerComplete(): Unit = {

    //#restart-failure-inner-complete
    val finiteSource = Source.tick(1.second, 1.second, "tick").take(3)
    val forever = RestartSource.onFailuresWithBackoff(RestartSettings(1.second, 10.seconds, 0.1))(() => finiteSource)
    forever.runWith(Sink.foreach(println))
    // prints
    // tick
    // tick
    // tick
    //#restart-failure-inner-complete
  }

  def onRestartWitFailureKillSwitch(): Unit = {
    //#restart-failure-inner-complete-kill-switch
    val flakySource: Source[() => Int, NotUsed] =
      Source(List(() => 1, () => 2, () => 3, () => throw CantConnectToDatabase("darn")))
    val stopRestarting: UniqueKillSwitch =
      RestartSource
        .onFailuresWithBackoff(RestartSettings(1.second, 10.seconds, 0.1))(() => flakySource)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.foreach(nr => println(s"Nr ${nr()}")))(Keep.left)
        .run()
    //... from some where else
    // stop the source from restarting
    stopRestarting.shutdown()
    //#restart-failure-inner-complete-kill-switch
  }
}
