/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.pattern

import akka.actor.{ ActorContext, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy }
import akka.cluster.sharding.ShardRegion.Passivate
import akka.pattern.{ BackoffOpts, BackoffSupervisor }
import akka.testkit.TestActors.EchoActor

class BackoffSupervisorDocSpec {

  class BackoffSupervisorDocSpecExampleStop {
    val system: ActorSystem = ???
    import scala.concurrent.duration._

    //#backoff-stop
    val childProps = Props(classOf[EchoActor])

    val supervisor = BackoffSupervisor.props(
      BackoffOpts.onStop(
        childProps,
        childName = "myEcho",
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
      ))

    system.actorOf(supervisor, name = "echoSupervisor")
    //#backoff-stop
  }

  class BackoffSupervisorDocSpecExampleFail {
    val system: ActorSystem = ???
    import scala.concurrent.duration._

    //#backoff-fail
    val childProps = Props(classOf[EchoActor])

    val supervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(
        childProps,
        childName = "myEcho",
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
      ))

    system.actorOf(supervisor, name = "echoSupervisor")
    //#backoff-fail
  }

  class BackoffSupervisorDocSpecExampleStopOptions {
    val system: ActorSystem = ???
    import scala.concurrent.duration._

    val childProps = Props(classOf[EchoActor])

    //#backoff-custom-stop
    val supervisor = BackoffSupervisor.props(
      BackoffOpts
        .onStop(
          childProps,
          childName = "myEcho",
          minBackoff = 3.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
        )
        .withManualReset // the child must send BackoffSupervisor.Reset to its parent
        .withDefaultStoppingStrategy // Stop at any Exception thrown
    )
    //#backoff-custom-stop

    system.actorOf(supervisor, name = "echoSupervisor")
  }

  class BackoffSupervisorDocSpecExampleFailureOptions {
    val system: ActorSystem = ???
    import scala.concurrent.duration._

    val childProps = Props(classOf[EchoActor])

    //#backoff-custom-fail
    val supervisor = BackoffSupervisor.props(
      BackoffOpts
        .onFailure(
          childProps,
          childName = "myEcho",
          minBackoff = 3.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
        )
        .withAutoReset(10.seconds) // reset if the child does not throw any errors within 10 seconds
        .withSupervisorStrategy(OneForOneStrategy() {
          case _: MyException => SupervisorStrategy.Restart
          case _              => SupervisorStrategy.Escalate
        }))
    //#backoff-custom-fail

    system.actorOf(supervisor, name = "echoSupervisor")
  }

  case class MyException(msg: String) extends Exception(msg)

  case object StopMessage

  class BackoffSupervisorDocSpecExampleSharding {
    val system: ActorSystem = ???
    val context: ActorContext = ???
    import scala.concurrent.duration._

    val childProps = Props(classOf[EchoActor])

    //#backoff-sharded
    val supervisor = BackoffSupervisor.props(
      BackoffOpts
        .onStop(childProps, childName = "myEcho", minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2)
        .withFinalStopMessage(_ == StopMessage))
    //#backoff-sharded

    //#backoff-sharded-passivation
    context.parent ! Passivate(StopMessage)
    //#backoff-sharded-passivation
  }
}
