/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package typed.tutorial_3

/*
//#read-protocol-1
//#device-with-read
package com.example

//#device-with-read
//#read-protocol-1
 */

object DeviceInProgress1 {

  //#read-protocol-1
  import akka.actor.typed.ActorRef

  object Device {
    sealed trait Command
    final case class ReadTemperature(replyTo: ActorRef[RespondTemperature]) extends Command
    final case class RespondTemperature(value: Option[Double])
  }
  //#read-protocol-1

}

object DeviceInProgress2 {

  //#device-with-read
  import akka.actor.typed.ActorRef
  import akka.actor.typed.Behavior
  import akka.actor.typed.PostStop
  import akka.actor.typed.Signal
  import akka.actor.typed.scaladsl.AbstractBehavior
  import akka.actor.typed.scaladsl.ActorContext
  import akka.actor.typed.scaladsl.Behaviors

  object Device {
    def apply(groupId: String, deviceId: String): Behavior[Command] =
      Behaviors.setup(context => new Device(context, groupId, deviceId))

    //#read-protocol-2
    sealed trait Command
    final case class ReadTemperature(requestId: Long, replyTo: ActorRef[RespondTemperature]) extends Command
    final case class RespondTemperature(requestId: Long, value: Option[Double])
    //#read-protocol-2
  }

  class Device(context: ActorContext[Device.Command], groupId: String, deviceId: String)
      extends AbstractBehavior[Device.Command](context) {
    import Device._

    var lastTemperatureReading: Option[Double] = None

    context.log.info("Device actor {}-{} started", groupId, deviceId)

    override def onMessage(msg: Command): Behavior[Command] = {
      msg match {
        case ReadTemperature(id, replyTo) =>
          replyTo ! RespondTemperature(id, lastTemperatureReading)
          this
      }
    }

    override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
      case PostStop =>
        context.log.info("Device actor {}-{} stopped", groupId, deviceId)
        this
    }

  }
  //#device-with-read

}

object DeviceInProgress3 {

  object Device {
    //#write-protocol-1
    sealed trait Command
    final case class RecordTemperature(value: Double) extends Command
    //#write-protocol-1
  }
}
