/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

import akka.actor._
import scala.collection.mutable.ListBuffer

/**
 * Requirements are as follows:
 * The first thing the actor needs to do, is to subscribe to a channel of events,
 * Then it must replay (process) all "old" events
 * Then it has to wait for a GoAhead message to begin processing the new events
 * It mustn't "miss" events that happen between catching up with the old events and getting the GoAhead signal
 */
class UnnestedReceives extends Actor {
  import context.become
  //If you need to store sender/senderFuture you can change it to ListBuffer[(Any, Channel)]
  val queue = new ListBuffer[Any]()

  //This message processes a message/event
  def process(msg: Any): Unit = println("processing: " + msg)
  //This method subscribes the actor to the event bus
  def subscribe(): Unit = {} //Your external stuff
  //This method retrieves all prior messages/events
  def allOldMessages() = List()

  override def preStart(): Unit = {
    //We override preStart to be sure that the first message the actor gets is
    //Replay, that message will start to be processed _after_ the actor is started
    self ! "Replay"
    //Then we subscribe to the stream of messages/events
    subscribe()
  }

  def receive = {
    case "Replay" => //Our first message should be a Replay message, all others are invalid
      allOldMessages().foreach(process) //Process all old messages/events
      become { //Switch behavior to look for the GoAhead signal
        case "GoAhead" => //When we get the GoAhead signal we process all our buffered messages/events
          queue.foreach(process)
          queue.clear()
          become { //Then we change behavior to process incoming messages/events as they arrive
            case msg => process(msg)
          }
        case msg => //While we haven't gotten the GoAhead signal, buffer all incoming messages
          queue += msg //Here you have full control, you can handle overflow etc
      }
  }
}
