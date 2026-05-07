/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.stream.typed

import akka.NotUsed
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.Behaviors

object ActorSourceSinkExample {

  def compileOnlySourceRef() = {
    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "ActorSourceSinkExample")

    // #actor-source-ref
    import akka.actor.typed.ActorRef
    import akka.stream.OverflowStrategy
    import akka.stream.scaladsl.{ Sink, Source }
    import akka.stream.typed.scaladsl.ActorSource

    trait Protocol
    case class Message(msg: String) extends Protocol
    case object Complete extends Protocol
    case class Fail(ex: Exception) extends Protocol

    val source: Source[Protocol, ActorRef[Protocol]] = ActorSource.actorRef[Protocol](completionMatcher = {
      case Complete =>
    }, failureMatcher = {
      case Fail(ex) => ex
    }, bufferSize = 8, overflowStrategy = OverflowStrategy.fail)

    val ref = source
      .collect {
        case Message(msg) => msg
      }
      .to(Sink.foreach(println))
      .run()

    ref ! Message("msg1")
    // ref ! "msg2" Does not compile
    // #actor-source-ref
  }

  def main(args: Array[String]): Unit = {

    // #actor-source-with-backpressure
    import akka.actor.typed.ActorRef
    import akka.stream.CompletionStrategy
    import akka.stream.scaladsl.Sink
    import akka.stream.typed.scaladsl.ActorSource

    object StreamFeeder {

      /** Signals that the latest element is emitted into the stream */
      case object Emitted

      sealed trait Event
      case class Element(content: String) extends Event
      case object ReachedEnd extends Event
      case class FailureOccured(ex: Exception) extends Event

      def apply(): Behavior[Emitted.type] =
        Behaviors.setup { context =>
          val streamActor = runStream(context.self)(context.system)
          streamActor ! Element("first")
          sender(streamActor, 0)
        }

      private def runStream(ackReceiver: ActorRef[Emitted.type])(implicit system: ActorSystem[_]): ActorRef[Event] = {
        val source =
          ActorSource.actorRefWithBackpressure[Event, Emitted.type](
            // get demand signalled to this actor receiving Ack
            ackTo = ackReceiver,
            ackMessage = Emitted,
            // complete when we send ReachedEnd
            completionMatcher = {
              case ReachedEnd => CompletionStrategy.draining
            },
            failureMatcher = {
              case FailureOccured(ex) => ex
            })

        val streamActor: ActorRef[Event] = source
          .collect {
            case Element(msg) => msg
          }
          .to(Sink.foreach(println))
          .run()

        streamActor
      }

      private def sender(streamSource: ActorRef[Event], counter: Int): Behavior[Emitted.type] =
        Behaviors.receiveMessage {
          case Emitted if counter < 5 =>
            streamSource ! Element(counter.toString)
            sender(streamSource, counter + 1)
          case _ =>
            streamSource ! ReachedEnd
            Behaviors.stopped
        }
    }

    ActorSystem(StreamFeeder(), "stream-feeder")

    // Will print:
    // first
    // 0
    // 1
    // 2
    // 3
    // 4
    // #actor-source-with-backpressure
  }

  def compileOnlyAcotrRef() = {
    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "ActorSourceSinkExample")

    def targetActor(): ActorRef[Protocol] = ???

    // #actor-sink-ref
    import akka.actor.typed.ActorRef
    import akka.stream.scaladsl.{ Sink, Source }
    import akka.stream.typed.scaladsl.ActorSink

    trait Protocol
    case class Message(msg: String) extends Protocol
    case object Complete extends Protocol
    case class Fail(ex: Throwable) extends Protocol

    val actor: ActorRef[Protocol] = targetActor()

    val sink: Sink[Protocol, NotUsed] =
      ActorSink.actorRef[Protocol](ref = actor, onCompleteMessage = Complete, onFailureMessage = Fail.apply)

    Source.single(Message("msg1")).runWith(sink)
    // #actor-sink-ref

  }

  def compileOnlySinkWithBackpressure() = {
    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "ActorSourceSinkExample")

    def targetActor(): ActorRef[Protocol] = ???

    // #actor-sink-ref-with-backpressure
    import akka.actor.typed.ActorRef
    import akka.stream.scaladsl.{ Sink, Source }
    import akka.stream.typed.scaladsl.ActorSink

    trait Ack
    object Ack extends Ack

    trait Protocol
    case class Init(ackTo: ActorRef[Ack]) extends Protocol
    case class Message(ackTo: ActorRef[Ack], msg: String) extends Protocol
    case object Complete extends Protocol
    case class Fail(ex: Throwable) extends Protocol

    val actor: ActorRef[Protocol] = targetActor()

    val sink: Sink[String, NotUsed] = ActorSink.actorRefWithBackpressure(
      ref = actor,
      messageAdapter = (responseActorRef: ActorRef[Ack], element) => Message(responseActorRef, element),
      onInitMessage = (responseActorRef: ActorRef[Ack]) => Init(responseActorRef),
      ackMessage = Ack,
      onCompleteMessage = Complete,
      onFailureMessage = (exception) => Fail(exception))

    Source.single("msg1").runWith(sink)
    // #actor-sink-ref-with-backpressure
  }
}
