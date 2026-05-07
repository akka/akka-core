/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.streamref

import akka.actor.{ ActorRef, DeadLetterSuppression }
import akka.annotation.InternalApi
import akka.stream.impl.ReactiveStreamsCompliance

/** INTERNAL API */
@InternalApi
private[akka] sealed trait StreamRefsProtocol

/** INTERNAL API */
@InternalApi
private[akka] object StreamRefsProtocol {

  /**
   * Sequenced `Subscriber#onNext` equivalent.
   * The receiving end of these messages MUST fail the stream if it observes gaps in the sequence,
   * as these messages will not be re-delivered.
   *
   * Sequence numbers start from `0`.
   */
  @InternalApi
  private[akka] final case class SequencedOnNext[T](seqNr: Long, payload: T)
      extends StreamRefsProtocol
      with DeadLetterSuppression {
    if (payload == null) throw ReactiveStreamsCompliance.elementMustNotBeNullException
  }

  /**
   * INTERNAL API: Initial message sent to remote side to establish partnership between origin and remote stream refs.
   */
  @InternalApi
  private[akka] final case class OnSubscribeHandshake(targetRef: ActorRef)
      extends StreamRefsProtocol
      with DeadLetterSuppression

  /**
   * INTERNAL API
   *
   * Sent to a the receiver side of a stream ref, once the sending side of the SinkRef gets signalled a Failure.
   * Sent to the sender of a stream if receiver downstream failed.
   */
  @InternalApi
  private[akka] final case class RemoteStreamFailure(msg: String) extends StreamRefsProtocol

  /**
   * INTERNAL API
   *
   * Sent to a the receiver side of a stream ref, once the sending side of the SinkRef gets signalled a completion.
   * Sent to the sender of a stream ref if receiver downstream cancelled.
   */
  @InternalApi
  private[akka] final case class RemoteStreamCompleted(seqNr: Long) extends StreamRefsProtocol

  /**
   * INTERNAL API: Cumulative demand, equivalent to sequence numbering all events in a stream.
   *
   * This message may be re-delivered.
   */
  @InternalApi
  private[akka] final case class CumulativeDemand(seqNr: Long) extends StreamRefsProtocol with DeadLetterSuppression {
    if (seqNr <= 0) throw ReactiveStreamsCompliance.numberOfElementsInRequestMustBePositiveException
  }

  /**
   * INTERNAL API
   *
   * Ack that failure or completion has been seen and the remote side can stop
   */
  @InternalApi
  private[akka] case object Ack extends StreamRefsProtocol with DeadLetterSuppression

}
