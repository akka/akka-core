/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators

import akka.actor.ActorSystem

object MergeSequenceDocExample {

  implicit val system: ActorSystem = ???

  // #merge-sequence
  import akka.NotUsed
  import akka.stream.ClosedShape
  import akka.stream.scaladsl.{ Flow, GraphDSL, MergeSequence, Partition, RunnableGraph, Sink, Source }

  val subscription: Source[Message, NotUsed] = createSubscription()
  val messageProcessor: Flow[(Message, Long), (Message, Long), NotUsed] =
    createMessageProcessor()
  val messageAcknowledger: Sink[Message, NotUsed] = createMessageAcknowledger()

  RunnableGraph
    .fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      // Partitions stream into messages that should or should not be processed
      val partition = builder.add(Partition[(Message, Long)](2, {
        case (message, _) if shouldProcess(message) => 0
        case _                                      => 1
      }))
      // Merges stream by the index produced by zipWithIndex
      val merge = builder.add(MergeSequence[(Message, Long)](2)(_._2))

      subscription.zipWithIndex ~> partition.in
      // First goes through message processor
      partition.out(0) ~> messageProcessor ~> merge
      // Second partition bypasses message processor
      partition.out(1) ~> merge
      merge.out.map(_._1) ~> messageAcknowledger
      ClosedShape
    })
    .run()

  // #merge-sequence

  def shouldProcess(message: Message): Boolean = true
  trait Message
  def createSubscription(): Source[Message, NotUsed] = ???
  def createMessageProcessor(): Flow[(Message, Long), (Message, Long), NotUsed] = ???
  def createMessageAcknowledger(): Sink[Message, NotUsed] = ???

}
