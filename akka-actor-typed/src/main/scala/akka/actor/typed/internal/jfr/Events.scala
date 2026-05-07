/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.jfr

import scala.annotation.nowarn

import jdk.jfr.Category
import jdk.jfr.Enabled
import jdk.jfr.Event
import jdk.jfr.Label
import jdk.jfr.StackTrace

import akka.annotation.InternalApi

// requires jdk9+ to compile
// for editing these in IntelliJ, open module settings, change JDK dependency to 11 for only this module

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController created")
final class DeliveryProducerCreated(val producerId: String, val actorPath: String) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController started")
final class DeliveryProducerStarted(val producerId: String, val actorPath: String) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController sent RequestNext")
final class DeliveryProducerRequestNext(val producerId: String, val currentSeqNr: Long, val confirmedSeqNr: Long)
    extends Event

/** INTERNAL API */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController sent SequencedMessage")
final class DeliveryProducerSent(val producerId: String, val seqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController waiting for demand")
final class DeliveryProducerWaitingForRequest(val producerId: String, val currentSeqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController resent unconfirmed")
final class DeliveryProducerResentUnconfirmed(val producerId: String, val fromSeqNr: Long, val toSeqNr: Long)
    extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController resent first")
final class DeliveryProducerResentFirst(val producerId: String, val firstSeqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label(
  "Delivery ProducerController resent first unconfirmed")
final class DeliveryProducerResentFirstUnconfirmed(val producerId: String, val seqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController received message")
final class DeliveryProducerReceived(val producerId: String, val currentSeqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController received demand request")
final class DeliveryProducerReceivedRequest(
    val producerId: String,
    val requestedSeqNr: Long,
    @nowarn("msg=never used") confirmedSeqNr: Long)
    extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ProducerController")) @Label("Delivery ProducerController received resend request")
final class DeliveryProducerReceivedResend(val producerId: String, val fromSeqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController created")
final class DeliveryConsumerCreated(val actorPath: String) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController started")
final class DeliveryConsumerStarted(val actorPath: String) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController received")
final class DeliveryConsumerReceived(val producerId: String, val seqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(false) // hi frequency event
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label(
  "Delivery ConsumerController received, previous in progress")
final class DeliveryConsumerReceivedPreviousInProgress(val producerId: String, val seqNr: Long, val stashed: Int)
    extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController received duplicate")
final class DeliveryConsumerDuplicate(val producerId: String, val expectedSeqNr: Long, val seqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController received missing")
final class DeliveryConsumerMissing(val producerId: String, val expectedSeqNr: Long, val seqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label(
  "Delivery ConsumerController received expected resend")
final class DeliveryConsumerReceivedResend(val seqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController sent demand Request")
final class DeliveryConsumerSentRequest(val producerId: String, val requestedSeqNr: Long) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController producer changed")
final class DeliveryConsumerChangedProducer(val producerId: String) extends Event

/** INTERNAL API */
@InternalApi
@Enabled(true)
@StackTrace(false)
@Category(Array("Akka", "Delivery", "ConsumerController")) @Label("Delivery ConsumerController stash is full")
final class DeliveryConsumerStashFull(val producerId: String, val seqNr: Long) extends Event
