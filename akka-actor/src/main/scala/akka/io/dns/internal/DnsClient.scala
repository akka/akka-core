/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.{ InetAddress, InetSocketAddress }

import scala.annotation.nowarn
import scala.collection.{ immutable => im }
import scala.concurrent.duration._
import scala.util.Try

import akka.actor.{ Actor, ActorRef, NoSerializationVerificationNeeded, Props, Stash }
import akka.actor.Status.Failure
import akka.annotation.InternalApi
import akka.event.LogMarker.Security
import akka.event.Logging
import akka.io.{ IO, Tcp, Udp }
import akka.io.dns.{ RecordClass, RecordType, ResourceRecord }
import akka.pattern.{ BackoffOpts, BackoffSupervisor }

/**
 * INTERNAL API
 */
@InternalApi private[akka] object DnsClient {
  sealed trait DnsQuestion {
    def id: Short
  }
  final case class SrvQuestion(id: Short, name: String) extends DnsQuestion
  final case class Question4(id: Short, name: String) extends DnsQuestion
  final case class Question6(id: Short, name: String) extends DnsQuestion
  final case class Answer(id: Short, rrs: im.Seq[ResourceRecord], additionalRecs: im.Seq[ResourceRecord] = Nil)
      extends NoSerializationVerificationNeeded

  final case class DropRequest(question: DnsQuestion)

  // sent as an indication that as of the time of sending, `id` is not being used
  //  by an active question.  Useful for a questioner which tracks which ids it can use
  final case class Dropped(id: Short) extends NoSerializationVerificationNeeded

  private final case class UdpAnswer(to: Seq[Question], content: Answer)

  private def withAndWithoutTrailingDots(question: Question): Seq[(String, RecordType)] = {
    val unchangedPair = question.name -> question.qType

    if (question.name.last == '.') Seq(unchangedPair, question.name.dropRight(1) -> question.qType)
    else Seq(unchangedPair, (question.name + '.') -> question.qType)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class DnsClient(ns: InetSocketAddress) extends Actor with Stash {

  import DnsClient._
  import context.system

  val log = Logging.withMarker(context.system, this)

  val udp = IO(Udp)
  val tcp = IO(Tcp)

  private[internal] var inflightRequests: Map[Short, (ActorRef, Message)] = Map.empty

  lazy val tcpDnsClient: ActorRef = createTcpClient()

  override def preStart() = {
    udp ! Udp.Bind(self, new InetSocketAddress(InetAddress.getByAddress(Array.ofDim(4)), 0))
  }

  def receive: Receive = {
    case Udp.Bound(local) =>
      log.debug("Bound to UDP address [{}]", local)
      context.become(ready(sender()))
      unstashAll()
    case _: Question4 =>
      stash()
    case _: Question6 =>
      stash()
    case _: SrvQuestion =>
      stash()
  }

  private def message(name: String, id: Short, recordType: RecordType): Message = {
    Message(id, MessageFlags(), im.Seq(Question(name, recordType, RecordClass.IN)))
  }

  /**
   * Silent to allow map update syntax
   */
  @nowarn()
  def ready(socket: ActorRef): Receive = {
    case DropRequest(question) =>
      val id = question.id
      inflightRequests.get(id) match {
        case Some((_, sentMsg)) =>
          val sentQs = sentMsg.questions.map { question =>
            question.name -> question.qType
          }

          val expectedQ =
            question match {
              case Question4(_, name)   => name -> RecordType.A
              case Question6(_, name)   => name -> RecordType.AAAA
              case SrvQuestion(_, name) => name -> RecordType.SRV
            }

          if (sentQs.contains(expectedQ)) {
            log.debug("Dropping request [{}]", id)
            inflightRequests -= id
            sender() ! Dropped(id)
          } else if (log.isInfoEnabled) {
            log.info(
              s"Requested to drop request for id [$id] expecting [$expectedQ] but found requests for [${sentQs.mkString(", ")}]... ignoring drop request")
          } // else silently ignore

        case None =>
          sender() ! Dropped(id)
      }

    case Question4(id, name) =>
      if (inflightRequests.contains(id)) {
        log.warning(
          "DNS transaction ID collision encountered for ID [{}], ignoring. This likely indicates a bug.",
          id: Any)
      } else {
        log.debug("Resolving [{}] (A)", name)

        val msg = message(name, id, RecordType.A)
        inflightRequests += (id -> (sender() -> msg))
        log.debug("Message [{}] to [{}]: [{}]", id, ns, msg)
        socket ! Udp.Send(msg.write(), ns)
      }

    case Question6(id, name) =>
      if (inflightRequests.contains(id)) {
        log.warning(
          "DNS transaction ID collision encountered for ID [{}], ignoring. This likely indicates a bug.",
          id: Any)
      } else {
        log.debug("Resolving [{}] (AAAA)", name)

        val msg = message(name, id, RecordType.AAAA)
        inflightRequests += (id -> (sender() -> msg))
        log.debug("Message to [{}]: [{}]", ns, msg)
        socket ! Udp.Send(msg.write(), ns)
      }

    case SrvQuestion(id, name) =>
      if (inflightRequests.contains(id)) {
        log.warning(
          "DNS transaction ID collision encountered for ID [{}], ignoring. This likely indicates a bug.",
          id: Any)
      } else {
        log.debug("Resolving [{}] (SRV)", name)
        val msg = message(name, id, RecordType.SRV)
        inflightRequests += (id -> (sender() -> msg))
        log.debug("Message to [{}]: [{}]", ns, msg)
        socket ! Udp.Send(msg.write(), ns)
      }

    case Udp.CommandFailed(cmd) =>
      log.debug("Command failed [{}]", cmd)
      cmd match {
        case send: Udp.Send =>
          // best effort, don't throw
          Try {
            val msg = Message.parse(send.payload)
            inflightRequests.get(msg.id).foreach {
              case (s, _) =>
                s ! Failure(new RuntimeException("Send failed to nameserver"))
                inflightRequests -= msg.id
            }
          }
        case _ =>
          log.warning("Dns client failed to send {}", cmd)
      }
    case Udp.Received(data, remote) =>
      log.debug("Received message from [{}]: [{}]", remote, data)
      val msg = Message.parse(data)
      log.debug("Decoded UDP DNS response [{}]", msg)

      if (msg.flags.isTruncated) {
        log.debug("DNS response truncated, falling back to TCP")
        inflightRequests.get(msg.id) match {
          case Some((_, msg)) =>
            tcpDnsClient ! msg
          case _ =>
            log.debug("Client for id {} not found. Discarding unsuccessful response.", msg.id)
        }
      } else {
        val (recs, additionalRecs) =
          if (msg.flags.responseCode == ResponseCode.SUCCESS) (msg.answerRecs, msg.additionalRecs) else (Nil, Nil)
        self ! UdpAnswer(msg.questions, Answer(msg.id, recs, additionalRecs))
      }
    case UdpAnswer(questions, response) =>
      inflightRequests.get(response.id) match {
        case Some((reply, sentMsg)) =>
          val sentQs = sentMsg.questions.flatMap(withAndWithoutTrailingDots).toSet
          val answeredQs = questions.flatMap(withAndWithoutTrailingDots).toSet

          if (answeredQs.isEmpty || answeredQs.intersect(sentQs).nonEmpty) {
            reply ! response
            inflightRequests -= response.id
          } else {
            log.warning(
              Security,
              "Martian DNS response for id [{}].  Expected names [{}], received names [{}].  Discarding response",
              response.id,
              sentQs,
              answeredQs)
          }

        case None =>
          log.debug("Client for id [{}] not found. Discarding response.", response.id)
      }

    // for TCP, we don't have to use the question for correlation
    case response: Answer =>
      inflightRequests.get(response.id) match {
        case Some((reply, sentMsg)) =>
          reply ! response
          inflightRequests -= response.id

        case None =>
          log.debug("Client for id [{}] not found. Discarding response.", response.id)
      }

    case Udp.Unbind  => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
  }

  def createTcpClient() = {
    context.actorOf(
      BackoffSupervisor.props(
        BackoffOpts.onFailure(
          Props(classOf[TcpDnsClient], tcp, ns, self),
          childName = "tcpDnsClient",
          minBackoff = 10.millis,
          maxBackoff = 20.seconds,
          randomFactor = 0.1)),
      "tcpDnsClientSupervisor")
  }
}
