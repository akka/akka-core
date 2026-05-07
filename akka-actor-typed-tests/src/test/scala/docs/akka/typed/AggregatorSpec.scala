/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed

import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

object AggregatorSpec {
  object IllustrateUsage {
    //#usage
    object Hotel1 {
      final case class RequestQuote(replyTo: ActorRef[Quote])
      final case class Quote(hotel: String, price: BigDecimal)
    }
    object Hotel2 {
      final case class RequestPrice(replyTo: ActorRef[Price])
      final case class Price(hotel: String, price: BigDecimal)
    }

    // Any since no common type between Hotel1 and Hotel2
    type Reply = Any

    object HotelCustomer {
      sealed trait Command
      final case class Quote(hotel: String, price: BigDecimal)
      final case class AggregatedQuotes(quotes: List[Quote]) extends Command

      def apply(hotel1: ActorRef[Hotel1.RequestQuote], hotel2: ActorRef[Hotel2.RequestPrice]): Behavior[Command] = {

        Behaviors.setup[Command] { context =>
          context.spawnAnonymous(
            Aggregator[Reply, AggregatedQuotes](
              sendRequests = { replyTo =>
                hotel1 ! Hotel1.RequestQuote(replyTo)
                hotel2 ! Hotel2.RequestPrice(replyTo)
              },
              expectedReplies = 2,
              context.self,
              aggregateReplies = replies =>
                // The hotels have different protocols with different replies,
                // convert them to `HotelCustomer.Quote` that this actor understands.
                AggregatedQuotes(
                  replies
                    .map {
                      case Hotel1.Quote(hotel, price) => Quote(hotel, price)
                      case Hotel2.Price(hotel, price) => Quote(hotel, price)
                      case unknown                    => throw new RuntimeException(s"Unknown reply $unknown")
                    }
                    .sortBy(_.price)
                    .toList),
              timeout = 5.seconds))

          Behaviors.receiveMessage {
            case AggregatedQuotes(quotes) =>
              context.log.info("Best {}", quotes.headOption.getOrElse("Quote N/A"))
              Behaviors.same
          }
        }
      }
    }
    //#usage
  }
}

class AggregatorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "Aggregator" must {

    "collect replies" in {
      val aggregateProbe = createTestProbe[List[String]]()
      def sendRequests(replyTo: ActorRef[String]): Unit = {
        replyTo ! "a"
        replyTo ! "b"
        replyTo ! "c"
      }
      spawn(Aggregator[String, List[String]](sendRequests, 3, aggregateProbe.ref, replies => replies.toList, 3.seconds))
      aggregateProbe.expectMessage(List("a", "b", "c"))
    }

    "timeout if not all replies received" in {
      val aggregateProbe = createTestProbe[List[String]]()
      def sendRequests(replyTo: ActorRef[String]): Unit = {
        replyTo ! "a"
        replyTo ! "c"
      }
      spawn(Aggregator[String, List[String]](sendRequests, 3, aggregateProbe.ref, replies => replies.toList, 1.seconds))
      aggregateProbe.expectNoMessage(100.millis)
      aggregateProbe.expectMessage(List("a", "c"))
    }

    "test usage example" in {
      import AggregatorSpec.IllustrateUsage._
      val hotel1 = createTestProbe[Hotel1.RequestQuote]()
      val hotel2 = createTestProbe[Hotel2.RequestPrice]()

      val spy = createTestProbe[HotelCustomer.Command]()

      spawn(Behaviors.monitor(spy.ref, HotelCustomer(hotel1.ref, hotel2.ref)))

      hotel1.receiveMessage().replyTo ! Hotel1.Quote("#1", 100)
      hotel2.receiveMessage().replyTo ! Hotel2.Price("#2", 95)
      val quotes = spy.expectMessageType[HotelCustomer.AggregatedQuotes].quotes
      quotes.head should ===(HotelCustomer.Quote("#2", 95))
      quotes.tail.head should ===(HotelCustomer.Quote("#1", 100))
      quotes.size should ===(2)
    }

  }
}
