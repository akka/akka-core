/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

// format: OFF
import static org.junit.Assert.assertEquals;
import static jdocs.akka.typed.AggregatorTest.IllustrateUsage.Hotel1;
import static jdocs.akka.typed.AggregatorTest.IllustrateUsage.Hotel2;
import static jdocs.akka.typed.AggregatorTest.IllustrateUsage.HotelCustomer;
// format: ON

public class AggregatorTest extends JUnitSuite {
  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void testCollectReplies() {
    TestProbe<List<String>> aggregateProbe = testKit.createTestProbe();
    Consumer<ActorRef<String>> sendRequests =
        replyTo -> {
          replyTo.tell("a");
          replyTo.tell("b");
          replyTo.tell("c");
        };
    Function<List<String>, List<String>> aggregateReplies = ArrayList::new;

    testKit.spawn(
        Aggregator.create(
            String.class,
            sendRequests,
            3,
            aggregateProbe.getRef(),
            aggregateReplies,
            Duration.ofSeconds(3)));

    aggregateProbe.expectMessage(Arrays.asList("a", "b", "c"));
  }

  @Test
  public void testTimeout() {
    TestProbe<List<String>> aggregateProbe = testKit.createTestProbe();
    Consumer<ActorRef<String>> sendRequests =
        replyTo -> {
          replyTo.tell("a");
          replyTo.tell("c");
        };
    Function<List<String>, List<String>> aggregateReplies = ArrayList::new;

    testKit.spawn(
        Aggregator.create(
            String.class,
            sendRequests,
            3,
            aggregateProbe.getRef(),
            aggregateReplies,
            Duration.ofSeconds(1)));

    aggregateProbe.expectNoMessage(Duration.ofMillis(100));
    aggregateProbe.expectMessage(Arrays.asList("a", "c"));
  }

  interface IllustrateUsage {
    // #usage
    public class Hotel1 {
      public static class RequestQuote {
        public final ActorRef<Quote> replyTo;

        public RequestQuote(ActorRef<Quote> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Quote {
        public final String hotel;
        public final BigDecimal price;

        public Quote(String hotel, BigDecimal price) {
          this.hotel = hotel;
          this.price = price;
        }
      }
    }

    public class Hotel2 {
      public static class RequestPrice {
        public final ActorRef<Price> replyTo;

        public RequestPrice(ActorRef<Price> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Price {
        public final String hotel;
        public final BigDecimal price;

        public Price(String hotel, BigDecimal price) {
          this.hotel = hotel;
          this.price = price;
        }
      }
    }

    public class HotelCustomer extends AbstractBehavior<HotelCustomer.Command> {

      interface Command {}

      public static class Quote {
        public final String hotel;
        public final BigDecimal price;

        public Quote(String hotel, BigDecimal price) {
          this.hotel = hotel;
          this.price = price;
        }
      }

      public static class AggregatedQuotes implements Command {
        public final List<Quote> quotes;

        public AggregatedQuotes(List<Quote> quotes) {
          this.quotes = quotes;
        }
      }

      public static Behavior<Command> create(
          ActorRef<Hotel1.RequestQuote> hotel1, ActorRef<Hotel2.RequestPrice> hotel2) {
        return Behaviors.setup(context -> new HotelCustomer(context, hotel1, hotel2));
      }

      public HotelCustomer(
          ActorContext<Command> context,
          ActorRef<Hotel1.RequestQuote> hotel1,
          ActorRef<Hotel2.RequestPrice> hotel2) {
        super(context);

        Consumer<ActorRef<Object>> sendRequests =
            replyTo -> {
              hotel1.tell(new Hotel1.RequestQuote(replyTo.narrow()));
              hotel2.tell(new Hotel2.RequestPrice(replyTo.narrow()));
            };

        int expectedReplies = 2;
        // Object since no common type between Hotel1 and Hotel2
        context.spawnAnonymous(
            Aggregator.create(
                Object.class,
                sendRequests,
                expectedReplies,
                context.getSelf(),
                this::aggregateReplies,
                Duration.ofSeconds(5)));
      }

      private AggregatedQuotes aggregateReplies(List<Object> replies) {
        List<Quote> quotes =
            replies.stream()
                .map(
                    r -> {
                      // The hotels have different protocols with different replies,
                      // convert them to `HotelCustomer.Quote` that this actor understands.
                      if (r instanceof Hotel1.Quote) {
                        Hotel1.Quote q = (Hotel1.Quote) r;
                        return new Quote(q.hotel, q.price);
                      } else if (r instanceof Hotel2.Price) {
                        Hotel2.Price p = (Hotel2.Price) r;
                        return new Quote(p.hotel, p.price);
                      } else {
                        throw new IllegalArgumentException("Unknown reply " + r);
                      }
                    })
                .sorted((a, b) -> a.price.compareTo(b.price))
                .collect(Collectors.toList());

        return new AggregatedQuotes(quotes);
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(AggregatedQuotes.class, this::onAggregatedQuotes)
            .build();
      }

      private Behavior<Command> onAggregatedQuotes(AggregatedQuotes aggregated) {
        if (aggregated.quotes.isEmpty()) getContext().getLog().info("Best Quote N/A");
        else getContext().getLog().info("Best {}", aggregated.quotes.get(0));
        return this;
      }
    }
    // #usage
  }

  @Test
  public void testUsageExample() {
    TestProbe<Hotel1.RequestQuote> hotel1 = testKit.createTestProbe();
    TestProbe<Hotel2.RequestPrice> hotel2 = testKit.createTestProbe();

    TestProbe<HotelCustomer.Command> spy = testKit.createTestProbe();

    testKit.spawn(
        Behaviors.monitor(
            HotelCustomer.Command.class,
            spy.getRef(),
            HotelCustomer.create(hotel1.getRef(), hotel2.getRef())));

    hotel1.receiveMessage().replyTo.tell(new Hotel1.Quote("#1", new BigDecimal(100)));
    hotel2.receiveMessage().replyTo.tell(new Hotel2.Price("#2", new BigDecimal(95)));
    List<HotelCustomer.Quote> quotes =
        spy.expectMessageClass(HotelCustomer.AggregatedQuotes.class).quotes;
    assertEquals("#2", quotes.get(0).hotel);
    assertEquals("#1", quotes.get(1).hotel);
    assertEquals(2, quotes.size());
  }
}
