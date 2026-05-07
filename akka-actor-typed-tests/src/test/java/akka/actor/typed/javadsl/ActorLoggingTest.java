/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.event.Logging;
import akka.japi.pf.PFBuilder;
import akka.testkit.CustomEventFilter;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import scala.concurrent.duration.FiniteDuration;

public class ActorLoggingTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(
          ConfigFactory.parseString(
              "akka.loglevel = INFO\n" + "akka.loggers = [\"akka.testkit.TestEventListener\"]"));

  interface Protocol {
    String getTransactionId();
  }

  static class Message implements Protocol {
    public final String transactionId;

    public Message(String transactionId) {
      this.transactionId = transactionId;
    }

    public String getTransactionId() {
      return transactionId;
    }
  }

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  @Test
  public void loggingProvidesClassWhereLogWasCalled() {
    CustomEventFilter eventFilter =
        new CustomEventFilter(
            new PFBuilder<Logging.LogEvent, Object>()
                .match(
                    Logging.LogEvent.class, (event) -> event.logClass() == ActorLoggingTest.class)
                .build(),
            1);

    Behavior<String> behavior =
        Behaviors.setup(
            (context) -> {
              context.getLog().info("Starting up");

              return Behaviors.empty();
            });

    testKit.spawn(behavior);
    eventFilter.awaitDone(FiniteDuration.create(3, TimeUnit.SECONDS));
  }

  @Test
  public void loggingProvidesMDC() {
    Behavior<Protocol> behavior =
        Behaviors.setup(
            context ->
                Behaviors.withMdc(
                    Protocol.class,
                    (message) -> {
                      Map<String, String> mdc = new HashMap<>();
                      mdc.put("txId", message.getTransactionId());
                      return mdc;
                    },
                    Behaviors.receive(Protocol.class)
                        .onMessage(
                            Message.class,
                            message -> {
                              context.getLog().info(message.toString());
                              return Behaviors.same();
                            })
                        .build()));

    CustomEventFilter eventFilter =
        new CustomEventFilter(
            new PFBuilder<Logging.LogEvent, Object>()
                .match(Logging.LogEvent.class, (event) -> event.getMDC().containsKey("txId"))
                .build(),
            1);

    testKit.spawn(behavior);
    eventFilter.awaitDone(FiniteDuration.create(3, TimeUnit.SECONDS));
  }

  @Test
  public void logMessagesBehavior() {
    Behavior<String> behavior = Behaviors.logMessages(Behaviors.empty());

    CustomEventFilter eventFilter =
        new CustomEventFilter(
            new PFBuilder<Logging.LogEvent, Object>()
                .match(
                    Logging.LogEvent.class,
                    (event) -> event.message().equals("received message Hello"))
                .build(),
            1);

    ActorRef<String> ref = testKit.spawn(behavior);
    ref.tell("Hello");
    eventFilter.awaitDone(FiniteDuration.create(3, TimeUnit.SECONDS));
  }
}
