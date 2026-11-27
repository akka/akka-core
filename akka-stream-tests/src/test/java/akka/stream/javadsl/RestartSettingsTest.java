/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.event.Logging;
import akka.stream.RestartSettings;
import akka.stream.StreamTest;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.Test;

public class RestartSettingsTest extends StreamTest {
  public RestartSettingsTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("RestartSettingsTest", AkkaSpec.testConf());

  @Test
  public void mustBeAbleToUseFailureLogging() throws Exception {
    final LinkedBlockingQueue<RestartSettings.RestartFailure> failures =
        new LinkedBlockingQueue<>();
    final RestartSettings settings =
        RestartSettings.create(Duration.ofMillis(10), Duration.ofMillis(20), 0.0)
            .withLogSettings(
                RestartSettings.createLogSettings(Logging.WarningLevel())
                    .withFailureLogging(
                        failure -> {
                          failures.add(failure);
                          failure.logDefault();
                        }));

    final AtomicInteger created = new AtomicInteger();
    final String result =
        RestartSource.onFailuresWithBackoff(
                settings,
                () ->
                    created.incrementAndGet() == 1
                        ? Source.<String>failed(new RuntimeException("expected"))
                        : Source.single("ok"))
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals("ok", result);
    final RestartSettings.RestartFailure failure = failures.poll(5, TimeUnit.SECONDS);
    assertEquals("expected", failure.cause().getMessage());
    assertEquals(1, failure.consecutiveFailures());
  }
}
