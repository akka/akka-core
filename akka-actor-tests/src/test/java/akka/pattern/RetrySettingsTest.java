/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import static org.junit.Assert.assertEquals;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class RetrySettingsTest extends JUnitSuite {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("RetrySettingsTest", AkkaSpec.testConf());

  @Test
  public void shouldCreateRetrySetting() {
    // mostly for testing compilation issues
    RetrySettings.create(2);
    RetrySettings.create(ConfigFactory.parseString("max-retries = 2"));
    RetrySettings.create(2).withFixedDelay(Duration.ofSeconds(1));
    RetrySettings.create(2)
        .withExponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(2), 0.1);
    RetrySettings.create(2).withJavaDelayFunction(retryNum -> Optional.of(Duration.ofSeconds(1)));
    RetrySettings.create(2).withJavaDecider(__ -> true);
  }

  @Test
  public void retryCallableWithClassicSystem() throws Exception {

    // make sure the overload is callable
    CompletionStage<String> result =
        Patterns.retry(
            () -> CompletableFuture.completedFuture("woho"),
            RetrySettings.create(2),
            actorSystemResource.getSystem());

    assertEquals("woho", result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }
}
