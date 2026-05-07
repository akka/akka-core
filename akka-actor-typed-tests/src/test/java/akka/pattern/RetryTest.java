/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import static org.junit.Assert.assertEquals;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.testkit.AkkaSpec;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class RetryTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit = new TestKitJunitResource(AkkaSpec.testConf());

  @Test
  public void testRetryIsCallableWithTypedSystem() throws Exception {

    // just a successful try to cover API and implicit system provider
    CompletionStage<Integer> retried =
        Patterns.retry(
            () -> CompletableFuture.completedFuture(5),
            RetrySettings.create(5).withFixedDelay(Duration.ofMillis(40)),
            testKit.system());

    assertEquals(Integer.valueOf(5), retried.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }
}
