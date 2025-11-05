/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.japi.Pair;
import akka.stream.StreamTest;
import akka.stream.ThrottleMode;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;

public class SourceWithContextThrottleTest extends StreamTest {

  public SourceWithContextThrottleTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("ThrottleTest", AkkaSpec.testConf());

  @Test
  public void mustBeAbleToUseThrottle() throws Exception {
    List<Pair<Integer, String>> list =
        Arrays.asList(
            new Pair<>(0, "context-a"), new Pair<>(1, "context-b"), new Pair<>(2, "context-c"));
    Pair<Integer, String> result =
        SourceWithContext.fromPairs(Source.from(list))
            .throttle(10, Duration.ofSeconds(1), 10, ThrottleMode.shaping())
            .throttle(10, Duration.ofSeconds(1), 10, ThrottleMode.enforcing())
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals(list.get(0), result);
  }
}
