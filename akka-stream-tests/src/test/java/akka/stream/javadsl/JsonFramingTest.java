/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.stream.StreamTest;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.util.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.ClassRule;
import org.junit.Test;

public class JsonFramingTest extends StreamTest {
  public JsonFramingTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("JsonFramingTest", AkkaSpec.testConf());

  @Test
  public void mustBeAbleToParseJsonArray()
      throws InterruptedException, ExecutionException, TimeoutException {
    // #using-json-framing
    String input =
        "[{ \"name\" : \"john\" }, { \"name\" : \"Ég get etið gler án þess að meiða mig\" }, { \"name\" : \"jack\" }]";
    CompletionStage<ArrayList<String>> result =
        Source.single(ByteString.fromString(input))
            .via(JsonFraming.objectScanner(Integer.MAX_VALUE))
            .runFold(
                new ArrayList<String>(),
                (acc, entry) -> {
                  acc.add(entry.utf8String());
                  return acc;
                },
                system);
    // #using-json-framing

    List<String> frames = result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("{ \"name\" : \"john\" }", frames.get(0));
    assertEquals("{ \"name\" : \"Ég get etið gler án þess að meiða mig\" }", frames.get(1));
    assertEquals("{ \"name\" : \"jack\" }", frames.get(2));
  }
}
