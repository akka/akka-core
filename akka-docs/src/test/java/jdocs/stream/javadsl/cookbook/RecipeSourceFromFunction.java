/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeSourceFromFunction extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system =
        ActorSystem.create(
            "RecipeSourceFromFunction",
            ConfigFactory.parseString(
                "akka.loglevel=DEBUG\nakka.loggers = [akka.testkit.TestEventListener]"));
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void beMappingOfRepeat() throws Exception {
    new TestKit(system) {
      final String builderFunction() {
        return UUID.randomUUID().toString();
      }

      {
        // #source-from-function
        final Source<String, NotUsed> source =
            Source.repeat(NotUsed.getInstance()).map(elem -> builderFunction());
        // #source-from-function

        final List<String> result =
            source
                .take(2)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .get(3, TimeUnit.SECONDS);

        Assert.assertEquals(2, result.size());
      }
    };
  }
}
