/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.future;

import akka.actor.typed.ActorSystem;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.util.Timeout;
import jdocs.AbstractJavaTest;
import org.junit.ClassRule;
import org.junit.Test;
import scala.jdk.javaapi.FutureConverters;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.actor.typed.javadsl.Adapter.toTyped;
import static akka.dispatch.Futures.future;
// #imports
import akka.pattern.Patterns;

// #imports
import static java.util.concurrent.TimeUnit.SECONDS;

public class FutureDocTest extends AbstractJavaTest {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("FutureDocTest", AkkaSpec.testConf());

  private final ActorSystem<Void> system = toTyped(actorSystemResource.getSystem());

  @Test(expected = java.util.concurrent.CompletionException.class)
  public void useAfter() throws Exception {
    final ExecutionContext ec = system.executionContext();
    // #after
    CompletionStage<String> failWithException =
        CompletableFuture.supplyAsync(
            () -> {
              throw new IllegalStateException("OHNOES1");
            });
    CompletionStage<String> delayed =
        Patterns.after(Duration.ofMillis(200), system, () -> failWithException);
    // #after
    Future<String> future =
        future(
            () -> {
              Thread.sleep(1000);
              return "foo";
            },
            ec);
    Future<String> result =
        Futures.firstCompletedOf(
            Arrays.<Future<String>>asList(future, FutureConverters.asScala(delayed)), ec);
    Timeout timeout = Timeout.create(Duration.ofSeconds(2));
    Await.result(result, timeout.duration());
  }

  @Test
  public void useRetry() throws Exception {
    // #retry
    Callable<CompletionStage<String>> attempt = () -> CompletableFuture.completedFuture("test");

    CompletionStage<String> retriedFuture =
        Patterns.retry(attempt, 3, java.time.Duration.ofMillis(200), system);
    // #retry

    retriedFuture.toCompletableFuture().get(2, SECONDS);
  }
}
