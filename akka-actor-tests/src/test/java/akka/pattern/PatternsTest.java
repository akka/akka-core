/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern;

import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import akka.actor.*;
import akka.dispatch.Futures;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.TestProbe;
import akka.util.Timeout;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

public class PatternsTest extends JUnitSuite {

  public static final class ExplicitAskTestActor extends AbstractActor {

    public static final class Message implements NoSerializationVerificationNeeded {

      public Message(final String text, ActorRef replyTo) {
        this.text = text;
        this.replyTo = replyTo;
      }

      public final String text;
      public final ActorRef replyTo;
    }

    public Receive createReceive() {
      return receiveBuilder()
          .match(Message.class, message -> message.replyTo.tell(message.text, getSelf()))
          .build();
    }
  }

  public static final class StopActor extends AbstractActor {

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(String.class, message -> sender().tell("Pong", getSelf()))
          .build();
    }
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("JavaAPI", AkkaSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();

  private final ExecutionContext ec = system.dispatcher();

  @Test
  public void useAsk() throws Exception {
    ActorRef testActor = system.actorOf(Props.create(JavaAPITestActor.class), "test");
    scala.concurrent.duration.Duration timeout =
        scala.concurrent.duration.Duration.create(3, "seconds");
    assertEquals(
        "Ask should return expected answer",
        JavaAPITestActor.ANSWER,
        Await.result(ask(testActor, "hey!", 3000), timeout));
  }

  @Test
  public void useAskWithActorSelection() throws Exception {
    scala.concurrent.duration.Duration timeout =
        scala.concurrent.duration.Duration.create(3, "seconds");
    ActorRef testActor = system.actorOf(Props.create(JavaAPITestActor.class), "test2");
    ActorSelection selection = system.actorSelection("/user/test2");
    ActorIdentity id =
        (ActorIdentity) Await.result(ask(selection, new Identify("yo!"), 3000), timeout);
    assertEquals(
        "Ask (Identify) should return the proper ActorIdentity", testActor, id.getActorRef().get());
  }

  @Test
  public void testCSAsk() throws Exception {
    ActorRef target = system.actorOf(Props.create(JavaAPITestActor.class));
    CompletionStage<String> result =
        Patterns.ask(target, "hello", Duration.ofSeconds(3)).thenApply(o -> (String) o);

    String actual = result.toCompletableFuture().get(3, SECONDS);
    assertEquals(JavaAPITestActor.ANSWER, actual);
  }

  @Test
  public void testCSAskWithActorSelection() throws Exception {
    ActorRef target = system.actorOf(Props.create(JavaAPITestActor.class), "test3");

    ActorSelection selection = system.actorSelection("/user/test3");
    ActorIdentity id =
        Patterns.ask(selection, new Identify("hello"), Duration.ofSeconds(3))
            .toCompletableFuture()
            .thenApply(o -> (ActorIdentity) o)
            .get(3, SECONDS);

    assertEquals(target, id.getActorRef().get());
  }

  @Test
  public void testCSAskWithReplyToTimeout() throws Exception {
    final String expected = "hello";

    final ActorRef echo = system.actorOf(Props.create(ExplicitAskTestActor.class));
    final CompletionStage<String> response =
        Patterns.askWithReplyTo(
                echo,
                replyTo -> new ExplicitAskTestActor.Message(expected, replyTo),
                Duration.ofSeconds(3))
            .thenApply(o -> (String) o);

    final String actual = response.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testCSAskWithReplyToTimeoutMillis() throws Exception {
    final String expected = "hello";

    final ActorRef echo = system.actorOf(Props.create(ExplicitAskTestActor.class));
    final CompletionStage<String> response =
        Patterns.askWithReplyTo(
                echo,
                replyTo -> new ExplicitAskTestActor.Message(expected, replyTo),
                Duration.ofSeconds(3))
            .thenApply(o -> (String) o);

    final String actual = response.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testCSAskSelectionWithReplyToTimeoutMillis() throws Exception {
    final String expected = "hello";

    final ActorRef echo = system.actorOf(Props.create(ExplicitAskTestActor.class));
    final ActorSelection selection = system.actorSelection(echo.path());
    final CompletionStage<String> response =
        Patterns.askWithReplyTo(
                selection,
                replyTo -> new ExplicitAskTestActor.Message(expected, replyTo),
                Duration.ofSeconds(3))
            .thenApply(o -> (String) o);

    final String actual = response.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testAskWithReplyToTimeoutMillis() throws Exception {
    final String expected = "hello";

    final ActorRef echo = system.actorOf(Props.create(ExplicitAskTestActor.class));
    final Future<Object> response =
        Patterns.askWithReplyTo(
            echo, replyTo -> new ExplicitAskTestActor.Message(expected, replyTo), 3000);

    final Object actual = Await.result(response, FiniteDuration.apply(3, SECONDS));
    assertEquals(expected, actual);
  }

  @Test
  public void testAskSelectionWithReplyToTimeoutMillis() throws Exception {
    final String expected = "hello";

    final ActorRef echo = system.actorOf(Props.create(ExplicitAskTestActor.class));
    final ActorSelection selection = system.actorSelection(echo.path());
    final Future<Object> response =
        Patterns.askWithReplyTo(
            selection, replyTo -> new ExplicitAskTestActor.Message(expected, replyTo), 3000);

    final Object actual = Await.result(response, FiniteDuration.apply(3, SECONDS));
    assertEquals(expected, actual);
  }

  @Test
  public void testAskWithReplyToTimeout() throws Exception {
    final String expected = "hello";

    final ActorRef echo = system.actorOf(Props.create(ExplicitAskTestActor.class));
    final Future<Object> response =
        Patterns.askWithReplyTo(
            echo,
            replyTo -> new ExplicitAskTestActor.Message(expected, replyTo),
            Timeout.apply(3, SECONDS));

    final Object actual = Await.result(response, FiniteDuration.apply(3, SECONDS));
    assertEquals(expected, actual);
  }

  @Test
  public void usePipe() throws Exception {
    TestProbe probe = new TestProbe(system);
    pipe(Futures.successful("ho!"), system.dispatcher()).to(probe.ref());
    probe.expectMsg("ho!");
  }

  @Test
  public void usePipeWithActorSelection() throws Exception {
    TestProbe probe = new TestProbe(system);
    ActorSelection selection = system.actorSelection(probe.ref().path());
    pipe(Futures.successful("hi!"), system.dispatcher()).to(selection);
    probe.expectMsg("hi!");
  }

  @Test
  public void testCSPipeToActorRef() throws Exception {
    TestProbe probe = new TestProbe(system);
    CompletableFuture<String> f = new CompletableFuture<>();
    f.complete("ho!");
    Patterns.pipe(f, ec).to(probe.ref());
    probe.expectMsg("ho!");
  }

  @Test
  public void testCSPipeToActorSelection() throws Exception {
    TestProbe probe = new TestProbe(system);
    ActorSelection selection = system.actorSelection(probe.ref().path());
    CompletableFuture<String> f = new CompletableFuture<>();
    f.complete("hi!");
    Patterns.pipe(f, ec).to(selection);
    probe.expectMsg("hi!");
  }

  @Test
  public void testRetryCompletionStageNoDelay() throws Exception {
    final String expected = "hello";

    CompletionStage<String> retriedFuture =
        Patterns.retry(() -> CompletableFuture.completedFuture(expected), 3, ec);

    String actual = retriedFuture.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testRetryCompletionStageRandomDelay() throws Exception {
    final String expected = "hello";
    final AtomicInteger counter = new AtomicInteger(0);
    CompletionStage<String> retriedFuture =
        Patterns.retry(
            () -> {
              if (counter.incrementAndGet() <= 3) {
                final CompletableFuture<String> empty = new CompletableFuture<>();
                empty.completeExceptionally(new RuntimeException("failed by purpose."));
                return empty;
              } else {
                return CompletableFuture.completedFuture(expected);
              }
            },
            3,
            Duration.ofMillis(100),
            Duration.ofMillis(200),
            0.2d,
            system.scheduler(),
            ec);

    String actual = retriedFuture.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testRetry() throws Exception {
    final String expected = "hello";

    Future<String> retriedFuture =
        Patterns.retry(
            () -> Futures.successful(expected),
            3,
            scala.concurrent.duration.Duration.apply(200, "millis"),
            system.scheduler(),
            ec);

    String actual = Await.result(retriedFuture, FiniteDuration.apply(3, SECONDS));
    assertEquals(expected, actual);
  }

  @Test
  public void testCSRetry() throws Exception {
    final String expected = "hello";

    Callable<CompletionStage<String>> attempt = () -> CompletableFuture.completedFuture(expected);

    CompletionStage<String> retriedStage =
        Patterns.retry(attempt, 3, Duration.ofMillis(200), system.scheduler(), ec);

    final String actual = retriedStage.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testShortCircuitRetry() throws Exception {
    AtomicInteger failureCounter = new AtomicInteger();

    Predicate<Throwable> decider =
        (ex) -> {
          return !(ex instanceof IllegalArgumentException);
        };

    final CompletableFuture<Object> ise = new CompletableFuture<Object>();
    final CompletableFuture<Object> iae = new CompletableFuture<Object>();
    ise.completeExceptionally(new IllegalStateException());
    iae.completeExceptionally(new IllegalArgumentException());

    CompletionStage<Object> retriedAttempts =
        Patterns.retry(
            () -> { // attempt
              if ((failureCounter.getAndIncrement() % 3) < 2) {
                return ise;
              } else {
                return iae;
              }
            },
            decider, // shouldRetry
            10,
            ec);

    try {
      retriedAttempts.toCompletableFuture().get(3, SECONDS);
      throw new AssertionError("future should have failed!");
    } catch (java.util.concurrent.ExecutionException e) {
      assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
    }

    assertEquals(failureCounter.get(), 3);
  }

  @Test(expected = IllegalStateException.class)
  public void testAfterFailedCallable() throws Exception {
    Callable<Future<String>> failedCallable =
        () -> Futures.failed(new IllegalStateException("Illegal!"));

    Future<String> delayedFuture =
        Patterns.after(
            scala.concurrent.duration.Duration.create(200, "millis"),
            system.scheduler(),
            ec,
            failedCallable);

    Future<String> resultFuture = Futures.firstCompletedOf(Arrays.asList(delayedFuture), ec);
    Await.result(resultFuture, scala.concurrent.duration.FiniteDuration.apply(3, SECONDS));
  }

  @Test(expected = IllegalStateException.class)
  public void testAfterFailedFuture() throws Exception {

    Future<String> delayedFuture =
        Patterns.after(
            scala.concurrent.duration.Duration.create(200, "millis"),
            system.scheduler(),
            ec,
            () -> Futures.failed(new IllegalStateException("Illegal!")));

    Future<String> resultFuture = Futures.firstCompletedOf(Arrays.asList(delayedFuture), ec);
    Await.result(resultFuture, FiniteDuration.apply(3, SECONDS));
  }

  @Test
  public void testAfterSuccessfulCallable() throws Exception {
    final String expected = "Hello";

    Future<String> delayedFuture =
        Patterns.after(
            scala.concurrent.duration.Duration.create(200, "millis"),
            system.scheduler(),
            ec,
            () -> Futures.successful(expected));

    Future<String> resultFuture = Futures.firstCompletedOf(Arrays.asList(delayedFuture), ec);
    final String actual = Await.result(resultFuture, FiniteDuration.apply(3, SECONDS));

    assertEquals(expected, actual);
  }

  @Test
  public void testAfterSuccessfulFuture() throws Exception {
    final String expected = "Hello";

    Future<String> delayedFuture =
        Patterns.after(
            scala.concurrent.duration.Duration.create(200, "millis"),
            system.scheduler(),
            ec,
            () -> Futures.successful(expected));

    Future<String> resultFuture = Futures.firstCompletedOf(Arrays.asList(delayedFuture), ec);

    final String actual = Await.result(resultFuture, FiniteDuration.apply(3, SECONDS));
    assertEquals(expected, actual);
  }

  @Test
  public void testAfterFiniteDuration() throws Exception {
    final String expected = "Hello";

    Future<String> delayedFuture =
        Patterns.after(
            scala.concurrent.duration.Duration.create(200, "millis"),
            system.scheduler(),
            ec,
            () -> Futures.successful("world"));

    Future<String> immediateFuture = Futures.future(() -> expected, ec);

    Future<String> resultFuture =
        Futures.firstCompletedOf(Arrays.asList(delayedFuture, immediateFuture), ec);

    final String actual = Await.result(resultFuture, FiniteDuration.apply(3, SECONDS));
    assertEquals(expected, actual);
  }

  @Test(expected = ExecutionException.class)
  public void testCSAfterFailedCallable() throws Exception {
    Callable<CompletionStage<String>> failedCallable =
        () -> {
          CompletableFuture<String> f = new CompletableFuture<>();
          f.completeExceptionally(new IllegalStateException("Illegal!"));
          return f;
        };

    CompletionStage<String> delayedStage =
        Patterns.after(Duration.ofMillis(200), system.scheduler(), ec, failedCallable);

    delayedStage.toCompletableFuture().get(3, SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void testCSAfterFailedFuture() throws Exception {
    Callable<CompletionStage<String>> failedFuture =
        () -> {
          CompletableFuture<String> f = new CompletableFuture<>();
          f.completeExceptionally(new IllegalStateException("Illegal!"));
          return f;
        };

    CompletionStage<String> delayedStage =
        Patterns.after(Duration.ofMillis(200), system.scheduler(), ec, failedFuture);

    String result = delayedStage.toCompletableFuture().get(3, SECONDS);
  }

  @Test
  public void testCSAfterSuccessfulCallable() throws Exception {
    final String expected = "Hello";

    final Callable<CompletionStage<String>> cf =
        () -> {
          CompletableFuture<String> f = CompletableFuture.completedFuture(expected);
          return f;
        };

    CompletionStage<String> delayedStage =
        Patterns.after(Duration.ofMillis(200), system.scheduler(), ec, cf);

    final String actual = delayedStage.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testCSAfterSuccessfulFuture() throws Exception {
    final String expected = "Hello";

    final CompletionStage<String> f = CompletableFuture.completedFuture(expected);

    CompletionStage<String> delayedStage =
        Patterns.after(
            Duration.ofMillis(200),
            system.scheduler(),
            ec,
            () -> CompletableFuture.completedFuture(expected));

    final String actual = delayedStage.toCompletableFuture().get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testCSAfterDuration() throws Exception {
    final String expected = "Hello";

    final CompletionStage<String> f = CompletableFuture.completedFuture("world!");

    CompletionStage<String> delayedStage =
        Patterns.after(
            Duration.ofMillis(200),
            system.scheduler(),
            ec,
            () -> CompletableFuture.completedFuture("world!"));

    CompletableFuture<String> immediateStage = CompletableFuture.completedFuture(expected);
    CompletableFuture<Object> resultStage =
        CompletableFuture.anyOf(delayedStage.toCompletableFuture(), immediateStage);

    final String actual = (String) resultStage.get(3, SECONDS);
    assertEquals(expected, actual);
  }

  @Test
  public void testGracefulStop() throws Exception {
    ActorRef target = system.actorOf(Props.create(StopActor.class));
    Future<Boolean> result =
        Patterns.gracefulStop(target, FiniteDuration.apply(200, TimeUnit.MILLISECONDS));

    Boolean actual = Await.result(result, FiniteDuration.apply(3, SECONDS));
    assertEquals(true, actual);
  }

  @Test
  public void testCSGracefulStop() throws Exception {
    ActorRef target = system.actorOf(Props.create(StopActor.class));
    CompletionStage<Boolean> result = Patterns.gracefulStop(target, Duration.ofMillis(200));

    Boolean actual = result.toCompletableFuture().get(3, SECONDS);
    assertEquals(true, actual);
  }
}
