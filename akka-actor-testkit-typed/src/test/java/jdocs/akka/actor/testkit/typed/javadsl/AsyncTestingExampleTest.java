/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.actor.testkit.typed.javadsl;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
// #test-header
import akka.actor.testkit.typed.javadsl.ActorTestKit;

// #test-header
import akka.actor.testkit.typed.javadsl.TestProbe;
import org.junit.AfterClass;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

import java.util.Objects;

import static org.junit.Assert.assertEquals;

// #test-header
public class AsyncTestingExampleTest
    // #test-header
    extends JUnitSuite
// #test-header
{
  static final ActorTestKit testKit = ActorTestKit.create();
  // #test-header

  // #under-test
  public static class Echo {
    public static class Ping {
      public final String message;
      public final ActorRef<Pong> replyTo;

      public Ping(String message, ActorRef<Pong> replyTo) {
        this.message = message;
        this.replyTo = replyTo;
      }
    }

    public static class Pong {
      public final String message;

      public Pong(String message) {
        this.message = message;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pong)) return false;
        Pong pong = (Pong) o;
        return message.equals(pong.message);
      }

      @Override
      public int hashCode() {
        return Objects.hash(message);
      }
    }

    public static Behavior<Ping> create() {
      return Behaviors.receive(Ping.class)
          .onMessage(
              Ping.class,
              ping -> {
                ping.replyTo.tell(new Pong(ping.message));
                return Behaviors.same();
              })
          .build();
    }
  }
  // #under-test

  // #under-test-2

  static class Message {
    int i;
    ActorRef<Integer> replyTo;

    Message(int i, ActorRef<Integer> replyTo) {
      this.i = i;
      this.replyTo = replyTo;
    }
  }

  public static class Producer {

    private Scheduler scheduler;
    private ActorRef<Message> publisher;

    Producer(Scheduler scheduler, ActorRef<Message> publisher) {
      this.scheduler = scheduler;
      this.publisher = publisher;
    }

    public void produce(int messages) {
      IntStream.range(0, messages).forEach(this::publish);
    }

    private CompletionStage<Integer> publish(int i) {
      return AskPattern.ask(
          publisher,
          (ActorRef<Integer> ref) -> new Message(i, ref),
          Duration.ofSeconds(3),
          scheduler);
    }
  }
  // #under-test-2

  // #test-shutdown
  @AfterClass
  public static void cleanup() {
    testKit.shutdownTestKit();
  }
  // #test-shutdown

  @Test
  public void testVerifyingAResponse() {
    // #test-spawn
    ActorRef<Echo.Ping> pinger = testKit.spawn(Echo.create(), "ping");
    TestProbe<Echo.Pong> probe = testKit.createTestProbe();
    pinger.tell(new Echo.Ping("hello", probe.ref()));
    probe.expectMessage(new Echo.Pong("hello"));
    // #test-spawn
  }

  @Test
  public void testVerifyingAResponseAnonymous() {
    // #test-spawn-anonymous
    ActorRef<Echo.Ping> pinger = testKit.spawn(Echo.create());
    // #test-spawn-anonymous
    TestProbe<Echo.Pong> probe = testKit.createTestProbe();
    pinger.tell(new Echo.Ping("hello", probe.ref()));
    probe.expectMessage(new Echo.Pong("hello"));
  }

  @Test
  public void testStoppingActors() {
    TestProbe<Echo.Pong> probe = testKit.createTestProbe();
    // #test-stop-actors
    ActorRef<Echo.Ping> pinger1 = testKit.spawn(Echo.create(), "pinger");
    pinger1.tell(new Echo.Ping("hello", probe.ref()));
    probe.expectMessage(new Echo.Pong("hello"));
    testKit.stop(pinger1);

    // Immediately creating an actor with the same name
    ActorRef<Echo.Ping> pinger2 = testKit.spawn(Echo.create(), "pinger");
    pinger2.tell(new Echo.Ping("hello", probe.ref()));
    probe.expectMessage(new Echo.Pong("hello"));
    testKit.stop(pinger2, Duration.ofSeconds(10));
    // #test-stop-actors
  }

  @Test
  public void testObserveMockedBehavior() {
    // #test-observe-mocked-behavior
    // simulate the happy path
    Behavior<Message> mockedBehavior =
        Behaviors.receiveMessage(
            message -> {
              message.replyTo.tell(message.i);
              return Behaviors.same();
            });
    TestProbe<Message> probe = testKit.createTestProbe();
    ActorRef<Message> mockedPublisher =
        testKit.spawn(Behaviors.monitor(Message.class, probe.ref(), mockedBehavior));

    // test our component
    Producer producer = new Producer(testKit.scheduler(), mockedPublisher);
    int messages = 3;
    producer.produce(messages);

    // verify expected behavior
    IntStream.range(0, messages)
        .forEach(
            i -> {
              Message msg = probe.expectMessageClass(Message.class);
              assertEquals(i, msg.i);
            });
    // #test-observe-mocked-behavior
  }

  @Test
  public void systemNameShouldComeFromTestClass() {
    assertEquals(testKit.system().name(), "AsyncTestingExampleTest");
  }
  // #test-header
}
// #test-header
