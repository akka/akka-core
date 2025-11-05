/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.TestLatch;
import akka.testkit.javadsl.TestKit;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class RecipeSimpleDrop extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeSimpleDrop");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void work() throws Exception {
    new TestKit(system) {
      {
        @SuppressWarnings("unused")
        // #simple-drop
        final Flow<Message, Message, NotUsed> droppyStream =
            Flow.of(Message.class).conflate((lastMessage, newMessage) -> newMessage);
        // #simple-drop
        final TestLatch latch = new TestLatch(2, system);
        final Flow<Message, Message, NotUsed> realDroppyStream =
            Flow.of(Message.class)
                .conflate(
                    (lastMessage, newMessage) -> {
                      latch.countDown();
                      return newMessage;
                    });

        final Pair<TestPublisher.Probe<Message>, TestSubscriber.Probe<Message>> pubSub =
            TestSource.<Message>probe(system)
                .via(realDroppyStream)
                .toMat(TestSink.probe(system), (pub, sub) -> new Pair<>(pub, sub))
                .run(system);
        final TestPublisher.Probe<Message> pub = pubSub.first();
        final TestSubscriber.Probe<Message> sub = pubSub.second();

        pub.sendNext(new Message("1"));
        pub.sendNext(new Message("2"));
        pub.sendNext(new Message("3"));

        Await.ready(latch, Duration.create(1, TimeUnit.SECONDS));

        sub.requestNext(new Message("3"));

        pub.sendComplete();
        sub.request(1);
        sub.expectComplete();
      }
    };
  }
}
