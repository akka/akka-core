/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

public class RecipeManualTrigger extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeManualTrigger");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  class Trigger {}

  public final Trigger TRIGGER = new Trigger();

  @Test
  public void zipped() throws Exception {
    new TestKit(system) {
      {
        final Source<Trigger, TestPublisher.Probe<Trigger>> triggerSource =
            TestSource.probe(system);
        final Sink<Message, TestSubscriber.Probe<Message>> messageSink = TestSink.probe(system);

        // #manually-triggered-stream
        final RunnableGraph<Pair<TestPublisher.Probe<Trigger>, TestSubscriber.Probe<Message>>> g =
            RunnableGraph
                .<Pair<TestPublisher.Probe<Trigger>, TestSubscriber.Probe<Message>>>fromGraph(
                    GraphDSL.create(
                        triggerSource,
                        messageSink,
                        (p, s) -> new Pair<>(p, s),
                        (builder, source, sink) -> {
                          SourceShape<Message> elements =
                              builder.add(
                                  Source.from(Arrays.asList("1", "2", "3", "4"))
                                      .map(t -> new Message(t)));
                          FlowShape<Pair<Message, Trigger>, Message> takeMessage =
                              builder.add(
                                  Flow.<Pair<Message, Trigger>>create().map(p -> p.first()));
                          final FanInShape2<Message, Trigger, Pair<Message, Trigger>> zip =
                              builder.add(Zip.create());
                          builder.from(elements).toInlet(zip.in0());
                          builder.from(source).toInlet(zip.in1());
                          builder.from(zip.out()).via(takeMessage).to(sink);
                          return ClosedShape.getInstance();
                        }));
        // #manually-triggered-stream

        Pair<TestPublisher.Probe<Trigger>, TestSubscriber.Probe<Message>> pubSub = g.run(system);
        TestPublisher.Probe<Trigger> pub = pubSub.first();
        TestSubscriber.Probe<Message> sub = pubSub.second();

        FiniteDuration timeout = FiniteDuration.create(100, TimeUnit.MILLISECONDS);
        sub.expectSubscription().request(1000);
        sub.expectNoMessage(timeout);

        pub.sendNext(TRIGGER);
        sub.expectNext(new Message("1"));
        sub.expectNoMessage(timeout);

        pub.sendNext(TRIGGER);
        pub.sendNext(TRIGGER);
        sub.expectNext(new Message("2"));
        sub.expectNext(new Message("3"));
        sub.expectNoMessage(timeout);

        pub.sendNext(TRIGGER);
        sub.expectNext(new Message("4"));
        sub.expectComplete();
      }
    };
  }

  @Test
  public void zipWith() throws Exception {
    new TestKit(system) {
      {
        final Source<Trigger, TestPublisher.Probe<Trigger>> triggerSource =
            TestSource.probe(system);
        final Sink<Message, TestSubscriber.Probe<Message>> messageSink = TestSink.probe(system);

        // #manually-triggered-stream-zipwith
        final RunnableGraph<Pair<TestPublisher.Probe<Trigger>, TestSubscriber.Probe<Message>>> g =
            RunnableGraph
                .<Pair<TestPublisher.Probe<Trigger>, TestSubscriber.Probe<Message>>>fromGraph(
                    GraphDSL.create(
                        triggerSource,
                        messageSink,
                        (p, s) -> new Pair<>(p, s),
                        (builder, source, sink) -> {
                          final SourceShape<Message> elements =
                              builder.add(
                                  Source.from(Arrays.asList("1", "2", "3", "4"))
                                      .map(t -> new Message(t)));
                          final FanInShape2<Message, Trigger, Message> zipWith =
                              builder.add(ZipWith.create((msg, trigger) -> msg));
                          builder.from(elements).toInlet(zipWith.in0());
                          builder.from(source).toInlet(zipWith.in1());
                          builder.from(zipWith.out()).to(sink);
                          return ClosedShape.getInstance();
                        }));
        // #manually-triggered-stream-zipwith

        Pair<TestPublisher.Probe<Trigger>, TestSubscriber.Probe<Message>> pubSub = g.run(system);
        TestPublisher.Probe<Trigger> pub = pubSub.first();
        TestSubscriber.Probe<Message> sub = pubSub.second();

        FiniteDuration timeout = FiniteDuration.create(100, TimeUnit.MILLISECONDS);
        sub.expectSubscription().request(1000);
        sub.expectNoMessage(timeout);

        pub.sendNext(TRIGGER);
        sub.expectNext(new Message("1"));
        sub.expectNoMessage(timeout);

        pub.sendNext(TRIGGER);
        pub.sendNext(TRIGGER);
        sub.expectNext(new Message("2"));
        sub.expectNext(new Message("3"));
        sub.expectNoMessage(timeout);

        pub.sendNext(TRIGGER);
        sub.expectNext(new Message("4"));
        sub.expectComplete();
      }
    };
  }
}
