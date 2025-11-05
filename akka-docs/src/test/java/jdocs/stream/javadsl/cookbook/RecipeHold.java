/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.stage.*;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.javadsl.TestKit;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

public class RecipeHold extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeHold");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  // #hold-version-1
  class HoldWithInitial<T> extends GraphStage<FlowShape<T, T>> {

    public Inlet<T> in = Inlet.<T>create("HoldWithInitial.in");
    public Outlet<T> out = Outlet.<T>create("HoldWithInitial.out");
    private FlowShape<T, T> shape = FlowShape.of(in, out);

    private final T initial;

    public HoldWithInitial(T initial) {
      this.initial = initial;
    }

    @Override
    public FlowShape<T, T> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
      return new GraphStageLogic(shape) {
        private T currentValue = initial;

        {
          setHandler(
              in,
              new AbstractInHandler() {
                @Override
                public void onPush() throws Exception {
                  currentValue = grab(in);
                  pull(in);
                }
              });
          setHandler(
              out,
              new AbstractOutHandler() {
                @Override
                public void onPull() throws Exception {
                  push(out, currentValue);
                }
              });
        }

        @Override
        public void preStart() {
          pull(in);
        }
      };
    }
  }
  // #hold-version-1

  // #hold-version-2
  class HoldWithWait<T> extends GraphStage<FlowShape<T, T>> {
    public Inlet<T> in = Inlet.<T>create("HoldWithInitial.in");
    public Outlet<T> out = Outlet.<T>create("HoldWithInitial.out");
    private FlowShape<T, T> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<T, T> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
      return new GraphStageLogic(shape) {
        private T currentValue = null;
        private boolean waitingFirstValue = true;

        {
          setHandler(
              in,
              new AbstractInHandler() {
                @Override
                public void onPush() throws Exception {
                  currentValue = grab(in);
                  if (waitingFirstValue) {
                    waitingFirstValue = false;
                    if (isAvailable(out)) push(out, currentValue);
                  }
                  pull(in);
                }
              });
          setHandler(
              out,
              new AbstractOutHandler() {
                @Override
                public void onPull() throws Exception {
                  if (!waitingFirstValue) push(out, currentValue);
                }
              });
        }

        @Override
        public void preStart() {
          pull(in);
        }
      };
    }
  }
  // #hold-version-2

  @Test
  public void workForVersion1() throws Exception {
    new TestKit(system) {
      {
        final Source<Integer, TestPublisher.Probe<Integer>> source = TestSource.probe(system);
        final Sink<Integer, TestSubscriber.Probe<Integer>> sink = TestSink.probe(system);

        Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> pubSub =
            source.via(new HoldWithInitial<>(0)).toMat(sink, Keep.both()).run(system);
        TestPublisher.Probe<Integer> pub = pubSub.first();
        TestSubscriber.Probe<Integer> sub = pubSub.second();

        sub.requestNext(0);
        sub.requestNext(0);

        pub.sendNext(1);
        pub.sendNext(2);

        sub.request(2);
        sub.expectNext(2, 2);

        pub.sendComplete();
        sub.request(1);
        sub.expectComplete();
      }
    };
  }

  @Test
  public void workForVersion2() throws Exception {
    new TestKit(system) {
      {
        final Source<Integer, TestPublisher.Probe<Integer>> source = TestSource.probe(system);
        final Sink<Integer, TestSubscriber.Probe<Integer>> sink = TestSink.probe(system);

        Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> pubSub =
            source.via(new HoldWithWait<>()).toMat(sink, Keep.both()).run(system);
        TestPublisher.Probe<Integer> pub = pubSub.first();
        TestSubscriber.Probe<Integer> sub = pubSub.second();

        FiniteDuration timeout = FiniteDuration.create(200, TimeUnit.MILLISECONDS);

        sub.request(1);
        sub.expectNoMessage(timeout);

        pub.sendNext(1);
        sub.expectNext(1);

        pub.sendNext(2);
        pub.sendNext(3);

        sub.request(2);
        sub.expectNext(3, 3);

        pub.sendComplete();
        sub.request(1);
        sub.expectComplete();
      }
    };
  }
}
