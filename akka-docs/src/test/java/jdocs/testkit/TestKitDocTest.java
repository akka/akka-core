/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.testkit;

import static org.junit.Assert.*;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorKilledException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.japi.JavaPartialFunction;
import akka.pattern.Patterns;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.CallingThreadDispatcher;
import akka.testkit.TestActor;
import akka.testkit.TestActor.AutoPilot;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.EventFilter;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import jdocs.AbstractJavaTest;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class TestKitDocTest extends AbstractJavaTest {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource(
          "TestKitDocTest",
          ConfigFactory.parseString("akka.loggers = [akka.testkit.TestEventListener]"));

  private final ActorSystem system = actorSystemResource.getSystem();

  // #test-actor-ref
  static class MyActor extends AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "say42",
              message -> {
                getSender().tell(42, getSelf());
              })
          .match(
              Exception.class,
              (Exception ex) -> {
                throw ex;
              })
          .build();
    }

    public boolean testMe() {
      return true;
    }
  }

  @Test
  public void demonstrateTestActorRef() {
    final Props props = Props.create(MyActor.class);
    final TestActorRef<MyActor> ref = TestActorRef.create(system, props, "testA");
    final MyActor actor = ref.underlyingActor();
    assertTrue(actor.testMe());
  }
  // #test-actor-ref

  // #timer
  static class TestTimerActor extends AbstractActorWithTimers {
    private static Object SCHED_KEY = "SchedKey";

    static final class TriggerScheduling {}

    static final class ScheduledMessage {}

    @Override
    public Receive createReceive() {
      return receiveBuilder().match(TriggerScheduling.class, msg -> triggerScheduling()).build();
    }

    void triggerScheduling() {
      getTimers().startSingleTimer(SCHED_KEY, new ScheduledMessage(), Duration.ofMillis(500));
    }
  }
  // #timer

  @Test
  public void demonstrateAsk() throws Exception {
    // #test-behavior
    final Props props = Props.create(MyActor.class);
    final TestActorRef<MyActor> ref = TestActorRef.create(system, props, "testB");
    final CompletableFuture<Object> future =
        Patterns.ask(ref, "say42", Duration.ofMillis(3000)).toCompletableFuture();
    assertTrue(future.isDone());
    assertEquals(42, future.get());
    // #test-behavior
  }

  @Test
  public void demonstrateExceptions() {
    // #test-expecting-exceptions
    final Props props = Props.create(MyActor.class);
    final TestActorRef<MyActor> ref = TestActorRef.create(system, props, "myActor");
    try {
      ref.receive(new Exception("expected"));
      Assert.fail("expected an exception to be thrown");
    } catch (Exception e) {
      assertEquals("expected", e.getMessage());
    }
    // #test-expecting-exceptions
  }

  @Test
  public void demonstrateWithin() {
    // #test-within
    new TestKit(system) {
      {
        getRef().tell(42, ActorRef.noSender());
        within(
            Duration.ZERO,
            Duration.ofSeconds(1),
            () -> {
              assertEquals((Integer) 42, expectMsgClass(Integer.class));
              return null;
            });
      }
    };
    // #test-within
  }

  @Test
  public void demonstrateExpectMsg() {
    // #test-expectmsg
    new TestKit(system) {
      {
        getRef().tell(42, ActorRef.noSender());
        final String out =
            expectMsgPF(
                "match hint",
                in -> {
                  if (in instanceof Integer) {
                    return "match";
                  } else {
                    throw JavaPartialFunction.noMatch();
                  }
                });
        assertEquals("match", out);
      }
    };
    // #test-expectmsg
  }

  @Test
  public void demonstrateReceiveWhile() {
    // #test-receivewhile
    new TestKit(system) {
      {
        getRef().tell(42, ActorRef.noSender());
        getRef().tell(43, ActorRef.noSender());
        getRef().tell("hello", ActorRef.noSender());

        final List<String> out =
            receiveWhile(
                Duration.ofSeconds(1),
                in -> {
                  if (in instanceof Integer) {
                    return in.toString();
                  } else {
                    throw JavaPartialFunction.noMatch();
                  }
                });

        assertArrayEquals(new String[] {"42", "43"}, out.toArray());
        expectMsgEquals("hello");
      }
    };
    // #test-receivewhile
    new TestKit(system) {
      {
        // #test-receivewhile-full
        receiveWhile(
            Duration.ofMillis(100),
            Duration.ofMillis(50),
            12,
            in -> {
              // #match-elided
              throw JavaPartialFunction.noMatch();
              // #match-elided
            });
        // #test-receivewhile-full
      }
    };
  }

  @Test
  public void demonstrateAwaitCond() {
    // #test-awaitCond
    new TestKit(system) {
      {
        getRef().tell(42, ActorRef.noSender());
        awaitCond(Duration.ofSeconds(1), Duration.ofMillis(100), this::msgAvailable);
      }
    };
    // #test-awaitCond
  }

  @Test
  public void demonstrateAwaitAssert() {
    // #test-awaitAssert
    new TestKit(system) {
      {
        getRef().tell(42, ActorRef.noSender());
        awaitAssert(
            Duration.ofSeconds(1),
            Duration.ofMillis(100),
            () -> {
              assertEquals(msgAvailable(), true);
              return null;
            });
      }
    };
    // #test-awaitAssert
  }

  @Test
  @SuppressWarnings({"unchecked", "unused"}) // due to generic varargs
  public void demonstrateExpect() {
    new TestKit(system) {
      {
        getRef().tell("hello", ActorRef.noSender());
        getRef().tell("hello", ActorRef.noSender());
        getRef().tell("hello", ActorRef.noSender());
        getRef().tell("world", ActorRef.noSender());
        getRef().tell(42, ActorRef.noSender());
        getRef().tell(42, ActorRef.noSender());
        // #test-expect
        final String hello = expectMsgEquals("hello");
        final String any = expectMsgAnyOf("hello", "world");
        final List<String> all = expectMsgAllOf("hello", "world");
        final int i = expectMsgClass(Integer.class);
        final Number j = expectMsgAnyClassOf(Integer.class, Long.class);
        expectNoMessage();
        // #test-expect
        getRef().tell("receveN-1", ActorRef.noSender());
        getRef().tell("receveN-2", ActorRef.noSender());
        // #test-expect
        final List<Object> two = receiveN(2);
        // #test-expect
        assertEquals("hello", hello);
        assertEquals("hello", any);
        assertEquals(42, i);
        assertEquals(42, j);
        assertArrayEquals(new String[] {"hello", "world"}, all.toArray());
      }
    };
  }

  @Test
  public void demonstrateIgnoreMsg() {
    // #test-ignoreMsg
    new TestKit(system) {
      {
        // ignore all Strings
        ignoreMsg(msg -> msg instanceof String);
        getRef().tell("hello", ActorRef.noSender());
        getRef().tell(42, ActorRef.noSender());
        expectMsgEquals(42);
        // remove message filter
        ignoreNoMsg();
        getRef().tell("hello", ActorRef.noSender());
        expectMsgEquals("hello");
      }
    };
    // #test-ignoreMsg
  }

  @Test
  public void demonstrateDilated() {
    // #duration-dilation
    new TestKit(system) {
      {
        final Duration original = Duration.ofSeconds(1);
        final Duration stretched = dilated(original);
        assertTrue("dilated", stretched.compareTo(original) >= 0);
      }
    };
    // #duration-dilation
  }

  @Test
  public void demonstrateProbe() {
    // #test-probe
    new TestKit(system) {
      {
        // simple actor which only forwards messages
        class Forwarder extends AbstractActor {
          final ActorRef target;

          @SuppressWarnings("unused")
          public Forwarder(ActorRef target) {
            this.target = target;
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .matchAny(message -> target.forward(message, getContext()))
                .build();
          }
        }

        // create a test probe
        final TestKit probe = new TestKit(system);

        // create a forwarder, injecting the probe’s testActor
        final Props props = Props.create(Forwarder.class, this, probe.getRef());
        final ActorRef forwarder = system.actorOf(props, "forwarder");

        // verify correct forwarding
        forwarder.tell(42, getRef());
        probe.expectMsgEquals(42);
        assertEquals(getRef(), probe.getLastSender());
      }
    };
    // #test-probe
  }

  @Test
  public void demonstrateTestProbeWithCustomName() {
    // #test-probe-with-custom-name
    new TestKit(system) {
      {
        final TestProbe worker = new TestProbe(system, "worker");
        final TestProbe aggregator = new TestProbe(system, "aggregator");

        assertTrue(worker.ref().path().name().startsWith("worker"));
        assertTrue(aggregator.ref().path().name().startsWith("aggregator"));
      }
    };
    // #test-probe-with-custom-name
  }

  @Test
  public void demonstrateSpecialProbe() {
    // #test-special-probe
    new TestKit(system) {
      {
        class MyProbe extends TestKit {
          public MyProbe() {
            super(system);
          }

          public void assertHello() {
            expectMsgEquals("hello");
          }
        }

        final MyProbe probe = new MyProbe();
        probe.getRef().tell("hello", ActorRef.noSender());
        probe.assertHello();
      }
    };
    // #test-special-probe
  }

  @Test
  public void demonstrateWatch() {
    final ActorRef target = system.actorOf(Props.create(MyActor.class));
    // #test-probe-watch
    new TestKit(system) {
      {
        final TestKit probe = new TestKit(system);
        probe.watch(target);
        target.tell(PoisonPill.getInstance(), ActorRef.noSender());
        final Terminated msg = probe.expectMsgClass(Terminated.class);
        assertEquals(msg.getActor(), target);
      }
    };
    // #test-probe-watch
  }

  @Test
  public void demonstrateReply() {
    // #test-probe-reply
    new TestKit(system) {
      {
        final TestKit probe = new TestKit(system);
        probe.getRef().tell("hello", getRef());
        probe.expectMsgEquals("hello");
        probe.reply("world");
        expectMsgEquals("world");
        assertEquals(probe.getRef(), getLastSender());
      }
    };
    // #test-probe-reply
  }

  @Test
  public void demonstrateForward() {
    // #test-probe-forward
    new TestKit(system) {
      {
        final TestKit probe = new TestKit(system);
        probe.getRef().tell("hello", getRef());
        probe.expectMsgEquals("hello");
        probe.forward(getRef());
        expectMsgEquals("hello");
        assertEquals(getRef(), getLastSender());
      }
    };
    // #test-probe-forward
  }

  @Test
  public void demonstrateUsingInheritenceToTestTimers() {
    // #timer-test
    new TestKit(system) {
      {
        final TestKit probe = new TestKit(system);
        final ActorRef target =
            system.actorOf(
                Props.create(
                    TestTimerActor.class,
                    () ->
                        new TestTimerActor() {
                          @Override
                          void triggerScheduling() {
                            probe.getRef().tell(new ScheduledMessage(), getSelf());
                          }
                        }));
        target.tell(new TestTimerActor.TriggerScheduling(), ActorRef.noSender());
        probe.expectMsgClass(TestTimerActor.ScheduledMessage.class);
      }
    };
    // #timer-test
  }

  @Test
  public void demonstrateWithinProbe() {
    try {
      // #test-within-probe
      new TestKit(system) {
        {
          final TestKit probe = new TestKit(system);
          within(Duration.ofSeconds(1), () -> probe.expectMsgEquals("hello"));
        }
      };
      // #test-within-probe
    } catch (AssertionError e) {
      // expected to fail
    }
  }

  @Test
  public void demonstrateAutoPilot() {
    // #test-auto-pilot
    new TestKit(system) {
      {
        final TestKit probe = new TestKit(system);
        // install auto-pilot
        probe.setAutoPilot(
            new TestActor.AutoPilot() {
              public AutoPilot run(ActorRef sender, Object msg) {
                sender.tell(msg, ActorRef.noSender());
                return noAutoPilot();
              }
            });
        // first one is replied to directly ...
        probe.getRef().tell("hello", getRef());
        expectMsgEquals("hello");
        // ... but then the auto-pilot switched itself off
        probe.getRef().tell("world", getRef());
        expectNoMessage();
      }
    };
    // #test-auto-pilot
  }

  // only compilation
  public void demonstrateCTD() {
    // #calling-thread-dispatcher
    system.actorOf(Props.create(MyActor.class).withDispatcher(CallingThreadDispatcher.Id()));
    // #calling-thread-dispatcher
  }

  @Test
  public void demonstrateEventFilter() {
    // #test-event-filter
    new TestKit(system) {
      {
        assertEquals("TestKitDocTest", system.name());
        final ActorRef victim = system.actorOf(Props.empty(), "victim");

        final int result =
            new EventFilter(ActorKilledException.class, system)
                .from("akka://TestKitDocTest/user/victim")
                .occurrences(1)
                .intercept(
                    () -> {
                      victim.tell(Kill.getInstance(), ActorRef.noSender());
                      return 42;
                    });
        assertEquals(42, result);
      }
    };
    // #test-event-filter
  }
}
