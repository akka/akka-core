/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.fsm;

import static jdocs.actor.fsm.FSMDocTest.Messages.*;
import static jdocs.actor.fsm.FSMDocTest.StateType.*;
import static org.junit.Assert.*;

import akka.actor.*;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FSMDocTest extends AbstractJavaTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("FSMDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  public enum StateType {
    SomeState,
    Processing,
    Idle,
    Active,
    Error
  }

  public enum Messages {
    WillDo,
    Tick
  }

  public enum Data {
    Foo,
    Bar
  };

  public static interface X {};

  public static class DummyFSM extends AbstractFSM<StateType, Integer> {
    Integer newData = 42;
    // #alt-transition-syntax
    public void handler(StateType from, StateType to) {
      // handle transition here
    }

    // #alt-transition-syntax
    {
      // #modifier-syntax
      when(
          SomeState,
          matchAnyEvent(
              (msg, data) -> {
                return goTo(Processing)
                    .using(newData)
                    .forMax(Duration.ofSeconds(5))
                    .replying(WillDo);
              }));
      // #modifier-syntax

      // #NullFunction
      when(SomeState, AbstractFSM.NullFunction());
      // #NullFunction

      // #transition-syntax
      onTransition(
          matchState(
                  Idle,
                  Active,
                  () -> startTimerWithFixedDelay("timeout", Tick, Duration.ofSeconds(1L)))
              .state(Active, null, () -> cancelTimer("timeout"))
              .state(null, Idle, (f, t) -> log().info("entering Idle from " + f)));
      // #transition-syntax

      // #alt-transition-syntax
      onTransition(this::handler);
      // #alt-transition-syntax

      // #stop-syntax
      when(
          Error,
          matchEventEquals(
              "stop",
              (event, data) -> {
                // do cleanup ...
                return stop();
              }));
      // #stop-syntax

      // #termination-syntax
      onTermination(
          matchStop(
                  Normal(),
                  (state, data) -> {
                    /* Do something here */
                  })
              .stop(
                  Shutdown(),
                  (state, data) -> {
                    /* Do something here */
                  })
              .stop(
                  Failure.class,
                  (reason, state, data) -> {
                    /* Do something here */
                  }));
      // #termination-syntax

      // #unhandled-syntax
      whenUnhandled(
          matchEvent(
                  X.class,
                  (x, data) -> {
                    log().info("Received unhandled event: " + x);
                    return stay();
                  })
              .anyEvent(
                  (event, data) -> {
                    log().warning("Received unknown event: " + event);
                    return goTo(Error);
                  }));
    }
    // #unhandled-syntax
  }

  public
  // #logging-fsm
  static class MyFSM extends AbstractLoggingFSM<StateType, Data> {
    // #body-elided
    // #logging-fsm
    ActorRef target = null;
    // #logging-fsm
    @Override
    public int logDepth() {
      return 12;
    }

    {
      onTermination(
          matchStop(
              Failure.class,
              (reason, state, data) -> {
                String lastEvents = getLog().mkString("\n\t");
                log()
                    .warning(
                        "Failure in state "
                            + state
                            + " with data "
                            + data
                            + "\n"
                            + "Events leading up to this point:\n\t"
                            + lastEvents);
                // #logging-fsm
                target.tell(reason.cause(), getSelf());
                target.tell(state, getSelf());
                target.tell(data, getSelf());
                target.tell(lastEvents, getSelf());
                // #logging-fsm
              }));
      // ...
      // #logging-fsm
      startWith(SomeState, Data.Foo);
      when(
          SomeState,
          matchEvent(
              ActorRef.class,
              Data.class,
              (ref, data) -> {
                target = ref;
                target.tell("going active", getSelf());
                return goTo(Active);
              }));
      when(
          Active,
          matchEventEquals(
              "stop",
              (event, data) -> {
                target.tell("stopping", getSelf());
                return stop(new Failure("This is not the error you're looking for"));
              }));
      initialize();
      // #logging-fsm
    }
    // #body-elided
  }
  // #logging-fsm

  @Test
  public void testLoggingFSM() {
    new TestKit(system) {
      {
        final ActorRef logger = system.actorOf(Props.create(MyFSM.class));
        final ActorRef probe = getRef();

        logger.tell(probe, probe);
        expectMsgEquals("going active");
        logger.tell("stop", probe);
        expectMsgEquals("stopping");
        expectMsgEquals("This is not the error you're looking for");
        expectMsgEquals(Active);
        expectMsgEquals(Data.Foo);
        String msg = expectMsgClass(String.class);
        assertTrue(msg.startsWith("LogEntry(SomeState,Foo,Actor[akka://FSMDocTest/system/"));
      }
    };
  }
}
