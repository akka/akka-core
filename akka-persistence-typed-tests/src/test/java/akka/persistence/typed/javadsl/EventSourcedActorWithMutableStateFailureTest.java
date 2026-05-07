/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl;

import static akka.persistence.typed.scaladsl.EventSourcedBehaviorFailureSpec.conf;

import akka.actor.testkit.typed.TestException;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.RecoveryCompleted;
import akka.persistence.typed.RecoveryFailed;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

class FailingEventSourcedActorWithMutableState
    extends EventSourcedBehavior<
        String, String, FailingEventSourcedActorWithMutableState.MutableState> {

  static final class MutableState {
    private String value;

    MutableState(String value) {
      this.value = value;
    }

    void add(String s) {
      value = value + s;
    }

    String getValue() {
      return value;
    }
  }

  private final ActorRef<String> probe;
  private final ActorRef<Throwable> recoveryFailureProbe;

  FailingEventSourcedActorWithMutableState(
      PersistenceId persistenceId,
      ActorRef<String> probe,
      ActorRef<Throwable> recoveryFailureProbe) {

    super(
        persistenceId,
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(1), Duration.ofMillis(5), 0.1));
    this.probe = probe;
    this.recoveryFailureProbe = recoveryFailureProbe;
  }

  @Override
  public SignalHandler<MutableState> signalHandler() {
    return newSignalHandlerBuilder()
        .onSignal(
            RecoveryCompleted.instance(),
            state -> {
              probe.tell("starting");
            })
        .onSignal(
            RecoveryFailed.class,
            (state, signal) -> {
              recoveryFailureProbe.tell(signal.getFailure());
            })
        .build();
  }

  @Override
  public MutableState emptyState() {
    return new MutableState("");
  }

  @Override
  public CommandHandler<String, String, MutableState> commandHandler() {
    return (state, command) -> {
      if (command.equals("get")) {
        probe.tell("state [" + state.getValue() + "]");
        return Effect().none();
      } else {
        probe.tell("persisting");
        return Effect().persist(command);
      }
    };
  }

  @Override
  public EventHandler<MutableState, String> eventHandler() {
    return (state, event) -> {
      probe.tell(event);
      state.add(event);
      return state;
    };
  }
}

public class EventSourcedActorWithMutableStateFailureTest extends JUnitSuite {

  public static final Config config = conf().withFallback(ConfigFactory.load());

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource(config);

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  public static Behavior<String> fail(
      PersistenceId pid, ActorRef<String> probe, ActorRef<Throwable> recoveryFailureProbe) {
    return new FailingEventSourcedActorWithMutableState(pid, probe, recoveryFailureProbe);
  }

  public static Behavior<String> fail(PersistenceId pid, ActorRef<String> probe) {
    return fail(pid, probe, testKit.<Throwable>createTestProbe().ref());
  }

  @Test
  public void notifyRecoveryFailure() {
    TestProbe<String> probe = testKit.createTestProbe();
    TestProbe<Throwable> recoveryFailureProbe = testKit.createTestProbe();
    Behavior<String> p1 =
        fail(
            PersistenceId.ofUniqueId("fail-recovery-once"),
            probe.ref(),
            recoveryFailureProbe.ref());
    testKit.spawn(p1);
    recoveryFailureProbe.expectMessageClass(TestException.class);
  }

  @Test
  public void persistEvents() throws Exception {
    TestProbe<String> probe = testKit.createTestProbe();
    Behavior<String> p1 = fail(PersistenceId.ofUniqueId("fail-first-2"), probe.ref());
    ActorRef<String> c = testKit.spawn(p1);
    probe.expectMessage("starting");
    // fail
    c.tell("one");
    probe.expectMessage("persisting");
    probe.expectMessage("one");
    probe.expectMessage("starting");
    c.tell("get");
    probe.expectMessage("state []");

    // fail
    c.tell("two");
    probe.expectMessage("persisting");
    probe.expectMessage("two");
    probe.expectMessage("starting");
    c.tell("get");
    probe.expectMessage("state []");

    // work
    c.tell("three");
    probe.expectMessage("persisting");
    probe.expectMessage("three");
    // no starting as this one did not fail
    probe.expectNoMessage();
    c.tell("get");
    probe.expectMessage("state [three]");
  }
}
