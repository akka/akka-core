/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.actor.testkit.typed.javadsl;

// #manual-scheduling-simple

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.ManualTime;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ManualTimerExampleTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit = new TestKitJunitResource(ManualTime.config());

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private final ManualTime manualTime = ManualTime.get(testKit.system());

  static final class Tick {
    private Tick() {}

    static final Tick INSTANCE = new Tick();
  }

  static final class Tock {}

  @Test
  public void testScheduleNonRepeatedTicks() {
    TestProbe<Tock> probe = testKit.createTestProbe();
    Behavior<Tick> behavior =
        Behaviors.withTimers(
            timer -> {
              timer.startSingleTimer(Tick.INSTANCE, Duration.ofMillis(10));
              return Behaviors.receiveMessage(
                  tick -> {
                    probe.ref().tell(new Tock());
                    return Behaviors.same();
                  });
            });

    testKit.spawn(behavior);

    manualTime.expectNoMessageFor(Duration.ofMillis(9), probe);

    manualTime.timePasses(Duration.ofMillis(2));
    probe.expectMessageClass(Tock.class);

    manualTime.expectNoMessageFor(Duration.ofSeconds(10), probe);
  }
}
// #manual-scheduling-simple
