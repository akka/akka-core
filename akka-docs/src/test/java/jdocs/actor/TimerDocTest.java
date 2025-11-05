/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

// #timers
import akka.actor.AbstractActorWithTimers;
import java.time.Duration;

// #timers

public class TimerDocTest {

  public
  // #timers
  static class MyActor extends AbstractActorWithTimers {

    private static Object TICK_KEY = "TickKey";

    private static final class FirstTick {}

    private static final class Tick {}

    public MyActor() {
      getTimers().startSingleTimer(TICK_KEY, new FirstTick(), Duration.ofMillis(500));
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              FirstTick.class,
              message -> {
                // do something useful here
                getTimers().startTimerWithFixedDelay(TICK_KEY, new Tick(), Duration.ofSeconds(1));
              })
          .match(
              Tick.class,
              message -> {
                // do something useful here
              })
          .build();
    }
  }
  // #timers
}
