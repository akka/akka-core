/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

// #imports1
import java.time.Duration;
// #imports1

// #imports2
import akka.actor.Cancellable;
// #imports2

import jdocs.AbstractJavaTest;
import akka.actor.Props;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.AkkaSpec;
import akka.testkit.AkkaJUnitActorSystemResource;
import org.junit.*;

public class SchedulerDocTest extends AbstractJavaTest {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("SchedulerDocTest", AkkaSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();
  private ActorRef testActor = system.actorOf(Props.create(MyActor.class));

  @Test
  public void scheduleOneOffTask() {
    // #schedule-one-off-message
    system
        .scheduler()
        .scheduleOnce(
            Duration.ofMillis(50), testActor, "foo", system.dispatcher(), ActorRef.noSender());
    // #schedule-one-off-message

    // #schedule-one-off-thunk
    system
        .scheduler()
        .scheduleOnce(
            Duration.ofMillis(50),
            new Runnable() {
              @Override
              public void run() {
                testActor.tell(System.currentTimeMillis(), ActorRef.noSender());
              }
            },
            system.dispatcher());
    // #schedule-one-off-thunk
  }

  @Test
  public void scheduleRecurringTask() {
    // #schedule-recurring
    class Ticker extends AbstractActor {
      @Override
      public Receive createReceive() {
        return receiveBuilder()
            .matchEquals(
                "Tick",
                m -> {
                  // Do something
                })
            .build();
      }
    }

    ActorRef tickActor = system.actorOf(Props.create(Ticker.class, this));

    // This will schedule to send the Tick-message
    // to the tickActor after 0ms repeating every 50ms
    Cancellable cancellable =
        system
            .scheduler()
            .scheduleWithFixedDelay(
                Duration.ZERO,
                Duration.ofMillis(50),
                tickActor,
                "Tick",
                system.dispatcher(),
                ActorRef.noSender());

    // This cancels further Ticks to be sent
    cancellable.cancel();
    // #schedule-recurring
    system.stop(tickActor);
  }
}
