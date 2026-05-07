/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.Cancellable;
import akka.japi.Creator;
import akka.stream.KillSwitches;
import akka.stream.RestartSettings;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.time.Duration;
import java.util.Arrays;

public class Restart {
  static akka.actor.ActorSystem system = akka.actor.ActorSystem.create();

  public static void onRestartWithBackoffInnerFailure() {
    // #restart-failure-inner-failure
    // could throw if for example it used a database connection to get rows
    Source<Creator<Integer>, NotUsed> flakySource =
        Source.from(
            Arrays.<Creator<Integer>>asList(
                () -> 1,
                () -> 2,
                () -> 3,
                () -> {
                  throw new RuntimeException("darn");
                }));
    Source<Creator<Integer>, NotUsed> forever =
        RestartSource.onFailuresWithBackoff(
            RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(10), 0.1),
            () -> flakySource);
    forever.runWith(
        Sink.foreach((Creator<Integer> nr) -> system.log().info("{}", nr.create())), system);
    // logs
    // [INFO] [12/10/2019 13:51:58.300] [default-akka.test.stream-dispatcher-7]
    // [akka.actor.ActorSystemImpl(default)] 1
    // [INFO] [12/10/2019 13:51:58.301] [default-akka.test.stream-dispatcher-7]
    // [akka.actor.ActorSystemImpl(default)] 2
    // [INFO] [12/10/2019 13:51:58.302] [default-akka.test.stream-dispatcher-7]
    // [akka.actor.ActorSystemImpl(default)] 3
    // [WARN] [12/10/2019 13:51:58.310] [default-akka.test.stream-dispatcher-7]
    // [RestartWithBackoffSource(akka://default)] Restarting graph due to failure. stack_trace:
    // (RuntimeException: darn)
    // --> 1 second gap
    // [INFO] [12/10/2019 13:51:59.379] [default-akka.test.stream-dispatcher-8]
    // [akka.actor.ActorSystemImpl(default)] 1
    // [INFO] [12/10/2019 13:51:59.382] [default-akka.test.stream-dispatcher-8]
    // [akka.actor.ActorSystemImpl(default)] 2
    // [INFO] [12/10/2019 13:51:59.383] [default-akka.test.stream-dispatcher-8]
    // [akka.actor.ActorSystemImpl(default)] 3
    // [WARN] [12/10/2019 13:51:59.386] [default-akka.test.stream-dispatcher-8]
    // [RestartWithBackoffSource(akka://default)] Restarting graph due to failure. stack_trace:
    // (RuntimeException: darn)
    // --> 2 second gap
    // [INFO] [12/10/2019 13:52:01.594] [default-akka.test.stream-dispatcher-8]
    // [akka.actor.ActorSystemImpl(default)] 1
    // [INFO] [12/10/2019 13:52:01.595] [default-akka.test.stream-dispatcher-8]
    // [akka.actor.ActorSystemImpl(default)] 2
    // [INFO] [12/10/2019 13:52:01.595] [default-akka.test.stream-dispatcher-8]
    // [akka.actor.ActorSystemImpl(default)] 3
    // [WARN] [12/10/2019 13:52:01.596] [default-akka.test.stream-dispatcher-8]
    // [RestartWithBackoffSource(akka://default)] Restarting graph due to failure. stack_trace:
    // (RuntimeException: darn)
    // #restart-failure-inner-failure

  }

  public static void onRestartWithBackoffInnerComplete() {
    // #restart-failure-inner-complete
    Source<String, Cancellable> finiteSource =
        Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "tick").take(3);
    Source<String, NotUsed> forever =
        RestartSource.onFailuresWithBackoff(
            RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(10), 0.1),
            () -> finiteSource);
    forever.runWith(Sink.foreach(System.out::println), system);
    // prints
    // tick
    // tick
    // tick
    // #restart-failure-inner-complete
  }

  public static void onRestartWitFailureKillSwitch() {
    // #restart-failure-inner-complete-kill-switch
    Source<Creator<Integer>, NotUsed> flakySource =
        Source.from(
            Arrays.<Creator<Integer>>asList(
                () -> 1,
                () -> 2,
                () -> 3,
                () -> {
                  throw new RuntimeException("darn");
                }));
    UniqueKillSwitch stopRestarting =
        RestartSource.onFailuresWithBackoff(
                RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(10), 0.1),
                () -> flakySource)
            .viaMat(KillSwitches.single(), Keep.right())
            .toMat(Sink.foreach(nr -> System.out.println("nr " + nr.create())), Keep.left())
            .run(system);
    // ... from some where else
    // stop the source from restarting
    stopRestarting.shutdown();
    // #restart-failure-inner-complete-kill-switch
  }
}
