/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;

public class Split {
  public static void splitWhenExample(String[] args) {
    ActorSystem system = ActorSystem.create();

    // #splitWhen
    Source.range(1, 100)
        .throttle(1, Duration.ofMillis(100))
        .map(elem -> new Pair<>(elem, Instant.now()))
        .statefulMapConcat(
            () -> {
              return new Function<Pair<Integer, Instant>, Iterable<Pair<Integer, Boolean>>>() {
                // stateful decision in statefulMapConcat
                // keep track of time bucket (one per second)
                LocalDateTime currentTimeBucket =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);

                @Override
                public Iterable<Pair<Integer, Boolean>> apply(
                    Pair<Integer, Instant> elemTimestamp) {
                  LocalDateTime time =
                      LocalDateTime.ofInstant(elemTimestamp.second(), ZoneOffset.UTC);
                  LocalDateTime bucket = time.withNano(0);
                  boolean newBucket = !bucket.equals(currentTimeBucket);
                  if (newBucket) currentTimeBucket = bucket;
                  return Collections.singleton(new Pair<>(elemTimestamp.first(), newBucket));
                }
              };
            })
        .splitWhen(elemDecision -> elemDecision.second()) // split when time bucket changes
        .map(elemDecision -> elemDecision.first())
        .fold(0, (acc, notUsed) -> acc + 1) // sum
        .to(Sink.foreach(System.out::println))
        .run(system);
    // 3
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 7
    // #splitWhen
  }

  public static void splitAfterExample(String[] args) {
    ActorSystem system = ActorSystem.create();

    // #splitAfter
    Source.range(1, 100)
        .throttle(1, Duration.ofMillis(100))
        .map(elem -> new Pair<>(elem, Instant.now()))
        .sliding(2, 1)
        .splitAfter(
            slidingElements -> {
              if (slidingElements.size() == 2) {
                Pair<Integer, Instant> current = slidingElements.get(0);
                Pair<Integer, Instant> next = slidingElements.get(1);
                LocalDateTime currentBucket =
                    LocalDateTime.ofInstant(current.second(), ZoneOffset.UTC).withNano(0);
                LocalDateTime nextBucket =
                    LocalDateTime.ofInstant(next.second(), ZoneOffset.UTC).withNano(0);
                return !currentBucket.equals(nextBucket);
              } else {
                return false;
              }
            })
        .map(slidingElements -> slidingElements.get(0).first())
        .fold(0, (acc, notUsed) -> acc + 1) // sum
        .to(Sink.foreach(System.out::println))
        .run(system);
    // 3
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 10
    // 6
    // note that the very last element is never included due to sliding,
    // but that would not be problem for an infinite stream
    // #splitAfter
  }
}
