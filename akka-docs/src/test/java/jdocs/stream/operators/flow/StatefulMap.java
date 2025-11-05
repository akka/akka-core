/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.flow;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import java.util.*;
import java.util.stream.IntStream;

public class StatefulMap {
  static final ActorSystem system = null;

  public void indexed() {
    // #zipWithIndex
    Source.from(Arrays.asList("A", "B", "C", "D"))
        .statefulMap(
            () -> 0L,
            (index, element) -> Pair.create(index + 1, Pair.create(element, index)),
            indexOnComplete -> Optional.empty())
        .runForeach(System.out::println, system);
    // prints
    // Pair(A,0)
    // Pair(B,1)
    // Pair(C,2)
    // Pair(D,3)
    // #zipWithIndex
  }

  public void bufferUntilChanged() {
    // #bufferUntilChanged
    Source.from(Arrays.asList("A", "B", "B", "C", "C", "C", "D"))
        .statefulMap(
            () -> (List<String>) new LinkedList<String>(),
            (buffer, element) -> {
              if (buffer.size() > 0 && (!buffer.get(0).equals(element))) {
                return Pair.create(
                    new LinkedList<>(Collections.singletonList(element)),
                    Collections.unmodifiableList(buffer));
              } else {
                buffer.add(element);
                return Pair.create(buffer, Collections.<String>emptyList());
              }
            },
            Optional::ofNullable)
        .filterNot(List::isEmpty)
        .runForeach(System.out::println, system);
    // prints
    // [A]
    // [B, B]
    // [C, C, C]
    // [D]
    // #bufferUntilChanged
  }

  public void distinctUntilChanged() {
    // #distinctUntilChanged
    Source.from(Arrays.asList("A", "B", "B", "C", "C", "C", "D"))
        .statefulMap(
            Optional::<String>empty,
            (lastElement, element) -> {
              if (lastElement.isPresent() && lastElement.get().equals(element)) {
                return Pair.create(lastElement, Optional.<String>empty());
              } else {
                return Pair.create(Optional.of(element), Optional.of(element));
              }
            },
            listOnComplete -> Optional.empty())
        .via(Flow.flattenOptional())
        .runForeach(System.out::println, system);
    // prints
    // A
    // B
    // C
    // D
    // #distinctUntilChanged
  }

  public void statefulMapConcatLike() {
    // #statefulMapConcatLike
    Source.fromJavaStream(() -> IntStream.rangeClosed(1, 10))
        .statefulMap(
            () -> new ArrayList<Integer>(3),
            (list, element) -> {
              list.add(element);
              if (list.size() == 3) {
                return Pair.create(new ArrayList<Integer>(3), Collections.unmodifiableList(list));
              } else {
                return Pair.create(list, Collections.<Integer>emptyList());
              }
            },
            Optional::ofNullable)
        .mapConcat(list -> list)
        .runForeach(System.out::println, system);
    // prints
    // 1
    // 2
    // 3
    // 4
    // 5
    // 6
    // 7
    // 8
    // 9
    // 10
    // #statefulMapConcatLike
  }
}
