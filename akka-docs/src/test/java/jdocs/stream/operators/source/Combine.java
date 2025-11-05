/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.ActorSystem;
// #imports
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Source;
// ...

// #imports
import java.util.Collections;

public class Combine {

  private static ActorSystem system;

  public void merge() throws Exception {
    // #source-combine-merge
    Source<Integer, NotUsed> source1 = Source.range(1, 3);
    Source<Integer, NotUsed> source2 = Source.range(8, 10);
    Source<Integer, NotUsed> source3 = Source.range(12, 14);

    final Source<Integer, NotUsed> combined =
        Source.combine(source1, source2, Collections.singletonList(source3), Merge::create);

    combined.runForeach(System.out::println, system);
    // could print (order between sources is not deterministic)
    // 1
    // 12
    // 8
    // 9
    // 13
    // 14
    // 2
    // 10
    // 3
    // #source-combine-merge
  }

  public void concat() throws Exception {
    // #source-combine-concat
    Source<Integer, NotUsed> source1 = Source.range(1, 3);
    Source<Integer, NotUsed> source2 = Source.range(8, 10);
    Source<Integer, NotUsed> source3 = Source.range(12, 14);

    final Source<Integer, NotUsed> sources =
        Source.combine(source1, source2, Collections.singletonList(source3), Concat::create);

    sources.runForeach(System.out::println, system);
    // prints (order is deterministic)
    // 1
    // 2
    // 3
    // 8
    // 9
    // 10
    // 12
    // 13
    // 14
    // #source-combine-concat
  }
}
