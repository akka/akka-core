/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import java.util.concurrent.CompletionStage;

public class LimitWeighted {
  public void simple() {
    ActorSystem<?> system = null;
    // #simple
    Source<ByteString, NotUsed> untrustedSource = Source.repeat(ByteString.fromString("element"));

    CompletionStage<ByteString> allBytes =
        untrustedSource
            .limitWeighted(
                10000, // max bytes
                bytes -> (long) bytes.length() // bytes of each chunk
                )
            .runReduce(ByteString::concat, system);
    // #simple
  }
}
