/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import akka.NotUsed;
import akka.event.LogMarker;
import akka.event.MarkerLoggingAdapter;
import akka.japi.function.Function;

public class FlowLogWithMarkerTest {

  public static // #signature
  <In> Flow<In, In, NotUsed> logWithMarker(
      String name,
      Function<In, LogMarker> marker,
      Function<In, Object> extract,
      MarkerLoggingAdapter log)
        // #signature
      {
    return Flow.<In>create().logWithMarker(name, marker, extract, log);
  }
}
