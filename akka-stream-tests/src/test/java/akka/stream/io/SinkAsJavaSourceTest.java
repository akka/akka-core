/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io;

import static org.junit.Assert.assertEquals;

import akka.stream.StreamTest;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.stream.testkit.Utils;
import akka.testkit.AkkaJUnitActorSystemResource;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

public class SinkAsJavaSourceTest extends StreamTest {
  public SinkAsJavaSourceTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("SinkAsJavaSourceTest", Utils.UnboundedMailboxConfig());

  @Test
  public void mustBeAbleToUseAsJavaStream() throws Exception {
    final List<Integer> list = Arrays.asList(1, 2, 3);
    final Sink<Integer, Stream<Integer>> streamSink = StreamConverters.asJavaStream();
    java.util.stream.Stream<Integer> javaStream = Source.from(list).runWith(streamSink, system);
    assertEquals(list, javaStream.collect(Collectors.toList()));
  }
}
