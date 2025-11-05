/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import akka.NotUsed;
import akka.stream.StreamTest;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.util.ByteString;
import org.junit.ClassRule;
import org.junit.Test;

public class FramingTest extends StreamTest {
  public FramingTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("FramingTest", AkkaSpec.testConf());

  @Test
  public void mustBeAbleToUseFraming() throws Exception {
    final Source<ByteString, NotUsed> in = Source.single(ByteString.fromString("1,3,4,5"));
    in.via(
            Framing.delimiter(
                ByteString.fromString(","), Integer.MAX_VALUE, FramingTruncation.ALLOW))
        .runWith(Sink.ignore(), system);
  }
}
