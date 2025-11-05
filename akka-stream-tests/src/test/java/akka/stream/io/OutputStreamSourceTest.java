/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io;

import static org.junit.Assert.assertEquals;

import akka.actor.ActorRef;
import akka.japi.function.Procedure;
import akka.stream.StreamTest;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.stream.testkit.Utils;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.io.OutputStream;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;

public class OutputStreamSourceTest extends StreamTest {
  public OutputStreamSourceTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("OutputStreamSourceTest", Utils.UnboundedMailboxConfig());

  @Test
  public void mustSendEventsViaOutputStream() throws Exception {
    final TestKit probe = new TestKit(system);
    final Duration timeout = Duration.ofSeconds(3);

    final Source<ByteString, OutputStream> source = StreamConverters.asOutputStream(timeout);
    final OutputStream s =
        source
            .to(
                Sink.foreach(
                    new Procedure<ByteString>() {
                      private static final long serialVersionUID = 1L;

                      public void apply(ByteString elem) {
                        probe.getRef().tell(elem, ActorRef.noSender());
                      }
                    }))
            .run(system);

    s.write("a".getBytes());

    assertEquals(ByteString.fromString("a"), probe.receiveOne(timeout));
    s.close();
  }
}
