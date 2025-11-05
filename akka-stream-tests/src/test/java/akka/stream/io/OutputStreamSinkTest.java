/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io;

import static org.junit.Assert.assertEquals;

import akka.stream.IOOperationIncompleteException;
import akka.stream.IOResult;
import akka.stream.StreamTest;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.stream.testkit.Utils;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.util.ByteString;
import java.io.OutputStream;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class OutputStreamSinkTest extends StreamTest {
  public OutputStreamSinkTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("OutputStreamSinkTest", Utils.UnboundedMailboxConfig());

  @Test
  public void mustSignalFailureViaFailingFuture() {

    final OutputStream os =
        new OutputStream() {
          volatile int left = 3;

          public void write(int data) {
            if (left == 0) {
              throw new RuntimeException("Can't accept more data.");
            }
            left -= 1;
          }
        };
    final CompletionStage<IOResult> resultFuture =
        Source.single(ByteString.fromString("123456"))
            .runWith(StreamConverters.fromOutputStream(() -> os), system);

    ExecutionException exception =
        Assert.assertThrows(
            "CompletableFuture.get() should throw ExecutionException",
            ExecutionException.class,
            () -> resultFuture.toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals(
        "The cause of ExecutionException should be IOOperationIncompleteException",
        exception.getCause().getClass(),
        IOOperationIncompleteException.class);
    assertEquals(exception.getCause().getCause().getMessage(), "Can't accept more data.");
  }
}
