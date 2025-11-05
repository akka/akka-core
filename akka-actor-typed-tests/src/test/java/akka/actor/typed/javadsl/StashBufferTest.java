/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.actor.testkit.typed.internal.StubbedActorContext;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.typed.internal.StashBufferImpl;
import java.util.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class StashBufferTest extends JUnitSuite {

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  StubbedActorContext<String> context =
      new StubbedActorContext<String>(
          "StashBufferTest",
          () -> {
            throw new UnsupportedOperationException("Will never be invoked in this test");
          });

  @Test
  public void testProcessElementsInTheRightOrder() {

    StashBuffer<String> buffer = StashBufferImpl.apply(context, 10);
    buffer.stash("m1");
    buffer.stash("m2");
    buffer.stash("m3");

    StringBuilder sb1 = new StringBuilder();
    buffer.forEach(sb1::append);
    assertEquals("m1m2m3", sb1.toString());

    buffer.unstash(Behaviors.ignore(), 1, Function.identity());
    StringBuilder sb2 = new StringBuilder();
    buffer.forEach(sb2::append);
    assertEquals("m2m3", sb2.toString());
  }

  @Test
  public void testAnyMatchAndContains() {
    StashBuffer<String> buffer = StashBufferImpl.apply(context, 10);
    buffer.stash("m1");
    buffer.stash("m2");

    assertTrue(buffer.anyMatch(m -> m.startsWith("m")));
    assertTrue(buffer.anyMatch(m -> m.endsWith("2")));

    assertTrue(buffer.contains("m1"));
    assertTrue(buffer.contains("m2"));
  }
}
