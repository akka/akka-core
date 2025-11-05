/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi;

import static org.junit.Assert.*;

import akka.actor.ExtendedActorSystem;
import akka.event.LoggingAdapter;
import akka.event.NoLogging;
import akka.serialization.JavaSerializer;
import java.util.concurrent.Callable;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class JavaAPITestBase extends JUnitSuite {

  @Test
  public void shouldCreateSomeString() {
    Option<String> o = Option.some("abc");
    assertFalse(o.isEmpty());
    assertTrue(o.isDefined());
    assertEquals("abc", o.get());
    assertEquals("abc", o.getOrElse("other"));
  }

  @Test
  public void shouldCreateNone() {
    Option<String> o1 = Option.none();
    assertTrue(o1.isEmpty());
    assertFalse(o1.isDefined());
    assertEquals("other", o1.getOrElse("other"));

    Option<Float> o2 = Option.none();
    assertTrue(o2.isEmpty());
    assertFalse(o2.isDefined());
    assertEquals("other", o1.getOrElse("other"));
  }

  @Test
  public void shouldEnterForLoop() {
    for (@SuppressWarnings("unused") String s : Option.some("abc")) {
      return;
    }
    org.junit.Assert.fail("for-loop not entered");
  }

  @Test
  public void shouldNotEnterForLoop() {
    for (@SuppressWarnings("unused") Object o : Option.none()) {
      org.junit.Assert.fail("for-loop entered");
    }
  }

  @Test
  public void shouldBeSingleton() {
    assertSame(Option.none(), Option.none());
  }

  @Test
  public void mustBeAbleToGetNoLogging() {
    LoggingAdapter a = NoLogging.getInstance();
    assertNotNull(a);
  }

  @Test
  public void mustBeAbleToUseCurrentSystem() {
    assertNull(
        JavaSerializer.currentSystem()
            .withValue(
                null,
                new Callable<ExtendedActorSystem>() {
                  public ExtendedActorSystem call() {
                    return JavaSerializer.currentSystem().value();
                  }
                }));
  }
}
