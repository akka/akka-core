/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.setup;

import static org.junit.Assert.*;

import java.util.Optional;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ActorSystemSetupTest extends JUnitSuite {

  static class JavaSetup extends Setup {
    public final String name;

    public JavaSetup(String name) {
      this.name = name;
    }
  }

  @Test
  public void apiMustBeUsableFromJava() {
    final JavaSetup javaSetting = new JavaSetup("Jasmine Rice");
    final Optional<JavaSetup> result =
        ActorSystemSetup.create().withSetup(javaSetting).get(JavaSetup.class);

    assertTrue(result.isPresent());
    assertEquals(javaSetting, result.get());
  }
}
