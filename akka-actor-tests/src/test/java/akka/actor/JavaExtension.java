/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import static org.junit.Assert.*;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import com.typesafe.config.ConfigFactory;
import org.junit.*;
import org.scalatestplus.junit.JUnitSuite;

public class JavaExtension extends JUnitSuite {

  static class TestExtensionId extends AbstractExtensionId<TestExtension>
      implements ExtensionIdProvider {
    public static final TestExtensionId TestExtensionProvider = new TestExtensionId();

    public ExtensionId<TestExtension> lookup() {
      return TestExtensionId.TestExtensionProvider;
    }

    public TestExtension createExtension(ExtendedActorSystem i) {
      return new TestExtension(i);
    }
  }

  static class TestExtension implements Extension {
    public final ExtendedActorSystem system;

    public TestExtension(ExtendedActorSystem i) {
      system = i;
    }
  }

  static class OtherExtensionId extends AbstractExtensionId<OtherExtension>
      implements ExtensionIdProvider {

    public static final OtherExtensionId OtherExtensionProvider = new OtherExtensionId();

    @Override
    public ExtensionId<OtherExtension> lookup() {
      return OtherExtensionId.OtherExtensionProvider;
    }

    @Override
    public OtherExtension createExtension(ExtendedActorSystem system) {
      return new OtherExtension(system);
    }
  }

  static class OtherExtension implements Extension {
    static final ExtensionId<OtherExtension> key = OtherExtensionId.OtherExtensionProvider;

    public final ExtendedActorSystem system;

    public OtherExtension(ExtendedActorSystem system) {
      this.system = system;
    }
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource(
          "JavaExtension",
          ConfigFactory.parseString(
                  "akka.extensions = [ \"akka.actor.JavaExtension$TestExtensionId\" ]")
              .withFallback(AkkaSpec.testConf()));

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void mustBeAccessible() {
    assertTrue(system.hasExtension((TestExtensionId.TestExtensionProvider)));
    assertSame(system.extension(TestExtensionId.TestExtensionProvider).system, system);
    assertSame(TestExtensionId.TestExtensionProvider.apply(system).system, system);
  }

  @Test
  public void mustBeAdHoc() {
    assertSame(OtherExtension.key.apply(system).system, system);
  }
}
