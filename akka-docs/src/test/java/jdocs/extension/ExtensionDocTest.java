/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.extension;

// #imports
import akka.actor.*;
import java.util.concurrent.atomic.AtomicLong;

// #imports

import jdocs.AbstractJavaTest;
import org.junit.Test;

public class ExtensionDocTest extends AbstractJavaTest {

  public
  // #extension
  static class CountExtensionImpl implements Extension {
    // Since this Extension is a shared instance
    // per ActorSystem we need to be threadsafe
    private final AtomicLong counter = new AtomicLong(0);

    // This is the operation this Extension provides
    public long increment() {
      return counter.incrementAndGet();
    }
  }

  // #extension

  public
  // #extensionid
  static class CountExtension extends AbstractExtensionId<CountExtensionImpl>
      implements ExtensionIdProvider {
    // This will be the identifier of our CountExtension
    public static final CountExtension CountExtensionProvider = new CountExtension();

    private CountExtension() {}

    // The lookup method is required by ExtensionIdProvider,
    // so we return ourselves here, this allows us
    // to configure our extension to be loaded when
    // the ActorSystem starts up
    public CountExtension lookup() {
      return CountExtension.CountExtensionProvider; // The public static final
    }

    // This method will be called by Akka
    // to instantiate our Extension
    public CountExtensionImpl createExtension(ExtendedActorSystem system) {
      return new CountExtensionImpl();
    }
  }

  // #extensionid

  public
  // #extension-usage-actor
  static class MyActor extends AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchAny(
              msg -> {
                // typically you would use static import of the
                // CountExtension.CountExtensionProvider field
                CountExtension.CountExtensionProvider.get(getContext().getSystem()).increment();
              })
          .build();
    }
  }

  // #extension-usage-actor

  @Test
  public void demonstrateHowToCreateAndUseAnAkkaExtensionInJava() {
    final ActorSystem system = null;
    try {
      // #extension-usage
      // typically you would use static import of the
      // CountExtension.CountExtensionProvider field
      CountExtension.CountExtensionProvider.get(system).increment();
      // #extension-usage
    } catch (Exception e) {
      // do nothing
    }
  }
}
