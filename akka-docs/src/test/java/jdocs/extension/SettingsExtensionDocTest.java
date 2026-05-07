/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.extension;

// #imports
import akka.actor.Extension;
import akka.actor.AbstractExtensionId;
import akka.actor.ExtensionIdProvider;
import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

// #imports

import jdocs.AbstractJavaTest;
import akka.actor.AbstractActor;
import org.junit.Test;

public class SettingsExtensionDocTest extends AbstractJavaTest {

  public
  // #extension
  static class SettingsImpl implements Extension {

    public final String DB_URI;
    public final Duration CIRCUIT_BREAKER_TIMEOUT;

    public SettingsImpl(Config config) {
      DB_URI = config.getString("myapp.db.uri");
      CIRCUIT_BREAKER_TIMEOUT =
          Duration.ofMillis(
              config.getDuration("myapp.circuit-breaker.timeout", TimeUnit.MILLISECONDS));
    }
  }

  // #extension

  public
  // #extensionid
  static class Settings extends AbstractExtensionId<SettingsImpl> implements ExtensionIdProvider {
    public static final Settings SettingsProvider = new Settings();

    private Settings() {}

    public Settings lookup() {
      return Settings.SettingsProvider;
    }

    public SettingsImpl createExtension(ExtendedActorSystem system) {
      return new SettingsImpl(system.settings().config());
    }
  }

  // #extensionid

  public
  // #extension-usage-actor
  static class MyActor extends AbstractActor {
    // typically you would use static import of the Settings.SettingsProvider field
    final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    Connection connection = connect(settings.DB_URI, settings.CIRCUIT_BREAKER_TIMEOUT);

    // #extension-usage-actor

    public Connection connect(String dbUri, Duration circuitBreakerTimeout) {
      return new Connection();
    }

    @Override
    public Receive createReceive() {
      return AbstractActor.emptyBehavior();
    }
    // #extension-usage-actor
  }
  // #extension-usage-actor

  public static class Connection {}

  @Test
  public void demonstrateHowToCreateAndUseAnAkkaExtensionInJava() {
    final ActorSystem system = null;
    try {
      // #extension-usage
      // typically you would use static import of the Settings.SettingsProvider field
      String dbUri = Settings.SettingsProvider.get(system).DB_URI;
      // #extension-usage
    } catch (Exception e) {
      // do nothing
    }
  }
}
