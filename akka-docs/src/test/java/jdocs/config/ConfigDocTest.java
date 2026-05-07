/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.config;

// #imports
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

// #imports
import akka.actor.testkit.typed.javadsl.ActorTestKit;

//#check-is-key-valid
import java.time.LocalDate;
import java.util.Optional;

//#check-is-key-valid

import static org.junit.Assert.*;

public class ConfigDocTest {

  private Behavior<Void> rootBehavior = Behaviors.empty();

  public void customConfig() {
    // #custom-config
    Config customConf = ConfigFactory.parseString("akka.log-config-on-start = on");
    // ConfigFactory.load sandwiches customConfig between default reference
    // config and default overrides, and then resolves it.
    ActorSystem<Void> system =
        ActorSystem.create(rootBehavior, "MySystem", ConfigFactory.load(customConf));
    // #custom-config

    ActorTestKit.shutdown(system);
  }

  public void compileOnlyPrintConfig() {
    // #dump-config
    ActorSystem<Void> system = ActorSystem.create(rootBehavior, "MySystem");
    system.logConfiguration();
    // #dump-config
  }

  public void compileOnlySeparateApps() {
    // #separate-apps
    Config config = ConfigFactory.load();
    ActorSystem<Void> app1 =
        ActorSystem.create(rootBehavior, "MyApp1", config.getConfig("myapp1").withFallback(config));
    ActorSystem<Void> app2 =
        ActorSystem.create(
            rootBehavior,
            "MyApp2",
            config.getConfig("myapp2").withOnlyPath("akka").withFallback(config));
    // #separate-apps
  }

  public ActorSystem createConfiguredSystem() {
    // #custom-config-2
    // make a Config with just your special setting
    Config myConfig = ConfigFactory.parseString("something=somethingElse");
    // load the normal config stack (system props,
    // then application.conf, then reference.conf)
    Config regularConfig = ConfigFactory.load();
    // override regular stack with myConfig
    Config combined = myConfig.withFallback(regularConfig);
    // put the result in between the overrides
    // (system props) and defaults again
    Config complete = ConfigFactory.load(combined);
    // create ActorSystem
    ActorSystem system = ActorSystem.create(rootBehavior, "myname", complete);
    // #custom-config-2
    return system;
  }

  public void compileOnlyIsLicenseKeyValid() {
    //#check-is-key-valid
    ActorSystem<Void> system = ActorSystem.create(rootBehavior, "name");
    Optional<LocalDate> licenseKey = system.getLicenseKeyExpiry();

    assertFalse(licenseKey.isEmpty());

    LocalDate nextMonth = LocalDate.now().plusMonths(1);
    assertTrue(licenseKey.get().isAfter(nextMonth));

    system.terminate();
    //#check-is-key-valid
  }
}
