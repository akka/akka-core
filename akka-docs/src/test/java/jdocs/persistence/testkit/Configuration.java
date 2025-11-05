/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence.testkit;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.persistence.testkit.PersistenceTestKitPlugin;
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin;
import akka.persistence.testkit.javadsl.PersistenceTestKit;
import akka.persistence.testkit.javadsl.SnapshotTestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Configuration {

  // #testkit-typed-conf
  public class PersistenceTestKitConfig {

    Config conf =
        PersistenceTestKitPlugin.getInstance()
            .config()
            .withFallback(ConfigFactory.defaultApplication());

    ActorSystem<Command> system = ActorSystem.create(new SomeBehavior(), "example", conf);

    PersistenceTestKit testKit = PersistenceTestKit.create(system);
  }
  // #testkit-typed-conf

  // #snapshot-typed-conf
  public class SnapshotTestKitConfig {

    Config conf =
        PersistenceTestKitSnapshotPlugin.getInstance()
            .config()
            .withFallback(ConfigFactory.defaultApplication());

    ActorSystem<Command> system = ActorSystem.create(new SomeBehavior(), "example", conf);

    SnapshotTestKit testKit = SnapshotTestKit.create(system);
  }
  // #snapshot-typed-conf

}

class SomeBehavior extends Behavior<Command> {
  public SomeBehavior() {
    super(1);
  }
}

class Command {}
