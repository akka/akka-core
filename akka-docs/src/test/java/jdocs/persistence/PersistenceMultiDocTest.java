/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence;

import akka.persistence.AbstractPersistentActor;
import akka.persistence.RuntimePluginConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PersistenceMultiDocTest {

  // #default-plugins
  abstract class AbstractPersistentActorWithDefaultPlugins extends AbstractPersistentActor {
    @Override
    public String persistenceId() {
      return "123";
    }
  }
  // #default-plugins

  // #override-plugins
  abstract class AbstractPersistentActorWithOverridePlugins extends AbstractPersistentActor {
    @Override
    public String persistenceId() {
      return "123";
    }

    // Absolute path to the journal plugin configuration entry in the `reference.conf`
    @Override
    public String journalPluginId() {
      return "akka.persistence.chronicle.journal";
    }

    // Absolute path to the snapshot store plugin configuration entry in the `reference.conf`
    @Override
    public String snapshotPluginId() {
      return "akka.persistence.chronicle.snapshot-store";
    }
  }
  // #override-plugins

  // #runtime-config
  abstract class AbstractPersistentActorWithRuntimePluginConfig extends AbstractPersistentActor
      implements RuntimePluginConfig {
    // Variable that is retrieved at runtime, from an external service for instance.
    String runtimeDistinction = "foo";

    @Override
    public String persistenceId() {
      return "123";
    }

    // Absolute path to the journal plugin configuration entry in the `reference.conf`
    @Override
    public String journalPluginId() {
      return "journal-plugin-" + runtimeDistinction;
    }

    // Absolute path to the snapshot store plugin configuration entry in the `reference.conf`
    @Override
    public String snapshotPluginId() {
      return "snapshot-store-plugin-" + runtimeDistinction;
    }

    // Configuration which contains the journal plugin id defined above
    @Override
    public Config journalPluginConfig() {
      return ConfigFactory.empty()
          .withValue(
              "journal-plugin-" + runtimeDistinction,
              getContext()
                  .getSystem()
                  .settings()
                  .config()
                  .getValue(
                      "journal-plugin") // or a very different configuration coming from an external
              // service.
              );
    }

    // Configuration which contains the snapshot store plugin id defined above
    @Override
    public Config snapshotPluginConfig() {
      return ConfigFactory.empty()
          .withValue(
              "snapshot-plugin-" + runtimeDistinction,
              getContext()
                  .getSystem()
                  .settings()
                  .config()
                  .getValue(
                      "snapshot-store-plugin") // or a very different configuration coming from an
              // external service.
              );
    }
  }
  // #runtime-config

}
