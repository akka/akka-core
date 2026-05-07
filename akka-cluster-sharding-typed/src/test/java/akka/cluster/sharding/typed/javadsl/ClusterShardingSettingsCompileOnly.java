/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.javadsl;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.ClusterShardingSettings;

public class ClusterShardingSettingsCompileOnly {

  static void shouldBeUsableFromJava() {
    ActorSystem<?> system = null;
    ClusterShardingSettings.StateStoreMode mode = ClusterShardingSettings.stateStoreModeDdata();
    ClusterShardingSettings.create(system)
        .withStateStoreMode(mode)
        .withRememberEntitiesStoreMode(ClusterShardingSettings.rememberEntitiesStoreModeDdata());
  }
}
