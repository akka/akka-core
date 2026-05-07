/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.state;

import akka.actor.ExtendedActorSystem;
import akka.persistence.state.DurableStateStoreProvider;
import akka.persistence.state.scaladsl.DurableStateStore;
import com.typesafe.config.Config;

// #plugin-provider
class MyJavaStateStoreProvider implements DurableStateStoreProvider {

  private ExtendedActorSystem system;
  private Config config;
  private String cfgPath;

  public MyJavaStateStoreProvider(ExtendedActorSystem system, Config config, String cfgPath) {
    this.system = system;
    this.config = config;
    this.cfgPath = cfgPath;
  }

  @Override
  public DurableStateStore<Object> scaladslDurableStateStore() {
    return new MyStateStore<>(this.system, this.config, this.cfgPath);
  }

  @Override
  public akka.persistence.state.javadsl.DurableStateStore<Object> javadslDurableStateStore() {
    return new MyJavaStateStore<>(this.system, this.config, this.cfgPath);
  }
}
// #plugin-provider
