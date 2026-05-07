/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import org.junit.rules.ExternalResource;

/**
 * This is a resource for creating an actor system before test start and shut it down afterwards.
 *
 * <p>To use it on a class level add this to your test class: <code>
 * &#64;ClassRule
 * public static AkkaJUnitActorSystemResource actorSystemResource =
 *   new AkkaJUnitActorSystemResource(name, config);
 *
 * private final ActorSystem system = actorSystemResource.getSystem();
 * </code> To use it on a per test level add this to your test class: <code>
 * &#64;Rule
 * public AkkaJUnitActorSystemResource actorSystemResource =
 *   new AkkaJUnitActorSystemResource(name, config);
 *
 * private ActorSystem system = null;
 *
 * &#64;Before
 * public void beforeEach() {
 *   system = actorSystemResource.getSystem();
 * }
 * </code> Note that it is important to not use <code>getSystem</code> from the constructor of the
 * test, because some test runners may create an instance of the class without actually using it
 * later, resulting in memory leaks because of not shutting down the actor system.
 */
public class AkkaJUnitActorSystemResource extends ExternalResource {
  private ActorSystem system = null;
  private final String name;
  private final Config config;

  private ActorSystem createSystem(String name, Config config) {
    try {
      if (config == null) return ActorSystem.create(name);
      else return ActorSystem.create(name, config);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public AkkaJUnitActorSystemResource(String name, Config config) {
    this.name = name;
    this.config = config;
  }

  public AkkaJUnitActorSystemResource(String name) {
    this(name, AkkaSpec.testConf());
  }

  @Override
  protected void before() throws Throwable {
    if (system == null) {
      system = createSystem(name, config);
    }
  }

  @Override
  protected void after() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  public ActorSystem getSystem() {
    if (system == null) {
      system = createSystem(name, config);
    }
    return system;
  }
}
