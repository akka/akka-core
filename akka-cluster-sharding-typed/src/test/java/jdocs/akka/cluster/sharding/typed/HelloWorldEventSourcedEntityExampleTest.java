/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import static jdocs.akka.cluster.sharding.typed.HelloWorldPersistentEntityExample.*;
import static org.junit.Assert.assertEquals;

import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import akka.persistence.typed.PersistenceId;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class HelloWorldEventSourcedEntityExampleTest extends JUnitSuite {

  public static final Config config =
      ConfigFactory.parseString(
          "akka.actor.provider = cluster \n"
              + "akka.remote.artery.canonical.port = 0 \n"
              + "akka.remote.artery.canonical.hostname = 127.0.0.1 \n"
              + "akka.persistence.journal.plugin = \"akka.persistence.journal.inmem\" \n"
              + "akka.persistence.journal.inmem.test-serialization = on \n");

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource(config);

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private ClusterSharding _sharding = null;

  private ClusterSharding sharding() {
    if (_sharding == null) {
      // initialize first time only
      Cluster cluster = Cluster.get(testKit.system());
      cluster.manager().tell(new Join(cluster.selfMember().address()));

      ClusterSharding sharding = ClusterSharding.get(testKit.system());
      sharding.init(
          Entity.of(
              HelloWorld.ENTITY_TYPE_KEY,
              entityContext ->
                  HelloWorld.create(
                      entityContext.getEntityId(),
                      PersistenceId.of(
                          entityContext.getEntityTypeKey().name(), entityContext.getEntityId()))));
      _sharding = sharding;
    }
    return _sharding;
  }

  @Test
  public void sayHello() {
    EntityRef<HelloWorld.Command> world = sharding().entityRefFor(HelloWorld.ENTITY_TYPE_KEY, "1");
    TestProbe<HelloWorld.Greeting> probe = testKit.createTestProbe(HelloWorld.Greeting.class);
    world.tell(new HelloWorld.Greet("Alice", probe.getRef()));
    HelloWorld.Greeting greeting1 = probe.receiveMessage();
    assertEquals("Alice", greeting1.whom);
    assertEquals(1, greeting1.numberOfPeople);

    world.tell(new HelloWorld.Greet("Bob", probe.getRef()));
    HelloWorld.Greeting greeting2 = probe.receiveMessage();
    assertEquals("Bob", greeting2.whom);
    assertEquals(2, greeting2.numberOfPeople);
  }

  @Test
  public void testSerialization() {
    TestProbe<HelloWorld.Greeting> probe = testKit.createTestProbe(HelloWorld.Greeting.class);
    testKit
        .serializationTestKit()
        .verifySerialization(new HelloWorld.Greet("Alice", probe.getRef()), false);

    testKit.serializationTestKit().verifySerialization(new HelloWorld.Greeted("Alice"), false);
    testKit.serializationTestKit().verifySerialization(new HelloWorld.Greeted("Alice"), false);
    HelloWorld.KnownPeople state = new HelloWorld.KnownPeople();
    state = state.add("Alice").add("Bob");
    HelloWorld.KnownPeople state2 =
        testKit.serializationTestKit().verifySerialization(state, false);
    assertEquals(state.numberOfPeople(), state2.numberOfPeople());
  }
}
