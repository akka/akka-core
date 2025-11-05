/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.discovery;

import akka.actor.ActorSystem;
// #lookup-dns
import akka.discovery.Discovery;
import akka.discovery.ServiceDiscovery;
// #lookup-dns
import akka.testkit.javadsl.TestKit;
import docs.discovery.DnsDiscoveryDocSpec;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public class DnsDiscoveryDocTest extends JUnitSuite {

  private static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("DnsDiscoveryDocTest", DnsDiscoveryDocSpec.config());
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void dnsDiscoveryShouldResolveAkkaIo() throws Exception {
    try {
      // #lookup-dns

      ServiceDiscovery discovery = Discovery.get(system).discovery();
      // ...
      CompletionStage<ServiceDiscovery.Resolved> result =
          discovery.lookup("foo", Duration.ofSeconds(3));
      // #lookup-dns

      result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      system.log().warning("Failed lookup akka.io, but ignoring: " + e);
      // don't fail this test
    }
  }
}
