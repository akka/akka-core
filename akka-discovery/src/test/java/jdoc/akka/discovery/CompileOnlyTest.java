/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdoc.akka.discovery;

import akka.actor.ActorSystem;
import akka.discovery.Discovery;
import akka.discovery.Lookup;
import akka.discovery.ServiceDiscovery;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class CompileOnlyTest {
  public static void example() {
    // #loading
    ActorSystem as = ActorSystem.create();
    ServiceDiscovery serviceDiscovery = Discovery.get(as).discovery();
    // #loading

    // #basic
    serviceDiscovery.lookup(Lookup.create("akka.io"), Duration.ofSeconds(1));
    // convenience for a Lookup with only a serviceName
    serviceDiscovery.lookup("akka.io", Duration.ofSeconds(1));
    // #basic

    // #full
    CompletionStage<ServiceDiscovery.Resolved> lookup =
        serviceDiscovery.lookup(
            Lookup.create("akka.io").withPortName("remoting").withProtocol("tcp"),
            Duration.ofSeconds(1));
    // #full

    // not-used warning
    lookup.thenAccept(System.out::println);
  }
}
