/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.io.dns;

import static akka.pattern.Patterns.ask;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.io.Dns;
import akka.io.dns.DnsProtocol;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import scala.Option;

public class DnsCompileOnlyDocTest {
  public static void example() {
    ActorSystem system = ActorSystem.create();

    ActorRef actorRef = null;
    final Duration timeout = Duration.ofMillis(1000L);

    // #resolve
    Option<DnsProtocol.Resolved> initial =
        Dns.get(system)
            .cache()
            .resolve(
                new DnsProtocol.Resolve("google.com", DnsProtocol.ipRequestType()),
                system,
                actorRef);
    Option<DnsProtocol.Resolved> cached =
        Dns.get(system)
            .cache()
            .cached(new DnsProtocol.Resolve("google.com", DnsProtocol.ipRequestType()));
    // #resolve

    {
      // #actor-api-inet-address
      final ActorRef dnsManager = Dns.get(system).manager();
      CompletionStage<Object> resolved =
          ask(
              dnsManager,
              new DnsProtocol.Resolve("google.com", DnsProtocol.ipRequestType()),
              timeout);
      // #actor-api-inet-address

    }

    {
      // #actor-api-async
      final ActorRef dnsManager = Dns.get(system).manager();
      CompletionStage<Object> resolved =
          ask(dnsManager, DnsProtocol.resolve("google.com"), timeout);
      // #actor-api-async
    }

    {
      // #srv
      final ActorRef dnsManager = Dns.get(system).manager();
      CompletionStage<Object> resolved =
          ask(dnsManager, DnsProtocol.resolve("google.com", DnsProtocol.srvRequestType()), timeout);
      // #srv
    }
  }
}
