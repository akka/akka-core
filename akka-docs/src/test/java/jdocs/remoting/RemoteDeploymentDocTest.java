/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.remoting;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import jdocs.AbstractJavaTest;
import org.junit.ClassRule;
import org.junit.Test;

import com.typesafe.config.ConfigFactory;

// #import
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.AddressFromURIString;
import akka.actor.Deploy;
import akka.actor.Props;
import akka.actor.ActorSystem;
import akka.remote.RemoteScope;
// #import

import akka.actor.AbstractActor;

import static org.junit.Assert.assertEquals;

public class RemoteDeploymentDocTest extends AbstractJavaTest {

  public static class SampleActor extends AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder().matchAny(message -> getSender().tell(getSelf(), getSelf())).build();
    }
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource(
          "RemoteDeploymentDocTest",
          ConfigFactory.parseString(
                  "   akka.actor.provider = remote\n"
                      + "    akka.remote.artery.canonical.port = 0\n"
                      + "    akka.remote.use-unsafe-remote-features-outside-cluster = on")
              .withFallback(AkkaSpec.testConf()));

  private final ActorSystem system = actorSystemResource.getSystem();

  @SuppressWarnings("unused")
  void makeAddress() {
    // #make-address-artery
    Address addr = new Address("akka", "sys", "host", 1234);
    addr = AddressFromURIString.parse("akka://sys@host:1234"); // the same
    // #make-address-artery
  }

  @Test
  public void demonstrateDeployment() {
    // #make-address
    Address addr = new Address("akka", "sys", "host", 1234);
    addr = AddressFromURIString.parse("akka://sys@host:1234"); // the same
    // #make-address
    // #deploy
    Props props = Props.create(SampleActor.class).withDeploy(new Deploy(new RemoteScope(addr)));
    ActorRef ref = system.actorOf(props);
    // #deploy
    assertEquals(addr, ref.path().address());
  }

  @Test
  public void demonstrateSampleActor() {
    // #sample-actor

    ActorRef actor = system.actorOf(Props.create(SampleActor.class), "sampleActor");
    actor.tell("Pretty slick", ActorRef.noSender());
    // #sample-actor
  }

  @Test
  public void demonstrateProgrammaticConfig() {
    // #programmatic-artery
    ConfigFactory.parseString("akka.remote.artery.canonical.hostname=\"1.2.3.4\"")
        .withFallback(ConfigFactory.load());
    // #programmatic-artery
  }
}
