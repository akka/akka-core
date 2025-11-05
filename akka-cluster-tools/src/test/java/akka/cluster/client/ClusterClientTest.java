/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.client;

import akka.actor.*;
import akka.testkit.AkkaJUnitActorSystemResource;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ClusterClientTest extends JUnitSuite {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource(
          "DistributedPubSubMediatorTest",
          ConfigFactory.parseString(
              "akka.actor.provider = \"cluster\"\n" + "akka.remote.artery.canonical.port=0"));

  private final ActorSystem system = actorSystemResource.getSystem();

  // #initialContacts
  Set<ActorPath> initialContacts() {
    return new HashSet<ActorPath>(
        Arrays.asList(
            ActorPaths.fromString("akka://OtherSys@host1:2552/system/receptionist"),
            ActorPaths.fromString("akka://OtherSys@host2:2552/system/receptionist")));
  }
  // #initialContacts

  @Test
  @Deprecated
  public void demonstrateUsage() {
    // #server
    ActorRef serviceA = system.actorOf(Props.create(Service.class), "serviceA");
    ClusterClientReceptionist.get(system).registerService(serviceA);

    ActorRef serviceB = system.actorOf(Props.create(Service.class), "serviceB");
    ClusterClientReceptionist.get(system).registerService(serviceB);
    // #server

    // #client
    final ActorRef c =
        system.actorOf(
            ClusterClient.props(
                ClusterClientSettings.create(system).withInitialContacts(initialContacts())),
            "client");
    c.tell(new ClusterClient.Send("/user/serviceA", "hello", true), ActorRef.noSender());
    c.tell(new ClusterClient.SendToAll("/user/serviceB", "hi"), ActorRef.noSender());
    // #client

    system.actorOf(Props.create(ClientListener.class, c));
    system.actorOf(
        Props.create(
            ReceptionistListener.class, ClusterClientReceptionist.get(system).underlying()));
  }

  public static class Service extends UntypedAbstractActor {
    public void onReceive(Object msg) {}
  }

  // #clientEventsListener
  public static class ClientListener extends AbstractActor {
    private final ActorRef targetClient;
    private final Set<ActorPath> contactPoints = new HashSet<>();

    public ClientListener(ActorRef targetClient) {
      this.targetClient = targetClient;
    }

    @Override
    public void preStart() {
      targetClient.tell(SubscribeContactPoints.getInstance(), sender());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              ContactPoints.class,
              msg -> {
                contactPoints.addAll(msg.getContactPoints());
                // Now do something with an up-to-date "contactPoints"
              })
          .match(
              ContactPointAdded.class,
              msg -> {
                contactPoints.add(msg.contactPoint());
                // Now do something with an up-to-date "contactPoints"
              })
          .match(
              ContactPointRemoved.class,
              msg -> {
                contactPoints.remove(msg.contactPoint());
                // Now do something with an up-to-date "contactPoints"
              })
          .build();
    }
  }
  // #clientEventsListener

  // #receptionistEventsListener
  public static class ReceptionistListener extends AbstractActor {
    private final ActorRef targetReceptionist;
    private final Set<ActorRef> clusterClients = new HashSet<>();

    public ReceptionistListener(ActorRef targetReceptionist) {
      this.targetReceptionist = targetReceptionist;
    }

    @Override
    public void preStart() {
      targetReceptionist.tell(SubscribeClusterClients.getInstance(), sender());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              ClusterClients.class,
              msg -> {
                clusterClients.addAll(msg.getClusterClients());
                // Now do something with an up-to-date "clusterClients"
              })
          .match(
              ClusterClientUp.class,
              msg -> {
                clusterClients.add(msg.clusterClient());
                // Now do something with an up-to-date "clusterClients"
              })
          .match(
              ClusterClientUnreachable.class,
              msg -> {
                clusterClients.remove(msg.clusterClient());
                // Now do something with an up-to-date "clusterClients"
              })
          .build();
    }
  }
  // #receptionistEventsListener

}
