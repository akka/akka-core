/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.DispatcherSelector;
// #pool
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.GroupRouter;
import akka.actor.typed.javadsl.PoolRouter;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;

import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

// #pool

public class RouterTest extends JUnitSuite {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  static // #routee
  class Worker {
    interface Command {}

    static class DoLog implements Command {
      public final String text;

      public DoLog(String text) {
        this.text = text;
      }
    }

    static final Behavior<Command> create() {
      return Behaviors.setup(
          context -> {
            context.getLog().info("Starting worker");

            return Behaviors.receive(Command.class)
                .onMessage(DoLog.class, doLog -> onDoLog(context, doLog))
                .build();
          });
    }

    private static Behavior<Command> onDoLog(ActorContext<Command> context, DoLog doLog) {
      context.getLog().info("Got message {}", doLog.text);
      return Behaviors.same();
    }
  }

  static class Proxy {

    public final ServiceKey<Message> registeringKey =
        ServiceKey.create(Message.class, "aggregator-key");

    public String mapping(Message message) {
      return message.getId();
    }

    static class Message {

      public Message(String id, String content) {
        this.id = id;
        this.content = content;
      }

      private String content;
      private String id;

      public final String getContent() {
        return content;
      }

      public final String getId() {
        return id;
      }
    }

    static Behavior<Message> create(ActorRef<String> monitor) {
      return Behaviors.receive(Message.class)
          .onMessage(Message.class, in -> onMyMessage(monitor, in))
          .build();
    }

    private static Behavior<Message> onMyMessage(ActorRef<String> monitor, Message message) {
      monitor.tell(message.getId());
      return Behaviors.same();
    }
  }

  // #routee

  // intentionally outside the routee scope
  static class DoBroadcastLog extends Worker.DoLog {

    public DoBroadcastLog(String text) {
      super(text);
    }
  }

  static Behavior<Void> showPoolRouting() {
    return
    // #pool
    // This would be defined within your actor class
    Behaviors.setup(
        context -> {
          int poolSize = 4;
          PoolRouter<Worker.Command> pool =
              Routers.pool(
                  poolSize,
                  // make sure the workers are restarted if they fail
                  Behaviors.supervise(Worker.create()).onFailure(SupervisorStrategy.restart()));
          ActorRef<Worker.Command> router = context.spawn(pool, "worker-pool");

          for (int i = 0; i < 10; i++) {
            router.tell(new Worker.DoLog("msg " + i));
          }
          // #pool

          // #pool-dispatcher
          // make sure workers use the default blocking IO dispatcher
          PoolRouter<Worker.Command> blockingPool =
              pool.withRouteeProps(DispatcherSelector.blocking());
          // spawn head router using the same executor as the parent
          ActorRef<Worker.Command> blockingRouter =
              context.spawn(blockingPool, "blocking-pool", DispatcherSelector.sameAsParent());
          // #pool-dispatcher

          // #strategy
          PoolRouter<Worker.Command> alternativePool = pool.withPoolSize(2).withRoundRobinRouting();
          // #strategy

          // #broadcast
          PoolRouter<Worker.Command> broadcastingPool =
              pool.withBroadcastPredicate(msg -> msg instanceof DoBroadcastLog);
          // #broadcast

          return Behaviors.empty();
          // #pool
        });
    // #pool

  }

  static Behavior<Void> showGroupRouting() {
    // #group
    ServiceKey<Worker.Command> serviceKey = ServiceKey.create(Worker.Command.class, "log-worker");

    // #group
    return
    // #group
    Behaviors.setup(
        context -> {

          // this would likely happen elsewhere - if we create it locally we
          // can just as well use a pool
          ActorRef<Worker.Command> worker = context.spawn(Worker.create(), "worker");
          context.getSystem().receptionist().tell(Receptionist.register(serviceKey, worker));

          GroupRouter<Worker.Command> group = Routers.group(serviceKey);
          ActorRef<Worker.Command> router = context.spawn(group, "worker-group");

          // the group router will stash messages until it sees the first listing of
          // registered
          // services from the receptionist, so it is safe to send messages right away
          for (int i = 0; i < 10; i++) {
            router.tell(new Worker.DoLog("msg " + i));
          }

          return Behaviors.empty();
        });
    // #group
  }

  @Test
  public void showGroupRoutingWithConsistentHashing() throws Exception {

    TestProbe<String> probe1 = testKit.createTestProbe();
    TestProbe<String> probe2 = testKit.createTestProbe();

    Proxy proxy = new Proxy();

    ActorRef<Proxy.Message> proxy1 = testKit.spawn(proxy.create(probe1.ref()));
    ActorRef<Proxy.Message> proxy2 = testKit.spawn(proxy.create(probe2.ref()));

    TestProbe<Receptionist.Registered> waiterProbe = testKit.createTestProbe();
    // registering proxies

    testKit
        .system()
        .receptionist()
        .tell(Receptionist.register(proxy.registeringKey, proxy1, waiterProbe.ref()));
    testKit
        .system()
        .receptionist()
        .tell(Receptionist.register(proxy.registeringKey, proxy2, waiterProbe.ref()));
    // wait until both registrations get Receptionist.Registered

    waiterProbe.receiveSeveralMessages(2);
    // messages sent to a router with consistent hashing
    // #consistent-hashing
    ActorRef<Proxy.Message> router =
        testKit.spawn(
            Routers.group(proxy.registeringKey)
                .withConsistentHashingRouting(10, command -> proxy.mapping(command)));

    router.tell(new Proxy.Message("123", "Text1"));
    router.tell(new Proxy.Message("123", "Text2"));

    router.tell(new Proxy.Message("zh3", "Text3"));
    router.tell(new Proxy.Message("zh3", "Text4"));
    // the hash is calculated over the Proxy.Message first parameter obtained through the
    // Proxy.mapping function
    // #consistent-hashing
    // Then messages with equal Message.id reach the same actor
    // so the first message in each probe queue is equal to its second
    probe1.expectMessage(probe1.receiveMessage());
    probe2.expectMessage(probe2.receiveMessage());
  }

  public static void main(String[] args) {
    ActorSystem<Void> system =
        ActorSystem.create(
            Behaviors.setup(
                context -> {
                  context.spawn(showPoolRouting(), "pool-router-setup");
                  context.spawn(showGroupRouting(), "group-router-setup");

                  return Behaviors.empty();
                }),
            "RouterTest");
  }
}
