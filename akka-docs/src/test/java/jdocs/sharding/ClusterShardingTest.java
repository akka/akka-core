/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.sharding;

import java.util.Optional;
import java.time.Duration;

import akka.actor.AbstractActor;
import akka.actor.ActorInitializationException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.OneForOneStrategy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.ReceiveTimeout;
// #counter-extractor
import akka.cluster.sharding.ShardRegion;

// #counter-extractor

// #counter-start
import akka.japi.Option;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;

// #counter-start
import akka.persistence.AbstractPersistentActor;
import akka.japi.pf.DeciderBuilder;

// Doc code, compile only
public class ClusterShardingTest {

  ActorSystem system = null;

  ActorRef getSelf() {
    return null;
  }

  public void demonstrateUsage() {
    // #counter-extractor
    ShardRegion.MessageExtractor messageExtractor =
        new ShardRegion.MessageExtractor() {

          @Override
          public String entityId(Object message) {
            if (message instanceof Counter.EntityEnvelope)
              return String.valueOf(((Counter.EntityEnvelope) message).id);
            else if (message instanceof Counter.Get)
              return String.valueOf(((Counter.Get) message).counterId);
            else return null;
          }

          @Override
          public Object entityMessage(Object message) {
            if (message instanceof Counter.EntityEnvelope)
              return ((Counter.EntityEnvelope) message).payload;
            else return message;
          }

          @Override
          public String shardId(Object message) {
            int numberOfShards = 100;
            if (message instanceof Counter.EntityEnvelope) {
              long id = ((Counter.EntityEnvelope) message).id;
              return String.valueOf(id % numberOfShards);
            } else if (message instanceof Counter.Get) {
              long id = ((Counter.Get) message).counterId;
              return String.valueOf(id % numberOfShards);
            } else {
              return null;
            }
          }
        };
    // #counter-extractor

    // #counter-start
    Option<String> roleOption = Option.none();
    ClusterShardingSettings settings = ClusterShardingSettings.create(system);
    ActorRef startedCounterRegion =
        ClusterSharding.get(system)
            .start("Counter", Props.create(Counter.class), settings, messageExtractor);
    // #counter-start

    // #counter-usage
    ActorRef counterRegion = ClusterSharding.get(system).shardRegion("Counter");
    counterRegion.tell(new Counter.Get(123), getSelf());

    counterRegion.tell(new Counter.EntityEnvelope(123, Counter.CounterOp.INCREMENT), getSelf());
    counterRegion.tell(new Counter.Get(123), getSelf());
    // #counter-usage

    // #counter-supervisor-start
    ClusterSharding.get(system)
        .start(
            "SupervisedCounter", Props.create(CounterSupervisor.class), settings, messageExtractor);
    // #counter-supervisor-start

    // #proxy-dc
    ActorRef counterProxyDcB =
        ClusterSharding.get(system)
            .startProxy(
                "Counter",
                Optional.empty(),
                Optional.of("B"), // data center name
                messageExtractor);
    // #proxy-dc
  }

  public void demonstrateUsage2() {
    ShardRegion.MessageExtractor messageExtractor =
        new ShardRegion.MessageExtractor() {

          @Override
          public String entityId(Object message) {
            if (message instanceof Counter.EntityEnvelope)
              return String.valueOf(((Counter.EntityEnvelope) message).id);
            else if (message instanceof Counter.Get)
              return String.valueOf(((Counter.Get) message).counterId);
            else return null;
          }

          @Override
          public Object entityMessage(Object message) {
            if (message instanceof Counter.EntityEnvelope)
              return ((Counter.EntityEnvelope) message).payload;
            else return message;
          }

          // #extractShardId-StartEntity
          @Override
          public String shardId(Object message) {
            int numberOfShards = 100;
            if (message instanceof Counter.EntityEnvelope) {
              long id = ((Counter.EntityEnvelope) message).id;
              return String.valueOf(id % numberOfShards);
            } else if (message instanceof Counter.Get) {
              long id = ((Counter.Get) message).counterId;
              return String.valueOf(id % numberOfShards);
            } else if (message instanceof ShardRegion.StartEntity) {
              long id = Long.valueOf(((ShardRegion.StartEntity) message).entityId());
              return String.valueOf(id % numberOfShards);
            } else {
              return null;
            }
          }
          // #extractShardId-StartEntity

        };
  }

  public // #counter-actor
  static class Counter extends AbstractPersistentActor {

    public enum CounterOp {
      INCREMENT,
      DECREMENT
    }

    public static class Get {
      public final long counterId;

      public Get(long counterId) {
        this.counterId = counterId;
      }
    }

    public static class EntityEnvelope {
      public final long id;
      public final Object payload;

      public EntityEnvelope(long id, Object payload) {
        this.id = id;
        this.payload = payload;
      }
    }

    public static class CounterChanged {
      public final int delta;

      public CounterChanged(int delta) {
        this.delta = delta;
      }
    }

    int count = 0;

    // getSelf().path().name() is the entity identifier (utf-8 URL-encoded)
    @Override
    public String persistenceId() {
      return "Counter-" + getSelf().path().name();
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      getContext().setReceiveTimeout(Duration.ofSeconds(120));
    }

    void updateState(CounterChanged event) {
      count += event.delta;
    }

    @Override
    public Receive createReceiveRecover() {
      return receiveBuilder().match(CounterChanged.class, this::updateState).build();
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(Get.class, this::receiveGet)
          .matchEquals(CounterOp.INCREMENT, msg -> receiveIncrement())
          .matchEquals(CounterOp.DECREMENT, msg -> receiveDecrement())
          .matchEquals(ReceiveTimeout.getInstance(), msg -> passivate())
          .build();
    }

    private void receiveGet(Get msg) {
      getSender().tell(count, getSelf());
    }

    private void receiveIncrement() {
      persist(new CounterChanged(+1), this::updateState);
    }

    private void receiveDecrement() {
      persist(new CounterChanged(-1), this::updateState);
    }

    private void passivate() {
      getContext().getParent().tell(new ShardRegion.Passivate(PoisonPill.getInstance()), getSelf());
    }
  }

  // #counter-actor

  public // #supervisor
  static class CounterSupervisor extends AbstractActor {

    private final ActorRef counter =
        getContext().actorOf(Props.create(Counter.class), "theCounter");

    private static final SupervisorStrategy strategy =
        new OneForOneStrategy(
            DeciderBuilder.match(IllegalArgumentException.class, e -> SupervisorStrategy.resume())
                .match(ActorInitializationException.class, e -> SupervisorStrategy.stop())
                .match(Exception.class, e -> SupervisorStrategy.restart())
                .matchAny(o -> SupervisorStrategy.escalate())
                .build());

    @Override
    public SupervisorStrategy supervisorStrategy() {
      return strategy;
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(Object.class, msg -> counter.forward(msg, getContext()))
          .build();
    }
  }
  // #supervisor

}
