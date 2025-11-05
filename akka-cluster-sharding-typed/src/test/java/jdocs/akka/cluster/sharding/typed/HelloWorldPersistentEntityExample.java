/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

// #persistent-entity-import
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.Effect;
import akka.persistence.typed.javadsl.EventHandler;
// #persistent-entity-import

// #persistent-entity-usage-import
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import akka.serialization.jackson.CborSerializable;
import akka.util.Timeout;
import com.fasterxml.jackson.annotation.JsonCreator;
// #persistent-entity-usage-import

public class HelloWorldPersistentEntityExample {

  // #persistent-entity-usage

  public static class HelloWorldService {
    private final ActorSystem<?> system;
    private final ClusterSharding sharding;
    private final Duration askTimeout = Duration.ofSeconds(5);

    // registration at startup
    public HelloWorldService(ActorSystem<?> system) {
      this.system = system;
      sharding = ClusterSharding.get(system);

      // registration at startup
      sharding.init(
          Entity.of(
              HelloWorld.ENTITY_TYPE_KEY,
              entityContext ->
                  HelloWorld.create(
                      entityContext.getEntityId(),
                      PersistenceId.of(
                          entityContext.getEntityTypeKey().name(), entityContext.getEntityId()))));
    }

    // usage example
    public CompletionStage<Integer> sayHello(String worldId, String whom) {
      EntityRef<HelloWorld.Command> entityRef =
          sharding.entityRefFor(HelloWorld.ENTITY_TYPE_KEY, worldId);
      CompletionStage<HelloWorld.Greeting> result =
          entityRef.ask(replyTo -> new HelloWorld.Greet(whom, replyTo), askTimeout);
      return result.thenApply(greeting -> greeting.numberOfPeople);
    }
  }
  // #persistent-entity-usage

  // #persistent-entity

  public static class HelloWorld
      extends EventSourcedBehavior<HelloWorld.Command, HelloWorld.Greeted, HelloWorld.KnownPeople> {

    // Command
    public interface Command extends CborSerializable {}

    public static final class Greet implements Command {
      public final String whom;
      public final ActorRef<Greeting> replyTo;

      public Greet(String whom, ActorRef<Greeting> replyTo) {
        this.whom = whom;
        this.replyTo = replyTo;
      }
    }

    // Response
    public static final class Greeting implements CborSerializable {
      public final String whom;
      public final int numberOfPeople;

      public Greeting(String whom, int numberOfPeople) {
        this.whom = whom;
        this.numberOfPeople = numberOfPeople;
      }
    }

    // Event
    public static final class Greeted implements CborSerializable {
      public final String whom;

      @JsonCreator
      public Greeted(String whom) {
        this.whom = whom;
      }
    }

    // State
    static final class KnownPeople implements CborSerializable {
      private Set<String> names = Collections.emptySet();

      KnownPeople() {}

      private KnownPeople(Set<String> names) {
        this.names = names;
      }

      KnownPeople add(String name) {
        Set<String> newNames = new HashSet<>(names);
        newNames.add(name);
        return new KnownPeople(newNames);
      }

      int numberOfPeople() {
        return names.size();
      }
    }

    public static final EntityTypeKey<Command> ENTITY_TYPE_KEY =
        EntityTypeKey.create(Command.class, "HelloWorld");

    public static Behavior<Command> create(String entityId, PersistenceId persistenceId) {
      return Behaviors.setup(context -> new HelloWorld(context, entityId, persistenceId));
    }

    private HelloWorld(
        ActorContext<Command> context, String entityId, PersistenceId persistenceId) {
      super(persistenceId);
      context.getLog().info("Starting HelloWorld {}", entityId);
    }

    @Override
    public KnownPeople emptyState() {
      return new KnownPeople();
    }

    @Override
    public CommandHandler<Command, Greeted, KnownPeople> commandHandler() {
      return newCommandHandlerBuilder().forAnyState().onCommand(Greet.class, this::greet).build();
    }

    private Effect<Greeted, KnownPeople> greet(KnownPeople state, Greet cmd) {
      return Effect()
          .persist(new Greeted(cmd.whom))
          .thenRun(newState -> cmd.replyTo.tell(new Greeting(cmd.whom, newState.numberOfPeople())));
    }

    @Override
    public EventHandler<KnownPeople, Greeted> eventHandler() {
      return (state, evt) -> state.add(evt.whom);
    }
  }
  // #persistent-entity
}
