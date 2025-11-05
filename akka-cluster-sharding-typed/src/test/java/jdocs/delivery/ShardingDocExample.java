/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.delivery;

// #imports
import akka.Done;
import akka.actor.Address;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.delivery.ConsumerController;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

// #imports

// #producer
import akka.cluster.sharding.typed.delivery.ShardingProducerController;

// #producer

// #init
import akka.cluster.sharding.typed.delivery.ShardingConsumerController;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import akka.actor.typed.ActorSystem;

// #init

interface ShardingDocExample {

  // #consumer

  interface DB {
    CompletionStage<Done> save(String id, TodoList.State state);

    CompletionStage<TodoList.State> load(String id);
  }

  public class TodoList {
    interface Command {}

    public static class AddTask implements Command {
      public final String item;

      public AddTask(String item) {
        this.item = item;
      }
    }

    public static class CompleteTask implements Command {
      public final String item;

      public CompleteTask(String item) {
        this.item = item;
      }
    }

    private static class InitialState implements Command {
      final State state;

      private InitialState(State state) {
        this.state = state;
      }
    }

    private static class SaveSuccess implements Command {
      final ActorRef<ConsumerController.Confirmed> confirmTo;

      private SaveSuccess(ActorRef<ConsumerController.Confirmed> confirmTo) {
        this.confirmTo = confirmTo;
      }
    }

    private static class DBError implements Command {
      final Exception cause;

      private DBError(Throwable cause) {
        if (cause instanceof Exception) this.cause = (Exception) cause;
        else this.cause = new RuntimeException(cause.getMessage(), cause);
      }
    }

    private static class CommandDelivery implements Command {
      final Command command;
      final ActorRef<ConsumerController.Confirmed> confirmTo;

      private CommandDelivery(Command command, ActorRef<ConsumerController.Confirmed> confirmTo) {
        this.command = command;
        this.confirmTo = confirmTo;
      }
    }

    public static class State {
      public final List<String> tasks;

      public State(List<String> tasks) {
        this.tasks = Collections.unmodifiableList(tasks);
      }

      public State add(String task) {
        ArrayList<String> copy = new ArrayList<>(tasks);
        copy.add(task);
        return new State(copy);
      }

      public State remove(String task) {
        ArrayList<String> copy = new ArrayList<>(tasks);
        copy.remove(task);
        return new State(copy);
      }
    }

    public static Behavior<Command> create(
        String id, DB db, ActorRef<ConsumerController.Start<Command>> consumerController) {
      return Init.create(id, db, consumerController);
    }

    private static Behavior<Command> onDBError(DBError error) throws Exception {
      throw error.cause;
    }

    static class Init extends AbstractBehavior<Command> {

      private final String id;
      private final DB db;
      private final ActorRef<ConsumerController.Start<Command>> consumerController;

      private Init(
          ActorContext<Command> context,
          String id,
          DB db,
          ActorRef<ConsumerController.Start<Command>> consumerController) {
        super(context);
        this.id = id;
        this.db = db;
        this.consumerController = consumerController;
      }

      static Behavior<Command> create(
          String id, DB db, ActorRef<ConsumerController.Start<Command>> consumerController) {
        return Behaviors.setup(
            context -> {
              context.pipeToSelf(
                  db.load(id),
                  (state, exc) -> {
                    if (exc == null) return new InitialState(state);
                    else return new DBError(exc);
                  });

              return new Init(context, id, db, consumerController);
            });
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(InitialState.class, this::onInitialState)
            .onMessage(DBError.class, TodoList::onDBError)
            .build();
      }

      private Behavior<Command> onInitialState(InitialState initial) {
        ActorRef<ConsumerController.Delivery<Command>> deliveryAdapter =
            getContext()
                .messageAdapter(
                    ConsumerController.deliveryClass(),
                    d -> new CommandDelivery(d.message(), d.confirmTo()));
        consumerController.tell(new ConsumerController.Start<>(deliveryAdapter));

        return Active.create(id, db, initial.state);
      }
    }

    static class Active extends AbstractBehavior<Command> {

      private final String id;
      private final DB db;
      private State state;

      private Active(ActorContext<Command> context, String id, DB db, State state) {
        super(context);
        this.id = id;
        this.db = db;
        this.state = state;
      }

      static Behavior<Command> create(String id, DB db, State state) {
        return Behaviors.setup(context -> new Active(context, id, db, state));
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(CommandDelivery.class, this::onDelivery)
            .onMessage(SaveSuccess.class, this::onSaveSuccess)
            .onMessage(DBError.class, TodoList::onDBError)
            .build();
      }

      private Behavior<Command> onDelivery(CommandDelivery delivery) {
        if (delivery.command instanceof AddTask) {
          AddTask addTask = (AddTask) delivery.command;
          state = state.add(addTask.item);
          save(state, delivery.confirmTo);
          return this;
        } else if (delivery.command instanceof CompleteTask) {
          CompleteTask completeTask = (CompleteTask) delivery.command;
          state = state.remove(completeTask.item);
          save(state, delivery.confirmTo);
          return this;
        } else {
          return Behaviors.unhandled();
        }
      }

      private void save(State newState, ActorRef<ConsumerController.Confirmed> confirmTo) {
        getContext()
            .pipeToSelf(
                db.save(id, newState),
                (state, exc) -> {
                  if (exc == null) return new SaveSuccess(confirmTo);
                  else return new DBError(exc);
                });
      }

      private Behavior<Command> onSaveSuccess(SaveSuccess success) {
        success.confirmTo.tell(ConsumerController.confirmed());
        return this;
      }
    }
  }
  // #consumer

  // #producer
  public class TodoService {

    interface Command {}

    public static class UpdateTodo implements Command {
      public final String listId;
      public final String item;
      public final boolean completed;
      public final ActorRef<Response> replyTo;

      public UpdateTodo(String listId, String item, boolean completed, ActorRef<Response> replyTo) {
        this.listId = listId;
        this.item = item;
        this.completed = completed;
        this.replyTo = replyTo;
      }
    }

    public enum Response {
      ACCEPTED,
      REJECTED,
      MAYBE_ACCEPTED
    }

    private static class Confirmed implements Command {
      final ActorRef<Response> originalReplyTo;

      private Confirmed(ActorRef<Response> originalReplyTo) {
        this.originalReplyTo = originalReplyTo;
      }
    }

    private static class TimedOut implements Command {
      final ActorRef<Response> originalReplyTo;

      private TimedOut(ActorRef<Response> originalReplyTo) {
        this.originalReplyTo = originalReplyTo;
      }
    }

    private static class WrappedRequestNext implements Command {
      final ShardingProducerController.RequestNext<TodoList.Command> next;

      private WrappedRequestNext(ShardingProducerController.RequestNext<TodoList.Command> next) {
        this.next = next;
      }
    }

    public static Behavior<Command> create(
        ActorRef<ShardingProducerController.Command<TodoList.Command>> producerController) {
      return Init.create(producerController);
    }

    static class Init extends AbstractBehavior<TodoService.Command> {

      static Behavior<Command> create(
          ActorRef<ShardingProducerController.Command<TodoList.Command>> producerController) {
        return Behaviors.setup(
            context -> {
              ActorRef<ShardingProducerController.RequestNext<TodoList.Command>>
                  requestNextAdapter =
                      context.messageAdapter(
                          ShardingProducerController.requestNextClass(), WrappedRequestNext::new);
              producerController.tell(new ShardingProducerController.Start<>(requestNextAdapter));

              return new Init(context);
            });
      }

      private Init(ActorContext<Command> context) {
        super(context);
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(WrappedRequestNext.class, w -> Active.create(w.next))
            .onMessage(
                UpdateTodo.class,
                command -> {
                  // not hooked up with shardingProducerController yet
                  command.replyTo.tell(Response.REJECTED);
                  return this;
                })
            .build();
      }
    }

    static class Active extends AbstractBehavior<TodoService.Command> {

      private ShardingProducerController.RequestNext<TodoList.Command> requestNext;

      static Behavior<Command> create(
          ShardingProducerController.RequestNext<TodoList.Command> requestNext) {
        return Behaviors.setup(context -> new Active(context, requestNext));
      }

      private Active(
          ActorContext<Command> context,
          ShardingProducerController.RequestNext<TodoList.Command> requestNext) {
        super(context);
        this.requestNext = requestNext;
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(WrappedRequestNext.class, this::onRequestNext)
            .onMessage(UpdateTodo.class, this::onUpdateTodo)
            .onMessage(Confirmed.class, this::onConfirmed)
            .onMessage(TimedOut.class, this::onTimedOut)
            .build();
      }

      private Behavior<Command> onRequestNext(WrappedRequestNext w) {
        requestNext = w.next;
        return this;
      }

      private Behavior<Command> onUpdateTodo(UpdateTodo command) {
        Integer buffered = requestNext.getBufferedForEntitiesWithoutDemand().get(command.listId);
        if (buffered != null && buffered >= 100) {
          command.replyTo.tell(Response.REJECTED);
        } else {
          TodoList.Command requestMsg;
          if (command.completed) requestMsg = new TodoList.CompleteTask(command.item);
          else requestMsg = new TodoList.AddTask(command.item);
          getContext()
              .ask(
                  Done.class,
                  requestNext.askNextTo(),
                  Duration.ofSeconds(5),
                  askReplyTo ->
                      new ShardingProducerController.MessageWithConfirmation<>(
                          command.listId, requestMsg, askReplyTo),
                  (done, exc) -> {
                    if (exc == null) return new Confirmed(command.replyTo);
                    else return new TimedOut(command.replyTo);
                  });
        }
        return this;
      }

      private Behavior<Command> onConfirmed(Confirmed confirmed) {
        confirmed.originalReplyTo.tell(Response.ACCEPTED);
        return this;
      }

      private Behavior<Command> onTimedOut(TimedOut timedOut) {
        timedOut.originalReplyTo.tell(Response.MAYBE_ACCEPTED);
        return this;
      }
    }
  }
  // #producer

  static void illustrateInit() {
    Behaviors.setup(
        context -> {
          // #init
          final DB db = theDatabaseImplementation();

          ActorSystem<Void> system = context.getSystem();

          EntityTypeKey<ConsumerController.SequencedMessage<TodoList.Command>> entityTypeKey =
              EntityTypeKey.create(ShardingConsumerController.entityTypeKeyClass(), "todo");

          ActorRef<ShardingEnvelope<ConsumerController.SequencedMessage<TodoList.Command>>> region =
              ClusterSharding.get(system)
                  .init(
                      Entity.of(
                          entityTypeKey,
                          entityContext ->
                              ShardingConsumerController.create(
                                  start ->
                                      TodoList.create(entityContext.getEntityId(), db, start))));

          Address selfAddress = Cluster.get(system).selfMember().address();
          String producerId = "todo-producer-" + selfAddress.hostPort();

          ActorRef<ShardingProducerController.Command<TodoList.Command>> producerController =
              context.spawn(
                  ShardingProducerController.create(
                      TodoList.Command.class, producerId, region, Optional.empty()),
                  "producerController");

          context.spawn(TodoService.create(producerController), "producer");
          // #init

          return Behaviors.empty();
        });
  }

  static DB theDatabaseImplementation() {
    return null;
  }
}
