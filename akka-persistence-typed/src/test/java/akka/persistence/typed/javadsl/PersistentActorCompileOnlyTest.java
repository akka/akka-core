/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.javadsl;

import static akka.actor.typed.javadsl.AskPattern.ask;

import akka.actor.testkit.typed.javadsl.TestInbox;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.japi.function.Procedure;
import akka.persistence.typed.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class PersistentActorCompileOnlyTest {

  public abstract static class Simple {

    // #event-wrapper
    public static class Wrapper<T> {
      private final T event;

      public Wrapper(T event) {
        this.event = event;
      }

      public T getEvent() {
        return event;
      }
    }

    public static class EventAdapterExample
        extends EventAdapter<SimpleEvent, Wrapper<SimpleEvent>> {
      @Override
      public Wrapper<SimpleEvent> toJournal(SimpleEvent simpleEvent) {
        return new Wrapper<>(simpleEvent);
      }

      @Override
      public String manifest(SimpleEvent event) {
        return "";
      }

      @Override
      public EventSeq<SimpleEvent> fromJournal(
          Wrapper<SimpleEvent> simpleEventWrapper, String manifest) {
        return EventSeq.single(simpleEventWrapper.getEvent());
      }
    }
    // #event-wrapper

    // try varargs
    private EventSeq<SimpleEvent> many = EventSeq.many(new SimpleEvent("a"), new SimpleEvent("b"));

    public static class SimpleCommand {
      public final String data;

      public SimpleCommand(String data) {
        this.data = data;
      }
    }

    static class SimpleEvent {
      private final String data;

      SimpleEvent(String data) {
        this.data = data;
      }
    }

    static class SimpleState {
      private final List<String> events;

      SimpleState(List<String> events) {
        this.events = events;
      }

      SimpleState() {
        this.events = new ArrayList<>();
      }

      SimpleState addEvent(SimpleEvent event) {
        List<String> newEvents = new ArrayList<>(events);
        newEvents.add(event.data);
        return new SimpleState(newEvents);
      }
    }

    public static EventSourcedBehavior<SimpleCommand, SimpleEvent, SimpleState> pb =
        new EventSourcedBehavior<SimpleCommand, SimpleEvent, SimpleState>(
            PersistenceId.ofUniqueId("p1")) {

          @Override
          public SimpleState emptyState() {
            return new SimpleState();
          }

          @Override
          public CommandHandler<SimpleCommand, SimpleEvent, SimpleState> commandHandler() {
            return (state, cmd) -> Effect().persist(new SimpleEvent(cmd.data));
          }

          @Override
          public EventHandler<SimpleState, SimpleEvent> eventHandler() {
            return SimpleState::addEvent;
          }

          // #install-event-adapter
          @Override
          public EventAdapter<SimpleEvent, Wrapper<SimpleEvent>> eventAdapter() {
            return new EventAdapterExample();
          }
          // #install-event-adapter

          @Override
          public SnapshotAdapter<SimpleState> snapshotAdapter() {
            return new SnapshotAdapter<SimpleState>() {

              @Override
              public Object toJournal(SimpleState simpleState) {
                return simpleState;
              }

              @Override
              public SimpleState fromJournal(Object from) {
                return (SimpleState) from;
              }
            };
          }
        };

    static class AdditionalSettings
        extends EventSourcedBehavior<SimpleCommand, SimpleEvent, SimpleState> {

      public AdditionalSettings(PersistenceId persistenceId) {
        super(PersistenceId.ofUniqueId("p1"));
      }

      @Override
      public SimpleState emptyState() {
        return new SimpleState();
      }

      @Override
      public CommandHandler<SimpleCommand, SimpleEvent, SimpleState> commandHandler() {
        return (state, cmd) -> Effect().persist(new SimpleEvent(cmd.data));
      }

      @Override
      public EventHandler<SimpleState, SimpleEvent> eventHandler() {
        return SimpleState::addEvent;
      }

      @Override
      public Recovery recovery() {
        return Recovery.withSnapshotSelectionCriteria(SnapshotSelectionCriteria.none());
      }

      @Override
      public String journalPluginId() {
        return "other.journal";
      }

      @Override
      public String snapshotPluginId() {
        return "other.snapshot-store";
      }
    }
  }

  abstract static class WithAck {
    public static class Ack {}

    interface MyCommand {}

    public static class Cmd implements MyCommand {
      private final String data;
      private final ActorRef<Ack> replyTo;

      public Cmd(String data, ActorRef<Ack> replyTo) {
        this.data = data;
        this.replyTo = replyTo;
      }
    }

    interface MyEvent {}

    public static class Evt implements MyEvent {
      private final String data;

      public Evt(String data) {
        this.data = data;
      }
    }

    static class ExampleState {
      private List<String> events = new ArrayList<>();
    }

    // #commonChainedEffects
    // Example factoring out a chained effect to use in several places with `thenRun`
    static final Procedure<ExampleState> commonChainedEffect =
        state -> System.out.println("Command handled!");

    // #commonChainedEffects

    private EventSourcedBehavior<MyCommand, MyEvent, ExampleState> pa =
        new EventSourcedBehavior<MyCommand, MyEvent, ExampleState>(PersistenceId.ofUniqueId("pa")) {

          @Override
          public ExampleState emptyState() {
            return new ExampleState();
          }

          // #commonChainedEffects
          @Override
          public CommandHandler<MyCommand, MyEvent, ExampleState> commandHandler() {
            return newCommandHandlerBuilder()
                .forStateType(ExampleState.class)
                .onCommand(
                    Cmd.class,
                    (state, cmd) ->
                        Effect()
                            .persist(new Evt(cmd.data))
                            .thenRun(() -> cmd.replyTo.tell(new Ack()))
                            .thenRun(commonChainedEffect))
                .build();
          }
          // #commonChainedEffects

          @Override
          public EventHandler<ExampleState, MyEvent> eventHandler() {
            return newEventHandlerBuilder()
                .forStateType(ExampleState.class)
                .onEvent(
                    Evt.class,
                    (state, event) -> {
                      state.events.add(event.data);
                      return state;
                    })
                .build();
          }
        };
  }

  abstract static class RecoveryComplete {
    interface Command {}

    static class DoSideEffect implements Command {
      final String data;

      DoSideEffect(String data) {
        this.data = data;
      }
    }

    static class AcknowledgeSideEffect implements Command {
      final int correlationId;

      AcknowledgeSideEffect(int correlationId) {
        this.correlationId = correlationId;
      }
    }

    interface Event {}

    static class IntentRecord implements Event {
      final int correlationId;
      final String data;

      IntentRecord(int correlationId, String data) {
        this.correlationId = correlationId;
        this.data = data;
      }
    }

    static class SideEffectAcknowledged implements Event {
      final int correlationId;

      SideEffectAcknowledged(int correlationId) {
        this.correlationId = correlationId;
      }
    }

    static class EventsInFlight {
      final int nextCorrelationId;
      final Map<Integer, String> dataByCorrelationId;

      EventsInFlight(int nextCorrelationId, Map<Integer, String> dataByCorrelationId) {
        this.nextCorrelationId = nextCorrelationId;
        this.dataByCorrelationId = dataByCorrelationId;
      }
    }

    static class Request {
      final int correlationId;
      final String data;
      final ActorRef<Response> sender;

      Request(int correlationId, String data, ActorRef<Response> sender) {
        this.correlationId = correlationId;
        this.data = data;
        this.sender = sender;
      }
    }

    static class Response {
      final int correlationId;

      Response(int correlationId) {
        this.correlationId = correlationId;
      }
    }

    static ActorRef<Request> sideEffectProcessor = TestInbox.<Request>create().getRef();
    static Duration timeout = Duration.ofSeconds(1);

    private static void performSideEffect(
        ActorRef<AcknowledgeSideEffect> sender,
        int correlationId,
        String data,
        Scheduler scheduler) {
      CompletionStage<Response> what =
          ask(
              sideEffectProcessor,
              (ActorRef<Response> ar) -> new Request(correlationId, data, ar),
              timeout,
              scheduler);
      what.thenApply(r -> new AcknowledgeSideEffect(r.correlationId)).thenAccept(sender::tell);
    }

    public Behavior<Command> behavior(PersistenceId persistenceId) {
      return Behaviors.setup(ctx -> new MyPersistentBehavior(persistenceId, ctx));
    }

    class MyPersistentBehavior
        extends EventSourcedBehavior<Command, Event, RecoveryComplete.EventsInFlight> {

      // this makes the context available to the command handler etc.
      private final ActorContext<Command> ctx;

      public MyPersistentBehavior(PersistenceId persistenceId, ActorContext<Command> ctx) {
        super(persistenceId);
        this.ctx = ctx;
      }

      @Override
      public EventsInFlight emptyState() {
        return new EventsInFlight(0, Collections.emptyMap());
      }

      @Override
      public CommandHandler<Command, Event, EventsInFlight> commandHandler() {
        return newCommandHandlerBuilder()
            .forAnyState()
            .onCommand(
                DoSideEffect.class,
                (state, cmd) ->
                    Effect()
                        .persist(new IntentRecord(state.nextCorrelationId, cmd.data))
                        .thenRun(
                            () ->
                                performSideEffect(
                                    ctx.getSelf().narrow(),
                                    state.nextCorrelationId,
                                    cmd.data,
                                    ctx.getSystem().scheduler())))
            .onCommand(
                AcknowledgeSideEffect.class,
                (state, command) ->
                    Effect().persist(new SideEffectAcknowledged(command.correlationId)))
            .build();
      }

      @Override
      public EventHandler<EventsInFlight, Event> eventHandler() {
        return newEventHandlerBuilder()
            .forAnyState()
            .onEvent(
                IntentRecord.class,
                (state, event) -> {
                  int nextCorrelationId = event.correlationId;
                  Map<Integer, String> newOutstanding = new HashMap<>(state.dataByCorrelationId);
                  newOutstanding.put(event.correlationId, event.data);
                  return new EventsInFlight(nextCorrelationId, newOutstanding);
                })
            .onEvent(
                SideEffectAcknowledged.class,
                (state, event) -> {
                  Map<Integer, String> newOutstanding = new HashMap<>(state.dataByCorrelationId);
                  newOutstanding.remove(event.correlationId);
                  return new EventsInFlight(state.nextCorrelationId, newOutstanding);
                })
            .build();
      }
    }
  }
}
