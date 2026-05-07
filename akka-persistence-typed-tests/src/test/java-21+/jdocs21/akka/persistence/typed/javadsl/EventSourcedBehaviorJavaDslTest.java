/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs21.akka.persistence.typed.javadsl;

import akka.Done;
import akka.actor.testkit.typed.javadsl.LogCapturing;
import akka.actor.testkit.typed.javadsl.LoggingTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.*;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.japi.Pair;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.NoOffset;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.Sequence;
import akka.persistence.testkit.PersistenceTestKitPlugin;
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin;
import akka.persistence.testkit.query.javadsl.PersistenceTestKitReadJournal;
import akka.persistence.typed.*;
import akka.persistence.typed.javadsl.*;
import akka.serialization.jackson.CborSerializable;
import akka.stream.javadsl.Sink;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.*;

import static akka.Done.done;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class EventSourcedBehaviorJavaDslTest extends JUnitSuite {

  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(
          ConfigFactory.parseString(
                  "akka.loglevel = INFO\n" + "akka.loggers = [\"akka.testkit.TestEventListener\"]")
              .withFallback(PersistenceTestKitPlugin.getInstance().config())
              .withFallback(PersistenceTestKitSnapshotPlugin.config()));

  @Rule public final LogCapturing logCapturing = new LogCapturing();

  private PersistenceTestKitReadJournal queries =
      PersistenceQuery.get(Adapter.toClassic(testKit.system()))
          .getReadJournalFor(
              PersistenceTestKitReadJournal.class, PersistenceTestKitReadJournal.Identifier());

  public sealed interface Command extends CborSerializable {}

  public enum Increment implements Command {
    INSTANCE
  }

  public enum Increment100OnTimeout implements Command {
    INSTANCE
  }

  public enum IncrementLater implements Command {
    INSTANCE
  }

  public enum DelayFinished implements Command {
    INSTANCE
  }

  public enum EmptyEventsListAndThenLog implements Command {
    INSTANCE
  }

  public enum IncrementTwiceAndLog implements Command {
    INSTANCE
  }

  public record IncrementWithConfirmation(ActorRef<Done> replyTo) implements Command {}

  public enum StopThenLog implements Command {
    INSTANCE
  }

  public enum Timeout implements Command {
    INSTANCE
  }

  public record GetValue(ActorRef<State> replyTo) implements Command {}

  // need the JsonTypeInfo because of the Wrapper
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({@JsonSubTypes.Type(value = Incremented.class, name = "incremented")})
  public sealed interface Event extends CborSerializable {}

  public record Incremented(int delta) implements Event {}

  public record State(int value, List<Integer> history) implements CborSerializable {
    static final State EMPTY = new State(0, Collections.emptyList());

    State incrementedBy(int delta) {
      List<Integer> newHistory = new ArrayList<>(history);
      newHistory.add(value);
      return new State(value + delta, newHistory);
    }
  }

  public enum Tick {
    INSTANCE
  }

  private Behavior<Command> counter(PersistenceId persistenceId) {
    return Behaviors.setup(ctx -> new CounterBehavior(persistenceId, ctx));
  }

  @SuppressWarnings("unused")
  private static class CounterBehavior extends EventSourcedOnCommandBehavior<Command, Event, State> {
    private final ActorContext<Command> ctx;

    CounterBehavior(PersistenceId persistenceId, ActorContext<Command> ctx) {
      super(
          persistenceId,
          SupervisorStrategy.restartWithBackoff(Duration.ofMillis(1), Duration.ofMillis(5), 0.1));
      this.ctx = ctx;
    }

    @Override
    public Effect<Event, State> onCommand(State state, Command command) {
      return switch (command) {
        case Increment increment -> increment(state, increment);
        case IncrementWithConfirmation incrementWithConfirmation -> incrementWithConfirmation(state, incrementWithConfirmation);
        case GetValue getValue -> getValue(state, getValue);
        case IncrementLater incrementLater -> incrementLater(state, incrementLater);
        case DelayFinished delayFinished -> delayFinished(state, delayFinished);
        case Increment100OnTimeout increment100OnTimeout -> increment100OnTimeout(state, increment100OnTimeout);
        case Timeout timeout -> timeout(state, timeout);
        case EmptyEventsListAndThenLog emptyEventsListAndThenLog -> emptyEventsListAndThenLog(state, emptyEventsListAndThenLog);
        case StopThenLog stopThenLog -> stopThenLog(state, stopThenLog);
        case IncrementTwiceAndLog incrementTwiceAndLog -> incrementTwiceAndLog(state, incrementTwiceAndLog);
      };
    }

    private Effect<Event, State> increment(State state, Increment command) {
      return Effect().persist(new Incremented(1));
    }

    private ReplyEffect<Event, State> incrementWithConfirmation(
        State state, IncrementWithConfirmation command) {
      return Effect().persist(new Incremented(1)).thenReply(command.replyTo, newState -> done());
    }

    private ReplyEffect<Event, State> getValue(State state, GetValue command) {
      return Effect().reply(command.replyTo, state);
    }

    private Effect<Event, State> incrementLater(State state, IncrementLater command) {
      ActorRef<Object> delay =
          ctx.spawnAnonymous(
              Behaviors.withTimers(
                  timers -> {
                    timers.startSingleTimer(Tick.INSTANCE, Tick.INSTANCE, Duration.ofMillis(10));
                    return Behaviors.receive((context, o) -> Behaviors.stopped());
                  }));
      ctx.watchWith(delay, DelayFinished.INSTANCE);
      return Effect().none();
    }

    private Effect<Event, State> delayFinished(State state, DelayFinished command) {
      return Effect().persist(new Incremented(10));
    }

    private Effect<Event, State> increment100OnTimeout(State state, Increment100OnTimeout command) {
      ctx.setReceiveTimeout(Duration.ofMillis(10), Timeout.INSTANCE);
      return Effect().none();
    }

    private Effect<Event, State> timeout(State state, Timeout command) {
      return Effect().persist(new Incremented(100));
    }

    private Effect<Event, State> emptyEventsListAndThenLog(
        State state, EmptyEventsListAndThenLog command) {
      return Effect().persist(Collections.emptyList()).thenRun(s -> log());
    }

    private Effect<Event, State> stopThenLog(State state, StopThenLog command) {
      return Effect().stop().thenRun(s -> log());
    }

    private Effect<Event, State> incrementTwiceAndLog(State state, IncrementTwiceAndLog command) {
      return Effect()
          .persist(Arrays.asList(new Incremented(1), new Incremented(1)))
          .thenRun(s -> log());
    }

    @Override
    public State onEvent(State state, Event event) {
      return switch (event) {
        case Incremented incremented -> applyIncremented(state, incremented);

      };
    }

    @Override
    public State emptyState() {
      return State.EMPTY;
    }

    protected State applyIncremented(State state, Incremented event) {
      // override to probe for events
      return state.incrementedBy(event.delta);
    }

    protected void log() {
      // override to probe for logs
    }
  }

  @Test
  public void persistEvents() {
    ActorRef<Command> c = testKit.spawn(counter(PersistenceId.ofUniqueId("c1")));
    TestProbe<State> probe = testKit.createTestProbe();
    c.tell(Increment.INSTANCE);
    c.tell(new GetValue(probe.ref()));
    probe.expectMessage(new State(1, singletonList(0)));
  }

  @Test
  public void replyStoredEvents() {
    ActorRef<Command> c = testKit.spawn(counter(PersistenceId.ofUniqueId("c2")));
    TestProbe<State> probe = testKit.createTestProbe();
    c.tell(Increment.INSTANCE);
    c.tell(Increment.INSTANCE);
    c.tell(Increment.INSTANCE);
    c.tell(new GetValue(probe.ref()));
    probe.expectMessage(new State(3, Arrays.asList(0, 1, 2)));

    ActorRef<Command> c2 = testKit.spawn(counter(PersistenceId.ofUniqueId("c2")));
    c2.tell(new GetValue(probe.ref()));
    probe.expectMessage(new State(3, Arrays.asList(0, 1, 2)));
    c2.tell(Increment.INSTANCE);
    c2.tell(new GetValue(probe.ref()));
    probe.expectMessage(new State(4, Arrays.asList(0, 1, 2, 3)));
  }

  @Test
  public void thenReplyEffect() {
    ActorRef<Command> c = testKit.spawn(counter(PersistenceId.ofUniqueId("c1b")));
    TestProbe<Done> probe = testKit.createTestProbe();
    c.tell(new IncrementWithConfirmation(probe.ref()));
    probe.expectMessage(Done.getInstance());
  }

  @Test
  public void handleTerminatedSignal() {
    TestProbe<Pair<State, Incremented>> eventHandlerProbe = testKit.createTestProbe();
    Behavior<Command> counter =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("c3"), ctx) {
                  @Override
                  protected State applyIncremented(State state, Incremented event) {
                    eventHandlerProbe.ref().tell(Pair.create(state, event));
                    return super.applyIncremented(state, event);
                  }
                });
    ActorRef<Command> c = testKit.spawn(counter);
    c.tell(Increment.INSTANCE);
    c.tell(IncrementLater.INSTANCE);
    eventHandlerProbe.expectMessage(Pair.create(State.EMPTY, new Incremented(1)));
    eventHandlerProbe.expectMessage(
        Pair.create(new State(1, Collections.singletonList(0)), new Incremented(10)));
  }

  @Test
  public void handleReceiveTimeout() {
    TestProbe<Pair<State, Incremented>> eventHandlerProbe = testKit.createTestProbe();
    Behavior<Command> counter =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("c4"), ctx) {
                  @Override
                  protected State applyIncremented(State state, Incremented event) {
                    eventHandlerProbe.ref().tell(Pair.create(state, event));
                    return super.applyIncremented(state, event);
                  }
                });
    ActorRef<Command> c = testKit.spawn(counter);
    c.tell(Increment100OnTimeout.INSTANCE);
    eventHandlerProbe.expectMessage(Pair.create(State.EMPTY, new Incremented(100)));
  }

  @Test
  public void chainableSideEffectsWithEvents() {
    TestProbe<String> loggingProbe = testKit.createTestProbe();
    Behavior<Command> counter =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("c5"), ctx) {
                  @Override
                  protected void log() {
                    loggingProbe.ref().tell("logged");
                  }
                });
    ActorRef<Command> c = testKit.spawn(counter);
    c.tell(EmptyEventsListAndThenLog.INSTANCE);
    loggingProbe.expectMessage("logged");
  }

  @Test
  public void workWhenWrappedInOtherBehavior() {
    Behavior<Command> behavior =
        Behaviors.supervise(counter(PersistenceId.ofUniqueId("c6")))
            .onFailure(
                SupervisorStrategy.restartWithBackoff(
                    Duration.ofSeconds(1), Duration.ofSeconds(10), 0.1));
    ActorRef<Command> c = testKit.spawn(behavior);

    TestProbe<State> probe = testKit.createTestProbe();
    c.tell(Increment.INSTANCE);
    c.tell(new GetValue(probe.ref()));
    probe.expectMessage(new State(1, singletonList(0)));
  }

  @Test
  public void snapshot() {
    TestProbe<Optional<Throwable>> snapshotProbe = testKit.createTestProbe();

    Behavior<Command> snapshoter =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("snapshot"), ctx) {
                  @Override
                  public boolean shouldSnapshot(State state, Event event, long sequenceNr) {
                    return state.value % 2 == 0;
                  }

                  @Override
                  public SignalHandler<State> signalHandler() {
                    return newSignalHandlerBuilder()
                        .onSignal(
                            SnapshotCompleted.class,
                            (state, completed) -> {
                              snapshotProbe.ref().tell(Optional.empty());
                            })
                        .onSignal(
                            SnapshotFailed.class,
                            (state, signal) -> {
                              snapshotProbe.ref().tell(Optional.of(signal.getFailure()));
                            })
                        .build();
                  }
                });
    ActorRef<Command> c = testKit.spawn(snapshoter);
    c.tell(Increment.INSTANCE);
    c.tell(Increment.INSTANCE);
    snapshotProbe.expectMessage(Optional.empty());
    c.tell(Increment.INSTANCE);

    TestProbe<State> stateProbe = testKit.createTestProbe();
    c.tell(new GetValue(stateProbe.ref()));
    stateProbe.expectMessage(new State(3, Arrays.asList(0, 1, 2)));

    TestProbe<Pair<State, Incremented>> eventHandlerProbe = testKit.createTestProbe();
    Behavior<Command> recovered =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("snapshot"), ctx) {
                  @Override
                  protected State applyIncremented(State state, Incremented event) {
                    eventHandlerProbe.ref().tell(Pair.create(state, event));
                    return super.applyIncremented(state, event);
                  }
                });
    ActorRef<Command> c2 = testKit.spawn(recovered);
    // First 2 are snapshot
    eventHandlerProbe.expectMessage(
        Pair.create(new State(2, Arrays.asList(0, 1)), new Incremented(1)));
    c2.tell(new GetValue(stateProbe.ref()));
    stateProbe.expectMessage(new State(3, Arrays.asList(0, 1, 2)));
  }

  @Test
  public void stopThenLog() {
    TestProbe<State> probe = testKit.createTestProbe();
    ActorRef<Command> c = testKit.spawn(counter(PersistenceId.ofUniqueId("c12")));
    c.tell(StopThenLog.INSTANCE);
    probe.expectTerminated(c);
  }

  @Test
  public void postStop() {
    TestProbe<String> probe = testKit.createTestProbe();
    Behavior<Command> counter =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("c5"), ctx) {

                  @Override
                  public SignalHandler<State> signalHandler() {
                    return newSignalHandlerBuilder()
                        .onSignal(
                            PostStop.instance(),
                            state -> {
                              probe.ref().tell("stopped");
                            })
                        .build();
                  }
                });
    ActorRef<Command> c = testKit.spawn(counter);
    c.tell(StopThenLog.INSTANCE);
    probe.expectMessage("stopped");
  }

  @Test
  public void tapPersistentActor() {
    TestProbe<Object> interceptProbe = testKit.createTestProbe();
    TestProbe<Signal> signalProbe = testKit.createTestProbe();
    BehaviorInterceptor<Command, Command> tap =
        new BehaviorInterceptor<Command, Command>(Command.class) {

          @Override
          public Behavior<Command> aroundReceive(
              TypedActorContext<Command> ctx, Command msg, ReceiveTarget<Command> target) {
            interceptProbe.ref().tell(msg);
            return target.apply(ctx, msg);
          }

          @Override
          public Behavior<Command> aroundSignal(
              TypedActorContext<Command> ctx, Signal signal, SignalTarget<Command> target) {
            signalProbe.ref().tell(signal);
            return target.apply(ctx, signal);
          }
        };
    ActorRef<Command> c =
        testKit.spawn(Behaviors.intercept(() -> tap, counter(PersistenceId.ofUniqueId("tap1"))));
    c.tell(Increment.INSTANCE);
    interceptProbe.expectMessage(Increment.INSTANCE);
    signalProbe.expectNoMessage();
  }

  @Test
  public void tagEvent() throws Exception {
    Behavior<Command> tagger =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("tagging"), ctx) {
                  @Override
                  public Set<String> tagsFor(Event incremented) {
                    return Sets.newHashSet("tag1", "tag2");
                  }
                });
    ActorRef<Command> c = testKit.spawn(tagger);

    c.tell(Increment.INSTANCE);

    TestProbe<State> stateProbe = testKit.createTestProbe();
    c.tell(new GetValue(stateProbe.ref()));
    stateProbe.expectMessage(new State(1, Collections.singletonList(0)));

    List<EventEnvelope> events =
        queries
            .currentEventsByTag("tag1", NoOffset.getInstance())
            .runWith(Sink.seq(), testKit.system())
            .toCompletableFuture()
            .get();
    assertEquals(1, events.size());
    EventEnvelope eventEnvelope = events.get(0);
    assertEquals(new Sequence(1), eventEnvelope.offset());
    assertEquals("tagging", eventEnvelope.persistenceId());
    assertEquals(new Incremented(1), eventEnvelope.event());
  }

  @Test
  public void transformEvent() throws Exception {
    Behavior<Command> transformer =
        Behaviors.setup(
            ctx ->
                new CounterBehavior(PersistenceId.ofUniqueId("transform"), ctx) {
                  private final EventAdapter<Event, ?> adapter = new WrapperEventAdapter();

                  public EventAdapter<Event, ?> eventAdapter() {
                    return adapter;
                  }
                });
    ActorRef<Command> c = testKit.spawn(transformer);

    c.tell(Increment.INSTANCE);

    TestProbe<State> stateProbe = testKit.createTestProbe();
    c.tell(new GetValue(stateProbe.ref()));
    stateProbe.expectMessage(new State(1, Collections.singletonList(0)));

    List<EventEnvelope> events =
        queries
            .currentEventsByPersistenceId("transform", 0, Long.MAX_VALUE)
            .runWith(Sink.seq(), testKit.system())
            .toCompletableFuture()
            .get();
    assertEquals(1, events.size());
    EventEnvelope eventEnvelope = events.get(0);
    assertEquals(new Sequence(1), eventEnvelope.offset());
    assertEquals("transform", eventEnvelope.persistenceId());
    assertEquals(new Wrapper(new Incremented(1)), eventEnvelope.event());

    ActorRef<Command> c2 = testKit.spawn(transformer);
    c2.tell(new GetValue(stateProbe.ref()));
    stateProbe.expectMessage(new State(1, Collections.singletonList(0)));
  }

  // event-wrapper
  public static class Wrapper implements CborSerializable {
    private final Event event;

    @JsonCreator
    public Wrapper(Event event) {
      this.event = event;
    }

    public Event getEvent() {
      return event;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Wrapper wrapper = (Wrapper) o;

      return event.equals(wrapper.event);
    }

    @Override
    public int hashCode() {
      return event.hashCode();
    }

    @Override
    public String toString() {
      return "Wrapper(" + event + ")";
    }
  }

  class WrapperEventAdapter extends EventAdapter<Event, Wrapper> {
    @Override
    public Wrapper toJournal(Event event) {
      return new Wrapper(event);
    }

    @Override
    public String manifest(Event event) {
      return "";
    }

    @Override
    public EventSeq<Event> fromJournal(Wrapper wrapper, String manifest) {
      return EventSeq.single(wrapper.getEvent());
    }
  }
  // event-wrapper

  static class IncorrectExpectedStateForThenRun
      extends EventSourcedBehavior<String, String, Object> {

    private final ActorRef<String> startedProbe;

    public IncorrectExpectedStateForThenRun(
        ActorRef<String> startedProbe, PersistenceId persistenceId) {
      super(persistenceId);
      this.startedProbe = startedProbe;
    }

    @Override
    public Object emptyState() {
      return 1;
    }

    @Override
    public CommandHandler<String, String, Object> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(
              msg -> msg.equals("expect wrong type"),
              (context) ->
                  Effect()
                      .none()
                      .thenRun(
                          (String wrongType) -> {
                            // wont happen
                          }))
          .build();
    }

    @Override
    public SignalHandler<Object> signalHandler() {
      return newSignalHandlerBuilder()
          .onSignal(
              RecoveryCompleted.class,
              (state, completed) -> {
                startedProbe.tell("started!");
              })
          .build();
    }

    @Override
    public EventHandler<Object, String> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onAnyEvent((event, state) -> state); // keep Integer state
    }
  }

  @Test
  public void failOnIncorrectExpectedStateForThenRun() {
    TestProbe<String> probe = testKit.createTestProbe();
    ActorRef<String> c =
        testKit.spawn(
            new IncorrectExpectedStateForThenRun(
                probe.getRef(), PersistenceId.ofUniqueId("foiesftr")));

    probe.expectMessage("started!");

    LoggingTestKit.empty()
        .withLogLevel(Level.ERROR)
        // the error messages slightly changed in later JDKs
        .withMessageRegex(
            "(class )?java.lang.Integer cannot be cast to (class )?java.lang.String.*")
        .expect(
            testKit.system(),
            () -> {
              c.tell("expect wrong type");
              return null;
            });

    probe.expectTerminated(c);
  }

  class SequenceNumberBehavior extends EventSourcedBehavior<String, String, String> {
    private final ActorRef<String> probe;
    private final ActorContext<String> context;

    public SequenceNumberBehavior(
        PersistenceId persistenceId, ActorRef<String> probe, ActorContext<String> context) {
      super(persistenceId);
      this.probe = probe;
      this.context = context;
    }

    @Override
    public String emptyState() {
      return "";
    }

    @Override
    public CommandHandler<String, String, String> commandHandler() {
      return newCommandHandlerBuilder()
          .forAnyState()
          .onAnyCommand(
              (state, cmd) -> {
                probe.tell(lastSequenceNumber(context) + " onCommand");
                return Effect()
                    .persist(cmd)
                    .thenRun((newState) -> probe.tell(lastSequenceNumber(context) + " thenRun"));
              });
    }

    @Override
    public EventHandler<String, String> eventHandler() {
      return newEventHandlerBuilder()
          .forAnyState()
          .onAnyEvent(
              (state, event) -> {
                probe.tell(lastSequenceNumber(context) + " applyEvent");
                return state + event;
              });
    }

    @Override
    public SignalHandler<String> signalHandler() {
      return newSignalHandlerBuilder()
          .onSignal(
              RecoveryCompleted.class,
              (state, completed) -> {
                probe.tell(lastSequenceNumber(context) + " onRecoveryCompleted");
              })
          .build();
    }
  }

  @Test
  public void accessLastSequenceNumber() {
    TestProbe<String> probe = testKit.createTestProbe(String.class);
    ActorRef<String> ref =
        testKit.spawn(
            Behaviors.<String>setup(
                context ->
                    new SequenceNumberBehavior(
                        PersistenceId.ofUniqueId("seqnr1"), probe.getRef(), context)));

    probe.expectMessage("0 onRecoveryCompleted");
    ref.tell("cmd");
    probe.expectMessage("0 onCommand");
    probe.expectMessage("1 applyEvent");
    probe.expectMessage("1 thenRun");
  }
}
