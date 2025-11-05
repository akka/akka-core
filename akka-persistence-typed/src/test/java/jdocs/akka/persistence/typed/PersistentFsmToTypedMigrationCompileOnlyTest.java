/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import static jdocs.akka.persistence.typed.WebStoreCustomerFSM.*;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.persistence.typed.*;
import akka.persistence.typed.javadsl.*;
import java.time.Duration;

@Deprecated
public class PersistentFsmToTypedMigrationCompileOnlyTest {

  // #commands
  interface Command {}

  public static class AddItem implements Command {
    public final Item item;

    public AddItem(Item item) {
      this.item = item;
    }
  }

  public static class GetCurrentCart implements Command {
    public final ActorRef<ShoppingCart> replyTo;

    public GetCurrentCart(ActorRef<ShoppingCart> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public enum Buy implements Command {
    INSTANCE
  }

  public enum Leave implements Command {
    INSTANCE
  }

  private enum Timeout implements Command {
    INSTANCE
  }
  // #commands

  // #state
  abstract static class State {
    public final ShoppingCart cart;

    protected State(ShoppingCart cart) {
      this.cart = cart;
    }
  }

  public static class LookingAround extends State {
    public LookingAround(ShoppingCart cart) {
      super(cart);
    }
  }

  public static class Shopping extends State {
    public Shopping(ShoppingCart cart) {
      super(cart);
    }
  }

  public static class Inactive extends State {
    public Inactive(ShoppingCart cart) {
      super(cart);
    }
  }

  public static class Paid extends State {
    public Paid(ShoppingCart cart) {
      super(cart);
    }
  }
  // #state

  // #event-adapter
  public static class PersistentFSMEventAdapter extends EventAdapter<DomainEvent, Object> {

    @Override
    public Object toJournal(DomainEvent domainEvent) {
      // leave events as is, can't roll back to PersistentFSM
      return domainEvent;
    }

    @Override
    public String manifest(DomainEvent event) {
      return "";
    }

    @Override
    public EventSeq<DomainEvent> fromJournal(Object event, String manifest) {
      if (event instanceof akka.persistence.fsm.PersistentFSM.StateChangeEvent) {
        // In this example the state transitions can be inferred from the events
        // Alternatively the StateChangeEvent can be converted to a private event if either the
        // StateChangeEvent.stateIdentifier
        // or StateChangeEvent.timeout is required
        // Many use cases have the same timeout so it can be hard coded, otherwise it cane be stored
        // in the state
        return EventSeq.empty();
      } else {
        // If using a new domain event model the conversion would happen here
        return EventSeq.single((DomainEvent) event);
      }
    }
    // #event-adapter
  }

  public static class ShoppingCartActor extends EventSourcedBehavior<Command, DomainEvent, State> {

    private static final String TIMEOUT_KEY = "state-timeout";
    private final TimerScheduler<Command> timers;

    public ShoppingCartActor(PersistenceId persistenceId, TimerScheduler<Command> timers) {
      super(persistenceId);
      this.timers = timers;
    }

    @Override
    public State emptyState() {
      return null;
    }

    @Override
    public CommandHandler<Command, DomainEvent, State> commandHandler() {
      // #command-handler
      CommandHandlerBuilder<Command, DomainEvent, State> builder = newCommandHandlerBuilder();

      builder.forStateType(LookingAround.class).onCommand(AddItem.class, this::addItem);

      builder
          .forStateType(Shopping.class)
          .onCommand(AddItem.class, this::addItem)
          .onCommand(Buy.class, this::buy)
          .onCommand(Leave.class, this::discardShoppingCart)
          .onCommand(Timeout.class, this::timeoutShopping);

      builder
          .forStateType(Inactive.class)
          .onCommand(AddItem.class, this::addItem)
          .onCommand(Timeout.class, () -> Effect().persist(OrderDiscarded.INSTANCE).thenStop());

      builder.forStateType(Paid.class).onCommand(Leave.class, () -> Effect().stop());

      builder.forAnyState().onCommand(GetCurrentCart.class, this::getCurrentCart);
      return builder.build();
    }
    // #command-handler

    private Effect<DomainEvent, State> addItem(AddItem item) {
      return Effect()
          .persist(new ItemAdded(item.item))
          .thenRun(
              () -> timers.startSingleTimer(TIMEOUT_KEY, Timeout.INSTANCE, Duration.ofSeconds(1)));
    }

    private Effect<DomainEvent, State> timeoutShopping() {
      return Effect()
          .persist(CustomerInactive.INSTANCE)
          .thenRun(
              () -> timers.startSingleTimer(TIMEOUT_KEY, Timeout.INSTANCE, Duration.ofSeconds(1)));
    }

    private Effect<DomainEvent, State> buy() {
      return Effect().persist(OrderExecuted.INSTANCE).thenRun(() -> timers.cancel(TIMEOUT_KEY));
    }

    private Effect<DomainEvent, State> discardShoppingCart() {
      return Effect().persist(OrderDiscarded.INSTANCE).thenRun(() -> timers.cancel(TIMEOUT_KEY));
    }

    private Effect<DomainEvent, State> getCurrentCart(State state, GetCurrentCart command) {
      command.replyTo.tell(state.cart);
      return Effect().none();
    }

    // #event-handler
    @Override
    public EventHandler<State, DomainEvent> eventHandler() {
      EventHandlerBuilder<State, DomainEvent> eventHandlerBuilder = newEventHandlerBuilder();

      eventHandlerBuilder
          .forStateType(LookingAround.class)
          .onEvent(ItemAdded.class, item -> new Shopping(new ShoppingCart(item.getItem())));

      eventHandlerBuilder
          .forStateType(Shopping.class)
          .onEvent(
              ItemAdded.class, (state, item) -> new Shopping(state.cart.addItem(item.getItem())))
          .onEvent(OrderExecuted.class, (state, item) -> new Paid(state.cart))
          .onEvent(OrderDiscarded.class, (state, item) -> state) // will be stopped
          .onEvent(CustomerInactive.class, (state, event) -> new Inactive(state.cart));

      eventHandlerBuilder
          .forStateType(Inactive.class)
          .onEvent(
              ItemAdded.class, (state, item) -> new Shopping(state.cart.addItem(item.getItem())))
          .onEvent(OrderDiscarded.class, (state, item) -> state); // will be stopped

      return eventHandlerBuilder.build();
    }
    // #event-handler

    @Override
    public EventAdapter<DomainEvent, ?> eventAdapter() {
      return new PersistentFSMEventAdapter();
    }

    // #signal-handler
    @Override
    public SignalHandler<State> signalHandler() {
      return newSignalHandlerBuilder()
          .onSignal(
              RecoveryCompleted.class,
              (state, signal) -> {
                if (state instanceof Shopping || state instanceof Inactive) {
                  timers.startSingleTimer(TIMEOUT_KEY, Timeout.INSTANCE, Duration.ofSeconds(1));
                }
              })
          .build();
    }
    // #signal-handler

    // #snapshot-adapter
    @Override
    public SnapshotAdapter<State> snapshotAdapter() {
      return PersistentFSMMigration.snapshotAdapter(
          (stateIdentifier, snapshot, timeout) -> {
            ShoppingCart cart = (ShoppingCart) snapshot;
            switch (stateIdentifier) {
              case "Looking Around":
                return new LookingAround(cart);
              case "Shopping":
                return new Shopping(cart);
              case "Inactive":
                return new Inactive(cart);
              case "Paid":
                return new Paid(cart);
              default:
                throw new IllegalStateException("Unexpected state identifier " + stateIdentifier);
            }
          });
    }
    // #snapshot-adapter
  }
}
