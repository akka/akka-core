/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.fsm;

import akka.actor.*;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Deprecated
public class AbstractPersistentFSMTest {
  // tests have been removed because of flaky test failures, see PR
  // https://github.com/akka/akka/pull/31128

  public static class WebStoreCustomerFSM
      extends AbstractPersistentFSM<
          WebStoreCustomerFSM.UserState,
          WebStoreCustomerFSM.ShoppingCart,
          WebStoreCustomerFSM.DomainEvent> {

    // State name
    // #customer-states
    enum UserState implements PersistentFSM.FSMState {
      LOOKING_AROUND("Looking Around"),
      SHOPPING("Shopping"),
      INACTIVE("Inactive"),
      PAID("Paid");

      private final String stateIdentifier;

      UserState(String stateIdentifier) {
        this.stateIdentifier = stateIdentifier;
      }

      @Override
      public String identifier() {
        return stateIdentifier;
      }
    }
    // #customer-states

    // #customer-states-data
    public static class ShoppingCart {
      private final List<Item> items = new ArrayList<>();

      public ShoppingCart(Item initialItem) {
        items.add(initialItem);
      }

      public ShoppingCart() {}

      public List<Item> getItems() {
        return Collections.unmodifiableList(items);
      }

      public ShoppingCart addItem(Item item) {
        items.add(item);
        return this;
      }

      public void empty() {
        items.clear();
      }
    }

    public static class Item implements Serializable {
      private final String id;
      private final String name;
      private final float price;

      Item(String id, String name, float price) {
        this.id = id;
        this.name = name;
        this.price = price;
      }

      public String getId() {
        return id;
      }

      public float getPrice() {
        return price;
      }

      public String getName() {
        return name;
      }

      @Override
      public String toString() {
        return String.format("Item{id=%s, name=%s, price=%s}", id, price, name);
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Item item = (Item) o;

        return item.price == price && id.equals(item.id) && name.equals(item.name);
      }
    }
    // #customer-states-data

    public interface Command {}

    // #customer-commands
    public static final class AddItem implements Command {
      private final Item item;

      public AddItem(Item item) {
        this.item = item;
      }

      public Item getItem() {
        return item;
      }
    }

    public enum Buy implements Command {
      INSTANCE
    }

    public enum Leave implements Command {
      INSTANCE
    }

    public enum GetCurrentCart implements Command {
      INSTANCE
    }
    // #customer-commands

    public interface DomainEvent extends Serializable {}

    // #customer-domain-events
    public static final class ItemAdded implements DomainEvent {
      private final Item item;

      public ItemAdded(Item item) {
        this.item = item;
      }

      public Item getItem() {
        return item;
      }
    }

    public enum OrderExecuted implements DomainEvent {
      INSTANCE
    }

    public enum OrderDiscarded implements DomainEvent {
      INSTANCE
    }
    // #customer-domain-events

    public enum CustomerInactive implements DomainEvent {
      INSTANCE
    }

    // Side effects - report events to be sent to some "Report Actor"
    public interface ReportEvent {}

    public static final class PurchaseWasMade implements ReportEvent {
      private final List<Item> items;

      public PurchaseWasMade(List<Item> items) {
        this.items = Collections.unmodifiableList(items);
      }

      public List<Item> getItems() {
        return items;
      }
    }

    public enum ShoppingCardDiscarded implements ReportEvent {
      INSTANCE
    }

    private final String persistenceId;

    @Override
    public Class<DomainEvent> domainEventClass() {
      return DomainEvent.class;
    }

    @Override
    public String persistenceId() {
      return persistenceId;
    }

    public static Props props(String persistenceId, ActorRef reportActor) {
      return Props.create(WebStoreCustomerFSM.class, persistenceId, reportActor);
    }

    public WebStoreCustomerFSM(String persistenceId, ActorRef reportActor) {
      this.persistenceId = persistenceId;

      // #customer-fsm-body
      startWith(UserState.LOOKING_AROUND, new ShoppingCart());

      when(
          UserState.LOOKING_AROUND,
          matchEvent(
                  AddItem.class,
                  (event, data) ->
                      goTo(UserState.SHOPPING)
                          .applying(new ItemAdded(event.getItem()))
                          .forMax(Duration.ofSeconds(1)))
              .event(GetCurrentCart.class, (event, data) -> stay().replying(data)));

      when(
          UserState.SHOPPING,
          matchEvent(
                  AddItem.class,
                  (event, data) ->
                      stay().applying(new ItemAdded(event.getItem())).forMax(Duration.ofSeconds(1)))
              .event(
                  Buy.class,
                  // #customer-andthen-example
                  (event, data) ->
                      goTo(UserState.PAID)
                          .applying(OrderExecuted.INSTANCE)
                          .andThen(
                              exec(
                                  cart -> {
                                    reportActor.tell(new PurchaseWasMade(cart.getItems()), self());
                                    // #customer-andthen-example
                                    saveStateSnapshot();
                                    // #customer-andthen-example
                                  })))
              // #customer-andthen-example
              .event(
                  Leave.class,
                  // #customer-snapshot-example
                  (event, data) ->
                      stop()
                          .applying(OrderDiscarded.INSTANCE)
                          .andThen(
                              exec(
                                  cart -> {
                                    reportActor.tell(ShoppingCardDiscarded.INSTANCE, self());
                                    saveStateSnapshot();
                                  })))
              // #customer-snapshot-example
              .event(GetCurrentCart.class, (event, data) -> stay().replying(data))
              .event(
                  StateTimeout$.class,
                  (event, data) -> goTo(UserState.INACTIVE).forMax(Duration.ofSeconds(2))));

      when(
          UserState.INACTIVE,
          matchEvent(
                  AddItem.class,
                  (event, data) ->
                      goTo(UserState.SHOPPING)
                          .applying(new ItemAdded(event.getItem()))
                          .forMax(Duration.ofSeconds(1)))
              .event(GetCurrentCart.class, (event, data) -> stay().replying(data))
              .event(
                  StateTimeout$.class,
                  (event, data) ->
                      stop()
                          .applying(OrderDiscarded.INSTANCE)
                          .andThen(
                              exec(
                                  cart ->
                                      reportActor.tell(ShoppingCardDiscarded.INSTANCE, self())))));

      when(
          UserState.PAID,
          matchEvent(Leave.class, (event, data) -> stop())
              .event(GetCurrentCart.class, (event, data) -> stay().replying(data)));
      // #customer-fsm-body
    }

    /**
     * Override this handler to define the action on Domain Event during recovery
     *
     * @param event domain event to apply
     * @param currentData state data of the previous state
     */
    // #customer-apply-event
    @Override
    public ShoppingCart applyEvent(DomainEvent event, ShoppingCart currentData) {
      if (event instanceof ItemAdded) {
        currentData.addItem(((ItemAdded) event).getItem());
        return currentData;
      } else if (event instanceof OrderExecuted) {
        return currentData;
      } else if (event instanceof OrderDiscarded) {
        currentData.empty();
        return currentData;
      }
      throw new RuntimeException("Unhandled");
    }
    // #customer-apply-event
  }

  enum PFSMState implements PersistentFSM.FSMState {
    STARTED;

    @Override
    public String identifier() {
      return this.name();
    }
  }

  public static class PFSMwithLog extends AbstractPersistentFSM<PFSMState, Integer, Integer> {
    {
      startWith(PFSMState.STARTED, 0);

      when(
          PFSMState.STARTED,
          matchEvent(
              String.class,
              (command, currentData) -> {
                sender().tell("started", getSelf());
                return stay();
              }));

      onTransition(this::transitionLogger); // this is tested command
    }

    private void transitionLogger(PFSMState from, PFSMState to) {
      System.out.println("transition from " + from + " to " + to);
    }

    @Override
    public Class<Integer> domainEventClass() {
      return Integer.class;
    }

    @Override
    public Integer applyEvent(final Integer domainEvent, final Integer currentData) {
      return 0;
    }

    @Override
    public String persistenceId() {
      return "id";
    }
  }
}
