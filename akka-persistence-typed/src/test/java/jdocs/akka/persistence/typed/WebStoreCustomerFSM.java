/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.persistence.typed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The model from akka.persistence.fsm.AbstractPersistentFSMTest.WebStoreCustomerFSM brought here
 * for the PersistentFsmToTypedMigrationCompileOnlyTest
 */
public class WebStoreCustomerFSM {
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

  public interface Command {}

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

  public interface DomainEvent extends Serializable {}

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
}
