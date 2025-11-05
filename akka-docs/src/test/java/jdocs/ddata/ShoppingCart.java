/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.ddata;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.ddata.*;
import akka.cluster.ddata.Replicator.GetFailure;
import akka.cluster.ddata.Replicator.GetResponse;
import akka.cluster.ddata.Replicator.GetSuccess;
import akka.cluster.ddata.Replicator.NotFound;
import akka.cluster.ddata.Replicator.ReadConsistency;
import akka.cluster.ddata.Replicator.ReadMajority;
import akka.cluster.ddata.Replicator.Update;
import akka.cluster.ddata.Replicator.UpdateFailure;
import akka.cluster.ddata.Replicator.UpdateSuccess;
import akka.cluster.ddata.Replicator.UpdateTimeout;
import akka.cluster.ddata.Replicator.WriteConsistency;
import akka.cluster.ddata.Replicator.WriteMajority;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@SuppressWarnings("unchecked")
public class ShoppingCart extends AbstractActor {

  // #read-write-majority
  private final WriteConsistency writeMajority = new WriteMajority(Duration.ofSeconds(3));
  private static final ReadConsistency readMajority = new ReadMajority(Duration.ofSeconds(3));
  // #read-write-majority

  public static final String GET_CART = "getCart";

  public static class AddItem {
    public final LineItem item;

    public AddItem(LineItem item) {
      this.item = item;
    }
  }

  public static class RemoveItem {
    public final String productId;

    public RemoveItem(String productId) {
      this.productId = productId;
    }
  }

  public static class Cart {
    public final Set<LineItem> items;

    public Cart(Set<LineItem> items) {
      this.items = items;
    }
  }

  public static class LineItem implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String productId;
    public final String title;
    public final int quantity;

    public LineItem(String productId, String title, int quantity) {
      this.productId = productId;
      this.title = title;
      this.quantity = quantity;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((productId == null) ? 0 : productId.hashCode());
      result = prime * result + quantity;
      result = prime * result + ((title == null) ? 0 : title.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      LineItem other = (LineItem) obj;
      if (productId == null) {
        if (other.productId != null) return false;
      } else if (!productId.equals(other.productId)) return false;
      if (quantity != other.quantity) return false;
      if (title == null) {
        if (other.title != null) return false;
      } else if (!title.equals(other.title)) return false;
      return true;
    }

    @Override
    public String toString() {
      return "LineItem [productId="
          + productId
          + ", title="
          + title
          + ", quantity="
          + quantity
          + "]";
    }
  }

  public static Props props(String userId) {
    return Props.create(ShoppingCart.class, userId);
  }

  private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
  private final SelfUniqueAddress node =
      DistributedData.get(getContext().getSystem()).selfUniqueAddress();

  @SuppressWarnings("unused")
  private final String userId;

  private final Key<LWWMap<String, LineItem>> dataKey;

  public ShoppingCart(String userId) {
    this.userId = userId;
    this.dataKey = LWWMapKey.create("cart-" + userId);
  }

  @Override
  public Receive createReceive() {
    return matchGetCart().orElse(matchAddItem()).orElse(matchRemoveItem()).orElse(matchOther());
  }

  // #get-cart
  private Receive matchGetCart() {
    return receiveBuilder()
        .matchEquals(GET_CART, s -> receiveGetCart())
        .match(
            GetSuccess.class,
            this::isResponseToGetCart,
            g -> receiveGetSuccess((GetSuccess<LWWMap<String, LineItem>>) g))
        .match(
            NotFound.class,
            this::isResponseToGetCart,
            n -> receiveNotFound((NotFound<LWWMap<String, LineItem>>) n))
        .match(
            GetFailure.class,
            this::isResponseToGetCart,
            f -> receiveGetFailure((GetFailure<LWWMap<String, LineItem>>) f))
        .build();
  }

  private void receiveGetCart() {
    Optional<Object> ctx = Optional.of(getSender());
    replicator.tell(
        new Replicator.Get<LWWMap<String, LineItem>>(dataKey, readMajority, ctx), getSelf());
  }

  private boolean isResponseToGetCart(GetResponse<?> response) {
    return response.key().equals(dataKey)
        && (response.getRequest().orElse(null) instanceof ActorRef);
  }

  private void receiveGetSuccess(GetSuccess<LWWMap<String, LineItem>> g) {
    Set<LineItem> items = new HashSet<>(g.dataValue().getEntries().values());
    ActorRef replyTo = (ActorRef) g.getRequest().get();
    replyTo.tell(new Cart(items), getSelf());
  }

  private void receiveNotFound(NotFound<LWWMap<String, LineItem>> n) {
    ActorRef replyTo = (ActorRef) n.getRequest().get();
    replyTo.tell(new Cart(new HashSet<>()), getSelf());
  }

  private void receiveGetFailure(GetFailure<LWWMap<String, LineItem>> f) {
    // ReadMajority failure, try again with local read
    Optional<Object> ctx = Optional.of(getSender());
    replicator.tell(
        new Replicator.Get<LWWMap<String, LineItem>>(dataKey, Replicator.readLocal(), ctx),
        getSelf());
  }
  // #get-cart

  // #add-item
  private Receive matchAddItem() {
    return receiveBuilder().match(AddItem.class, this::receiveAddItem).build();
  }

  private void receiveAddItem(AddItem add) {
    Update<LWWMap<String, LineItem>> update =
        new Update<>(dataKey, LWWMap.create(), writeMajority, cart -> updateCart(cart, add.item));
    replicator.tell(update, getSelf());
  }

  // #add-item

  private LWWMap<String, LineItem> updateCart(LWWMap<String, LineItem> data, LineItem item) {
    if (data.contains(item.productId)) {
      LineItem existingItem = data.get(item.productId).get();
      int newQuantity = existingItem.quantity + item.quantity;
      LineItem newItem = new LineItem(item.productId, item.title, newQuantity);
      return data.put(node, item.productId, newItem);
    } else {
      return data.put(node, item.productId, item);
    }
  }

  private Receive matchRemoveItem() {
    return receiveBuilder()
        .match(RemoveItem.class, this::receiveRemoveItem)
        .match(
            GetSuccess.class,
            this::isResponseToRemoveItem,
            g -> receiveRemoveItemGetSuccess((GetSuccess<LWWMap<String, LineItem>>) g))
        .match(
            GetFailure.class,
            this::isResponseToRemoveItem,
            f -> receiveRemoveItemGetFailure((GetFailure<LWWMap<String, LineItem>>) f))
        .match(
            NotFound.class,
            this::isResponseToRemoveItem,
            n -> {
              /* nothing to remove */
            })
        .build();
  }

  // #remove-item
  private void receiveRemoveItem(RemoveItem rm) {
    // Try to fetch latest from a majority of nodes first, since ORMap
    // remove must have seen the item to be able to remove it.
    Optional<Object> ctx = Optional.of(rm);
    replicator.tell(
        new Replicator.Get<LWWMap<String, LineItem>>(dataKey, readMajority, ctx), getSelf());
  }

  private void receiveRemoveItemGetSuccess(GetSuccess<LWWMap<String, LineItem>> g) {
    RemoveItem rm = (RemoveItem) g.getRequest().get();
    removeItem(rm.productId);
  }

  private void receiveRemoveItemGetFailure(GetFailure<LWWMap<String, LineItem>> f) {
    // ReadMajority failed, fall back to best effort local value
    RemoveItem rm = (RemoveItem) f.getRequest().get();
    removeItem(rm.productId);
  }

  private void removeItem(String productId) {
    Update<LWWMap<String, LineItem>> update =
        new Update<>(dataKey, LWWMap.create(), writeMajority, cart -> cart.remove(node, productId));
    replicator.tell(update, getSelf());
  }

  private boolean isResponseToRemoveItem(GetResponse<?> response) {
    return response.key().equals(dataKey)
        && (response.getRequest().orElse(null) instanceof RemoveItem);
  }
  // #remove-item

  private Receive matchOther() {
    return receiveBuilder()
        .match(
            UpdateSuccess.class,
            u -> {
              // ok
            })
        .match(
            UpdateTimeout.class,
            t -> {
              // will eventually be replicated
            })
        .match(
            UpdateFailure.class,
            f -> {
              throw new IllegalStateException("Unexpected failure: " + f);
            })
        .build();
  }
}
