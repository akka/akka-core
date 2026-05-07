package sample.distributeddata;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.SelfUniqueAddress;
import akka.cluster.ddata.typed.javadsl.DistributedData;
import akka.cluster.ddata.typed.javadsl.Replicator.GetFailure;
import akka.cluster.ddata.typed.javadsl.Replicator.GetResponse;
import akka.cluster.ddata.typed.javadsl.Replicator.GetSuccess;
import akka.cluster.ddata.typed.javadsl.Replicator.NotFound;
import akka.cluster.ddata.typed.javadsl.Replicator.ReadConsistency;
import akka.cluster.ddata.typed.javadsl.Replicator.ReadMajority;
import akka.cluster.ddata.typed.javadsl.Replicator.Update;
import akka.cluster.ddata.typed.javadsl.Replicator.UpdateFailure;
import akka.cluster.ddata.typed.javadsl.Replicator.UpdateResponse;
import akka.cluster.ddata.typed.javadsl.Replicator.UpdateSuccess;
import akka.cluster.ddata.typed.javadsl.Replicator.UpdateTimeout;
import akka.cluster.ddata.typed.javadsl.Replicator.WriteConsistency;
import akka.cluster.ddata.typed.javadsl.Replicator.WriteMajority;
import akka.cluster.ddata.typed.javadsl.Replicator.Get;
import akka.cluster.ddata.typed.javadsl.Replicator;
import akka.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter;

public class ShoppingCart {

  //#read-write-majority
  private final WriteConsistency writeMajority = 
      new WriteMajority(Duration.ofSeconds(3));
  private final static ReadConsistency readMajority = 
      new ReadMajority(Duration.ofSeconds(3));
  //#read-write-majority

  public interface Command {}

  public static final class GetCart implements Command {
    public final ActorRef<Cart> replyTo;

    public GetCart(ActorRef<Cart> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class AddItem implements Command {
    public final LineItem item;

    public AddItem(LineItem item) {
      this.item = item;
    }
  }

  public static class RemoveItem implements Command {
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

  public static class LineItem {
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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      LineItem other = (LineItem) obj;
      if (productId == null) {
        if (other.productId != null)
          return false;
      } else if (!productId.equals(other.productId))
        return false;
      if (quantity != other.quantity)
        return false;
      if (title == null) {
        if (other.title != null)
          return false;
      } else if (!title.equals(other.title))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "LineItem [productId=" + productId + ", title=" + title + ", quantity=" + quantity + "]";
    }

  }

  private interface InternalCommand extends Command {}

  private static class InternalGetResponse implements InternalCommand {
    public final GetResponse<LWWMap<String, LineItem>> rsp;
    public final ActorRef<Cart> replyTo;

    private InternalGetResponse(GetResponse<LWWMap<String, LineItem>> rsp, ActorRef<Cart> replyTo) {
      this.rsp = rsp;
      this.replyTo = replyTo;
    }
  }

  private static class InternalUpdateResponse<A extends ReplicatedData> implements InternalCommand {
    public final UpdateResponse<A> rsp;

    private InternalUpdateResponse(UpdateResponse<A> rsp) {
      this.rsp = rsp;
    }
  }

  private static class InternalRemoveItem implements InternalCommand {
    public final String productId;
    public final GetResponse<LWWMap<String, LineItem>> rsp;

    private InternalRemoveItem(String productId, GetResponse<LWWMap<String, LineItem>> rsp) {
      this.productId = productId;
      this.rsp = rsp;
    }
  }

  public static Behavior<Command> create(String userId) {
    return Behaviors.setup(context ->
        DistributedData.withReplicatorMessageAdapter(
            (ReplicatorMessageAdapter<Command, LWWMap<String, LineItem>> replicator) ->
                new ShoppingCart(context, replicator, userId).createBehavior()));
  }

  private final ReplicatorMessageAdapter<Command, LWWMap<String, LineItem>> replicator;
  private final Key<LWWMap<String, LineItem>> dataKey;
  private final SelfUniqueAddress node;

  public ShoppingCart(
      ActorContext<Command> context,
      ReplicatorMessageAdapter<Command, LWWMap<String, LineItem>> replicator,
      String userId
  ) {
    this.replicator = replicator;
    this.dataKey = LWWMapKey.create("cart-" + userId);
    node = DistributedData.get(context.getSystem()).selfUniqueAddress();
  }

  public Behavior<Command> createBehavior() {
    return Behaviors
        .receive(Command.class)
        .onMessage(GetCart.class, this::onGetCart)
        .onMessage(InternalGetResponse.class, this::onInternalGetResponse)
        .onMessage(AddItem.class, this::onAddItem)
        .onMessage(RemoveItem.class, this::onRemoveItem)
        .onMessage(InternalRemoveItem.class, this::onInternalRemoveItem)
        .onMessage(InternalUpdateResponse.class, this::onInternalUpdateResponse)
        .build();
  }

  //#get-cart
  private Behavior<Command> onGetCart(GetCart command) {
    replicator.askGet(
        askReplyTo -> new Get<>(dataKey, readMajority, askReplyTo),
        rsp -> new InternalGetResponse(rsp, command.replyTo));

    return Behaviors.same();
  }

  private Behavior<Command> onInternalGetResponse(InternalGetResponse msg) {
    if (msg.rsp instanceof GetSuccess) {
      LWWMap<String, LineItem> data = ((GetSuccess<LWWMap<String, LineItem>>) msg.rsp).get(dataKey);
      msg.replyTo.tell(new Cart(new HashSet<>(data.getEntries().values())));
    } else if (msg.rsp instanceof NotFound) {
      msg.replyTo.tell(new Cart(new HashSet<>()));
    } else if (msg.rsp instanceof GetFailure) {
      // ReadMajority failure, try again with local read
      replicator.askGet(
          askReplyTo -> new Get<>(dataKey, Replicator.readLocal(), askReplyTo),
          rsp -> new InternalGetResponse(rsp, msg.replyTo)
      );
    }
    return Behaviors.same();
  }
  //#get-cart

  //#add-item
  private Behavior<Command> onAddItem(AddItem command) {
    replicator.askUpdate(
        askReplyTo ->
            new Update<>(
                dataKey,
                LWWMap.empty(),
                writeMajority,
                askReplyTo,
                cart -> updateCart(cart, command.item)
            ),
        InternalUpdateResponse::new);

    return Behaviors.same();
  }

  //#add-item

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

  //#remove-item
  private Behavior<Command> onRemoveItem(RemoveItem command) {
    // Try to fetch latest from a majority of nodes first, since ORMap
    // remove must have seen the item to be able to remove it.
    replicator.askGet(
        askReplyTo -> new Get<>(dataKey, readMajority, askReplyTo),
        rsp -> new InternalRemoveItem(command.productId, rsp));

    return Behaviors.same();
  }

  private Behavior<Command> onInternalRemoveItem(InternalRemoveItem msg) {
    if (msg.rsp instanceof GetSuccess) {
      removeItem(msg.productId);
    } else if (msg.rsp instanceof NotFound) {
      /* nothing to remove */
    } else if (msg.rsp instanceof GetFailure) {
      // ReadMajority failed, fall back to best effort local value
      removeItem(msg.productId);
    }
    return Behaviors.same();
  }

  private void removeItem(String productId) {
    replicator.askUpdate(
        askReplyTo ->
            new Update<>(
                dataKey,
                LWWMap.empty(),
                writeMajority,
                askReplyTo,
                cart -> cart.remove(node, productId)
            ),
        InternalUpdateResponse::new);
  }
  //#remove-item

  private Behavior<Command> onInternalUpdateResponse(InternalUpdateResponse<?> msg) {
    if (msg.rsp instanceof UpdateSuccess) {
      // ok
    } else if (msg.rsp instanceof UpdateTimeout) {
      // will eventually be replicated
    } else if (msg.rsp instanceof UpdateFailure) {
      throw new IllegalStateException("Unexpected failure: " + msg.rsp);
    }
    return Behaviors.same();
  }
}
