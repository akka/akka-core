/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.ddata.typed.javadsl;

// #sample
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.ddata.GCounter;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.SelfUniqueAddress;
import akka.cluster.ddata.typed.javadsl.DistributedData;
import akka.cluster.ddata.typed.javadsl.Replicator;
import akka.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter;

// #sample

interface ReplicatorDocSample {
  // #sample
  public class Counter extends AbstractBehavior<Counter.Command> {
    interface Command {}

    enum Increment implements Command {
      INSTANCE
    }

    public static class GetValue implements Command {
      public final ActorRef<Integer> replyTo;

      public GetValue(ActorRef<Integer> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public static class GetCachedValue implements Command {
      public final ActorRef<Integer> replyTo;

      public GetCachedValue(ActorRef<Integer> replyTo) {
        this.replyTo = replyTo;
      }
    }

    enum Unsubscribe implements Command {
      INSTANCE
    }

    private interface InternalCommand extends Command {}

    private static class InternalUpdateResponse implements InternalCommand {
      final Replicator.UpdateResponse<GCounter> rsp;

      InternalUpdateResponse(Replicator.UpdateResponse<GCounter> rsp) {
        this.rsp = rsp;
      }
    }

    private static class InternalGetResponse implements InternalCommand {
      final Replicator.GetResponse<GCounter> rsp;
      final ActorRef<Integer> replyTo;

      InternalGetResponse(Replicator.GetResponse<GCounter> rsp, ActorRef<Integer> replyTo) {
        this.rsp = rsp;
        this.replyTo = replyTo;
      }
    }

    private static final class InternalSubscribeResponse implements InternalCommand {
      final Replicator.SubscribeResponse<GCounter> rsp;

      InternalSubscribeResponse(Replicator.SubscribeResponse<GCounter> rsp) {
        this.rsp = rsp;
      }
    }

    public static Behavior<Command> create(Key<GCounter> key) {
      return Behaviors.setup(
          ctx ->
              DistributedData.withReplicatorMessageAdapter(
                  (ReplicatorMessageAdapter<Command, GCounter> replicatorAdapter) ->
                      new Counter(ctx, replicatorAdapter, key)));
    }

    // adapter that turns the response messages from the replicator into our own protocol
    private final ReplicatorMessageAdapter<Command, GCounter> replicatorAdapter;
    private final SelfUniqueAddress node;
    private final Key<GCounter> key;

    private int cachedValue = 0;

    private Counter(
        ActorContext<Command> context,
        ReplicatorMessageAdapter<Command, GCounter> replicatorAdapter,
        Key<GCounter> key) {
      super(context);

      this.replicatorAdapter = replicatorAdapter;
      this.key = key;

      // #selfUniqueAddress
      final SelfUniqueAddress node = DistributedData.get(context.getSystem()).selfUniqueAddress();
      // #selfUniqueAddress

      this.node = DistributedData.get(context.getSystem()).selfUniqueAddress();

      // #subscribe
      this.replicatorAdapter.subscribe(this.key, InternalSubscribeResponse::new);
      // #subscribe
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Increment.class, this::onIncrement)
          .onMessage(InternalUpdateResponse.class, msg -> Behaviors.same())
          .onMessage(GetValue.class, this::onGetValue)
          .onMessage(GetCachedValue.class, this::onGetCachedValue)
          .onMessage(Unsubscribe.class, this::onUnsubscribe)
          .onMessage(InternalGetResponse.class, this::onInternalGetResponse)
          .onMessage(InternalSubscribeResponse.class, this::onInternalSubscribeResponse)
          .build();
    }

    private Behavior<Command> onIncrement(Increment cmd) {
      replicatorAdapter.askUpdate(
          askReplyTo ->
              new Replicator.Update<>(
                  key,
                  GCounter.empty(),
                  Replicator.writeLocal(),
                  askReplyTo,
                  curr -> curr.increment(node, 1)),
          InternalUpdateResponse::new);

      return this;
    }

    private Behavior<Command> onGetValue(GetValue cmd) {
      replicatorAdapter.askGet(
          askReplyTo -> new Replicator.Get<>(key, Replicator.readLocal(), askReplyTo),
          rsp -> new InternalGetResponse(rsp, cmd.replyTo));

      return this;
    }

    private Behavior<Command> onGetCachedValue(GetCachedValue cmd) {
      cmd.replyTo.tell(cachedValue);
      return this;
    }

    private Behavior<Command> onUnsubscribe(Unsubscribe cmd) {
      replicatorAdapter.unsubscribe(key);
      return this;
    }

    private Behavior<Command> onInternalGetResponse(InternalGetResponse msg) {
      if (msg.rsp instanceof Replicator.GetSuccess) {
        int value = ((Replicator.GetSuccess<?>) msg.rsp).get(key).getValue().intValue();
        msg.replyTo.tell(value);
        return this;
      } else {
        // not dealing with failures
        return Behaviors.unhandled();
      }
    }

    private Behavior<Command> onInternalSubscribeResponse(InternalSubscribeResponse msg) {
      if (msg.rsp instanceof Replicator.Changed) {
        GCounter counter = ((Replicator.Changed<?>) msg.rsp).get(key);
        cachedValue = counter.getValue().intValue();
        return this;
      } else {
        // no deletes
        return Behaviors.unhandled();
      }
    }
  }
}
// #sample
