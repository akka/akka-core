package sample.distributeddata;

import static akka.cluster.ddata.typed.javadsl.Replicator.readLocal;
import static akka.cluster.ddata.typed.javadsl.Replicator.writeLocal;

import java.util.Optional;
import scala.Option;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.cluster.ddata.SelfUniqueAddress;
import akka.cluster.ddata.typed.javadsl.DistributedData;
import akka.cluster.ddata.typed.javadsl.Replicator.Get;
import akka.cluster.ddata.typed.javadsl.Replicator.GetResponse;
import akka.cluster.ddata.typed.javadsl.Replicator.GetSuccess;
import akka.cluster.ddata.typed.javadsl.Replicator.NotFound;
import akka.cluster.ddata.typed.javadsl.Replicator.Update;
import akka.cluster.ddata.typed.javadsl.Replicator.UpdateResponse;
import akka.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter;

public class ReplicatedCache {

  public interface Command {}

  public static class PutInCache implements Command {
    public final String key;
    public final String value;

    public PutInCache(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetFromCache implements Command {
    public final String key;
    public final ActorRef<Cached> replyTo;

    public GetFromCache(String key, ActorRef<Cached> replyTo) {
      this.key = key;
      this.replyTo = replyTo;
    }
  }

  public static class Cached {
    public final String key;
    public final Optional<String> value;

    public Cached(String key, Optional<String> value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((key == null) ? 0 : key.hashCode());
      result = prime * result + ((value == null) ? 0 : value.hashCode());
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
      Cached other = (Cached) obj;
      if (key == null) {
        if (other.key != null)
          return false;
      } else if (!key.equals(other.key))
        return false;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Cached [key=" + key + ", value=" + value + "]";
    }

  }

  public static class Evict implements Command {
    public final String key;

    public Evict(String key) {
      this.key = key;
    }
  }

  private interface InternalCommand extends Command {}

  private static class InternalGetResponse implements InternalCommand {
    public final String key;
    public final ActorRef<Cached> replyTo;
    public final GetResponse<LWWMap<String, String>> rsp;

    private InternalGetResponse(
        String key, ActorRef<Cached> replyTo, GetResponse<LWWMap<String, String>> rsp
    ) {
      this.key = key;
      this.replyTo = replyTo;
      this.rsp = rsp;
    }
  }

  private static class InternalUpdateResponse implements InternalCommand {
    public final UpdateResponse<LWWMap<String, String>> rsp;

    private InternalUpdateResponse(UpdateResponse<LWWMap<String, String>> rsp) {
      this.rsp = rsp;
    }
  }

  public static Behavior<Command> create() {
    return Behaviors.setup(context ->
        DistributedData.withReplicatorMessageAdapter(
            (ReplicatorMessageAdapter<Command, LWWMap<String, String>> replicator) ->
                new ReplicatedCache(context, replicator).createBehavior()));
  }

  private final ReplicatorMessageAdapter<Command, LWWMap<String, String>> replicator;
  private final SelfUniqueAddress node;

  public ReplicatedCache(
      ActorContext<Command> context,
      ReplicatorMessageAdapter<Command, LWWMap<String, String>> replicator
  ) {
    this.replicator = replicator;
    node = DistributedData.get(context.getSystem()).selfUniqueAddress();
  }

  public Behavior<Command> createBehavior() {
    return Behaviors
        .receive(Command.class)
        .onMessage(PutInCache.class, cmd -> receivePutInCache(cmd.key, cmd.value))
        .onMessage(Evict.class, cmd -> receiveEvict(cmd.key))
        .onMessage(GetFromCache.class, cmd -> receiveGetFromCache(cmd.key, cmd.replyTo))
        .onMessage(InternalGetResponse.class, this::onInternalGetResponse)
        .onMessage(InternalUpdateResponse.class, notUsed -> Behaviors.same())
        .build();
  }

  private Behavior<Command> receivePutInCache(String key, String value) {
    replicator.askUpdate(
        askReplyTo ->
            new Update<>(
                dataKey(key),
                LWWMap.empty(),
                writeLocal(),
                askReplyTo,
                curr -> curr.put(node, key, value)
            ),
        InternalUpdateResponse::new);

    return Behaviors.same();
  }

  private Behavior<Command> receiveEvict(String key) {
    replicator.askUpdate(
        askReplyTo ->
            new Update<>(
                dataKey(key),
                LWWMap.empty(),
                writeLocal(),
                askReplyTo,
                curr -> curr.remove(node, key)
            ),
        InternalUpdateResponse::new);

    return Behaviors.same();
  }

  private Behavior<Command> receiveGetFromCache(String key, ActorRef<Cached> replyTo) {
    replicator.askGet(
        askReplyTo -> new Get<>(dataKey(key), readLocal(), askReplyTo),
        rsp -> new InternalGetResponse(key, replyTo, rsp));

    return Behaviors.same();
  }

  private Behavior<Command> onInternalGetResponse(InternalGetResponse msg) {
    if (msg.rsp instanceof GetSuccess) {
      Option<String> valueOption = ((GetSuccess<LWWMap<String, String>>) msg.rsp).get(dataKey(msg.key)).get(msg.key);
      Optional<String> valueOptional = Optional.ofNullable(valueOption.isDefined() ? valueOption.get() : null);
      msg.replyTo.tell(new Cached(msg.key, valueOptional));
    } else if (msg.rsp instanceof NotFound) {
      msg.replyTo.tell(new Cached(msg.key, Optional.empty()));
    }
    return Behaviors.same();
  }

  private Key<LWWMap<String, String>> dataKey(String entryKey) {
    return LWWMapKey.create("cache-" + Math.abs(entryKey.hashCode() % 100));
  }

}
