package sample.distributeddata;

import static akka.cluster.ddata.typed.javadsl.Replicator.writeLocal;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import akka.actor.Address;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.eventstream.EventStream;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.SelfUniqueAddress;
import akka.cluster.ddata.typed.javadsl.DistributedData;
import akka.cluster.ddata.typed.javadsl.Replicator.Changed;
import akka.cluster.ddata.typed.javadsl.Replicator.SubscribeResponse;
import akka.cluster.ddata.typed.javadsl.Replicator.Update;
import akka.cluster.ddata.typed.javadsl.Replicator.UpdateResponse;
import akka.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Subscribe;

public class ReplicatedMetrics extends AbstractBehavior<ReplicatedMetrics.Command> {

  public interface Command {}

  private enum Tick implements Command {
    INSTANCE
  }

  private enum Cleanup implements Command {
    INSTANCE
  }

  private interface InternalCommand extends Command {}

  private static class InternalSubscribeResponse implements InternalCommand {
    public final SubscribeResponse<LWWMap<String, Long>> rsp;

    private InternalSubscribeResponse(SubscribeResponse<LWWMap<String, Long>> rsp) {
      this.rsp = rsp;
    }
  }

  private static class InternalClusterMemberUp implements InternalCommand {
    public final ClusterEvent.MemberUp msg;

    private InternalClusterMemberUp(ClusterEvent.MemberUp msg) {
      this.msg = msg;
    }
  }

  private static class InternalClusterMemberRemoved implements InternalCommand {
    public final ClusterEvent.MemberRemoved msg;

    private InternalClusterMemberRemoved(ClusterEvent.MemberRemoved msg) {
      this.msg = msg;
    }
  }

  private static class InternalUpdateResponse<A extends ReplicatedData> implements InternalCommand {
    public final UpdateResponse<A> rsp;

    private InternalUpdateResponse(UpdateResponse<A> rsp) {
      this.rsp = rsp;
    }
  }

  public static Behavior<Command> create(Duration measureInterval, Duration cleanupInterval) {
    return Behaviors.setup(context -> Behaviors.withTimers(timers -> {
      timers.startTimerAtFixedRate(Tick.INSTANCE, Tick.INSTANCE, measureInterval);
      timers.startTimerAtFixedRate(Cleanup.INSTANCE, Cleanup.INSTANCE, cleanupInterval);
      return DistributedData.withReplicatorMessageAdapter(
          (ReplicatorMessageAdapter<Command, LWWMap<String, Long>> replicator)->
              new ReplicatedMetrics(context, replicator)
      );
    }));
  }

  public static class UsedHeap {
    public Map<String, Double> percentPerNode;
    public UsedHeap(Map<String, Double> percentPerNode) {
      this.percentPerNode = percentPerNode;
    }
    @Override
    public String toString() {
      return percentPerNode.entrySet().stream().map(entry ->
        entry.getKey() + " --> " + entry.getValue() + " %"
      ).reduce((str1, str2) -> str1 + "\n" + str2).get();
    }
  }

  public static String nodeKey(Address address) {
    return address.host().get() + ":" + address.port().get();
  }

  private final ReplicatorMessageAdapter<Command, LWWMap<String, Long>> replicator;
  private final SelfUniqueAddress node;
  private final Cluster cluster;
  private final String selfNodeKey;
  private final MemoryMXBean memoryMBean = ManagementFactory.getMemoryMXBean();

  private final Key<LWWMap<String, Long>> usedHeapKey = LWWMapKey.create("usedHeap");
  private final Key<LWWMap<String, Long>> maxHeapKey = LWWMapKey.create("maxHeap");

  private Map<String, Long> maxHeap = new HashMap<>();
  private final Set<String> nodesInCluster = new HashSet<>();

  private ReplicatedMetrics(
      ActorContext<ReplicatedMetrics.Command> context,
      ReplicatorMessageAdapter<Command, LWWMap<String, Long>> replicator
  ) {
    super(context);
    this.replicator = replicator;

    node = DistributedData.get(context.getSystem()).selfUniqueAddress();
    cluster = Cluster.get(context.getSystem());
    selfNodeKey = nodeKey(cluster.selfMember().address());

    replicator.subscribe(usedHeapKey, InternalSubscribeResponse::new);
    replicator.subscribe(maxHeapKey, InternalSubscribeResponse::new);

    ActorRef<MemberUp> memberUpRef =
        context.messageAdapter(MemberUp.class, InternalClusterMemberUp::new);
    ActorRef<MemberRemoved> memberRemovedRef =
        context.messageAdapter(MemberRemoved.class, InternalClusterMemberRemoved::new);
    cluster.subscriptions().tell(new Subscribe<>(memberUpRef, MemberUp.class));
    cluster.subscriptions().tell(new Subscribe<>(memberRemovedRef, MemberRemoved.class));
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
      .onMessageEquals(Tick.INSTANCE, this::receiveTick)
      .onMessage(InternalSubscribeResponse.class, this::onInternalSubscribeResponse)
      .onMessage(InternalUpdateResponse.class, notUsed -> this)
      .onMessage(InternalClusterMemberUp.class, rsp -> receiveMemberUp(rsp.msg.member().address()))
      .onMessage(InternalClusterMemberRemoved.class, rsp -> receiveMemberRemoved(rsp.msg.member().address()))
      .onMessageEquals(Cleanup.INSTANCE, this::receiveCleanup)
      .build();
  }

  private Behavior<Command> receiveTick() {
    MemoryUsage heap = memoryMBean.getHeapMemoryUsage();
    long used = heap.getUsed();
    long max = heap.getMax();

    replicator.askUpdate(
        askReplyTo -> new Update<>(usedHeapKey, LWWMap.create(), writeLocal(), askReplyTo,
            curr -> curr.put(node, selfNodeKey, used)),
        InternalUpdateResponse::new);

    replicator.askUpdate(
        askReplyTo -> new Update<>(maxHeapKey, LWWMap.create(), writeLocal(), askReplyTo, curr -> {
          if (curr.contains(selfNodeKey) && curr.get(selfNodeKey).get() == max)
            return curr; // unchanged
          else
            return curr.put(node, selfNodeKey, max);
        }),
        InternalUpdateResponse::new);

    return this;
  }

  private Behavior<Command> onInternalSubscribeResponse(InternalSubscribeResponse rsp) {
    SubscribeResponse<LWWMap<String, Long>> rsp1 = rsp.rsp;
    if (rsp1 instanceof Changed && rsp1.key().equals(maxHeapKey)) {
      receiveMaxHeapChanged((Changed<LWWMap<String, Long>>) rsp1);
    } else if (rsp1 instanceof Changed && rsp1.key().equals(usedHeapKey)) {
      receiveUsedHeapChanged((Changed<LWWMap<String, Long>>) rsp1);
    }
    return this;
  }

  private void receiveMaxHeapChanged(Changed<LWWMap<String, Long>> c) {
    maxHeap = c.dataValue().getEntries();
  }

  private void receiveUsedHeapChanged(Changed<LWWMap<String, Long>> c) {
    Map<String, Double> percentPerNode = new HashMap<>();
    for (Map.Entry<String, Long> entry : c.dataValue().getEntries().entrySet()) {
      if (maxHeap.containsKey(entry.getKey())) {
        double percent = (entry.getValue().doubleValue() / maxHeap.get(entry.getKey())) * 100.0;
        percentPerNode.put(entry.getKey(), percent);
      }
    }
    UsedHeap usedHeap = new UsedHeap(percentPerNode);
    getContext().getSystem().eventStream().tell(new EventStream.Publish<>(usedHeap));
  }

  private Behavior<Command> receiveMemberUp(Address address) {
    nodesInCluster.add(nodeKey(address));
    return this;
  }

  private Behavior<Command> receiveMemberRemoved(Address address) {
    nodesInCluster.remove(nodeKey(address));
    if (address.equals(cluster.selfMember().uniqueAddress().address()))
      return Behaviors.stopped();
    else
      return this;
  }

  private Behavior<Command> receiveCleanup() {
    // note that this function is run in the replicator, so we cannot safely reference
    // mutable internal state of this actor, we therefore make a defensive copy here
    final Set<String> nodesAtCleanup = Collections.unmodifiableSet(new HashSet<>(nodesInCluster));
    Function<LWWMap<String, Long>, LWWMap<String, Long>> cleanupRemoved = (data) -> {
      LWWMap<String, Long> result = data;
      for (String k : data.getEntries().keySet()) {
        if (!nodesAtCleanup.contains(k)) {
          result = result.remove(node, k);
        }
      }
      return result;
    };

    replicator.askUpdate(
        askReplyTo -> new Update<>(usedHeapKey, LWWMap.create(), writeLocal(), askReplyTo, cleanupRemoved),
        InternalUpdateResponse::new);

    replicator.askUpdate(
        askReplyTo -> new Update<>(maxHeapKey, LWWMap.create(), writeLocal(), askReplyTo, cleanupRemoved),
        InternalUpdateResponse::new);

    return this;
  }

}
