package sample.distributeddata;

import java.time.Duration;
import java.util.HashMap;
import java.math.BigInteger;
import java.util.Map;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.BehaviorBuilder;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.ddata.*;
import akka.cluster.ddata.typed.javadsl.DistributedData;
import akka.cluster.ddata.typed.javadsl.ReplicatorMessageAdapter;

import static akka.cluster.ddata.typed.javadsl.Replicator.*;

public class VotingService {

  public interface Command {}

  public enum Open implements Command {
    INSTANCE
  }

  public enum Close implements Command {
    INSTANCE
  }

  public static class Votes {
    public final Map<String, BigInteger> result;
    public final boolean open;

    public Votes(Map<String, BigInteger> result, boolean open) {
      this.result = result;
      this.open = open;
    }
  }

  public static class Vote implements Command {
    public final String participant;

    public Vote(String participant) {
      this.participant = participant;
    }
  }

  public static class GetVotes implements Command {
    public final ActorRef<Votes> replyTo;

    public GetVotes(ActorRef<Votes> replyTo) {
      this.replyTo = replyTo;
    }
  }

  private interface InternalCommand extends Command {}

  private static class InternalSubscribeResponse implements InternalCommand {
    public final SubscribeResponse<Flag> rsp;

    private InternalSubscribeResponse(SubscribeResponse<Flag> rsp) {
      this.rsp = rsp;
    }
  }

  private static class InternalUpdateResponse<A extends ReplicatedData> implements InternalCommand {
    public final UpdateResponse<A> rsp;

    private InternalUpdateResponse(UpdateResponse<A> rsp) {
      this.rsp = rsp;
    }
  }

  private static class InternalGetResponse implements InternalCommand {
    public final ActorRef<Votes> replyTo;
    public final GetResponse<PNCounterMap<String>> rsp;

    private InternalGetResponse(ActorRef<Votes> replyTo, GetResponse<PNCounterMap<String>> rsp) {
      this.replyTo = replyTo;
      this.rsp = rsp;
    }
  }

  private final ReplicatorMessageAdapter<Command, Flag> replicatorFlag;
  private final ReplicatorMessageAdapter<Command, PNCounterMap<String>> replicatorCounters;
  private final SelfUniqueAddress node;

  private final Key<Flag> openedKey = FlagKey.create("contestOpened");
  private final Key<Flag> closedKey = FlagKey.create("contestClosed");
  private final Key<PNCounterMap<String>> countersKey = PNCounterMapKey.create("contestCounters");
  private final WriteConsistency writeAll = new WriteAll(Duration.ofSeconds(5));
  private final ReadConsistency readAll = new ReadAll(Duration.ofSeconds(3));

  public static Behavior<Command> create() {
    return Behaviors.setup(context ->
      DistributedData.withReplicatorMessageAdapter(
        (ReplicatorMessageAdapter<Command, Flag> replicatorFlag) ->
          DistributedData.withReplicatorMessageAdapter(
            (ReplicatorMessageAdapter<Command, PNCounterMap<String>> replicatorCounters) ->
              new VotingService(context, replicatorFlag, replicatorCounters).createBehavior()
          )
      )
    );
  }

  private VotingService(
      ActorContext<Command> context,
      ReplicatorMessageAdapter<Command, Flag> replicatorFlag,
      ReplicatorMessageAdapter<Command, PNCounterMap<String>> replicatorCounters
  ) {
    this.replicatorFlag = replicatorFlag;
    this.replicatorCounters = replicatorCounters;

    node = DistributedData.get(context.getSystem()).selfUniqueAddress();

    replicatorFlag.subscribe(openedKey, InternalSubscribeResponse::new);
  }

  public Behavior<Command> createBehavior() {
    return Behaviors
      .receive(Command.class)
      .onMessageEquals(Open.INSTANCE, this::receiveOpen)
      .onMessage(InternalSubscribeResponse.class, this::onInternalSubscribeResponse)
      .onMessage(GetVotes.class, this::receiveGetVotesEmpty)
      .build();
  }

  private Behavior<Command> receiveOpen() {
    replicatorFlag.askUpdate(
        askReplyTo -> new Update<>(openedKey, Flag.create(), writeAll, askReplyTo, Flag::switchOn),
        InternalUpdateResponse::new);
    return becomeOpen();
  }

  private Behavior<Command> becomeOpen() {
    replicatorFlag.unsubscribe(openedKey);
    replicatorFlag.subscribe(closedKey, InternalSubscribeResponse::new);
    return matchGetVotesImpl(true, matchOpen());
  }

  private Behavior<Command> receiveGetVotesEmpty(GetVotes getVotes) {
    getVotes.replyTo.tell(new Votes(new HashMap<>(), false));
    return Behaviors.same();
  }

  private BehaviorBuilder<Command> matchOpen() {
    return Behaviors
      .receive(Command.class)
      .onMessage(Vote.class, this::receiveVote)
      .onMessage(InternalUpdateResponse.class, notUsed -> Behaviors.same()) // ok
      .onMessageEquals(Close.INSTANCE, this::receiveClose)
      .onMessage(InternalSubscribeResponse.class, this::onInternalSubscribeResponse);
  }

  private Behavior<Command> receiveVote(Vote vote) {
    replicatorCounters.askUpdate(
        askReplyTo -> new Update<>(countersKey, PNCounterMap.create(), writeLocal(), askReplyTo,
            curr -> curr.increment(node, vote.participant, 1)),
        InternalUpdateResponse::new);

    return Behaviors.same();
  }

  private Behavior<Command> receiveClose() {
    replicatorFlag.askUpdate(
        askReplyTo -> new Update<>(closedKey, Flag.create(), writeAll, askReplyTo, Flag::switchOn),
        InternalUpdateResponse::new);

    return matchGetVotes(false);
  }

  private Behavior<Command> onInternalSubscribeResponse(InternalSubscribeResponse rsp) {
    if (rsp.rsp instanceof Changed && rsp.rsp.key().equals(openedKey)) {
      if (((Changed<Flag>) rsp.rsp).dataValue().enabled()) {
        return becomeOpen();
      }
    } else if (rsp.rsp instanceof Changed && rsp.rsp.key().equals(closedKey)) {
      if (((Changed<Flag>) rsp.rsp).dataValue().enabled()) {
        return matchGetVotes(false);
      }
    }
    return Behaviors.same();
  }

  private Behavior<Command> matchGetVotes(boolean open) {
    return matchGetVotesImpl(open, Behaviors.receive(Command.class));
  }

  private Behavior<Command> matchGetVotesImpl(boolean open, BehaviorBuilder<Command> receive) {
    return receive
      .onMessage(GetVotes.class, this::receiveGetVotes)
      .onMessage(InternalGetResponse.class, rsp -> onInternalGetResponse(open, rsp))
      .onMessage(InternalUpdateResponse.class, notUsed -> Behaviors.same())
      .build();
  }

  private Behavior<Command> receiveGetVotes(GetVotes getVotes) {
    replicatorCounters.askGet(
        askReplyTo -> new Get<>(countersKey, readAll, askReplyTo),
        rsp -> new InternalGetResponse(getVotes.replyTo, rsp)
    );
    return Behaviors.same();
  }

  private Behavior<Command> onInternalGetResponse(boolean open, InternalGetResponse rsp) {
    if (rsp.rsp instanceof GetSuccess && rsp.rsp.key().equals(countersKey)) {
      GetSuccess<PNCounterMap<String>> rsp1 = (GetSuccess<PNCounterMap<String>>) rsp.rsp;
      Map<String, BigInteger> result = rsp1.dataValue().getEntries();
      rsp.replyTo.tell(new Votes(result, open));
    } else if (rsp.rsp instanceof NotFound && rsp.rsp.key().equals(countersKey)) {
      rsp.replyTo.tell(new Votes(new HashMap<>(), open));
    } else if (rsp.rsp instanceof GetFailure && rsp.rsp.key().equals(countersKey)) {
      // skip
    }
    return Behaviors.same();
  }
}
