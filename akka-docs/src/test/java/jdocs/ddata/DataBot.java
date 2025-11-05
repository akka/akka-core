/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.ddata;

// #data-bot
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.cluster.ddata.DistributedData;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.ORSet;
import akka.cluster.ddata.ORSetKey;
import akka.cluster.ddata.Replicator;
import akka.cluster.ddata.Replicator.Changed;
import akka.cluster.ddata.Replicator.Subscribe;
import akka.cluster.ddata.Replicator.Update;
import akka.cluster.ddata.Replicator.UpdateResponse;
import akka.cluster.ddata.SelfUniqueAddress;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class DataBot extends AbstractActor {

  private static final String TICK = "tick";

  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
  private final SelfUniqueAddress node =
      DistributedData.get(getContext().getSystem()).selfUniqueAddress();

  private final Cancellable tickTask =
      getContext()
          .getSystem()
          .scheduler()
          .scheduleWithFixedDelay(
              Duration.ofSeconds(5),
              Duration.ofSeconds(5),
              getSelf(),
              TICK,
              getContext().getDispatcher(),
              getSelf());

  private final Key<ORSet<String>> dataKey = ORSetKey.create("key");

  @SuppressWarnings("unchecked")
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(String.class, a -> a.equals(TICK), a -> receiveTick())
        .match(
            Changed.class,
            c -> c.key().equals(dataKey),
            c -> receiveChanged((Changed<ORSet<String>>) c))
        .match(UpdateResponse.class, r -> receiveUpdateResponse())
        .build();
  }

  private void receiveTick() {
    String s = String.valueOf((char) ThreadLocalRandom.current().nextInt(97, 123));
    if (ThreadLocalRandom.current().nextBoolean()) {
      // add
      log.info("Adding: {}", s);
      Update<ORSet<String>> update =
          new Update<>(dataKey, ORSet.create(), Replicator.writeLocal(), curr -> curr.add(node, s));
      replicator.tell(update, getSelf());
    } else {
      // remove
      log.info("Removing: {}", s);
      Update<ORSet<String>> update =
          new Update<>(
              dataKey, ORSet.create(), Replicator.writeLocal(), curr -> curr.remove(node, s));
      replicator.tell(update, getSelf());
    }
  }

  private void receiveChanged(Changed<ORSet<String>> c) {
    ORSet<String> data = c.dataValue();
    log.info("Current elements: {}", data.getElements());
  }

  private void receiveUpdateResponse() {
    // ignore
  }

  @Override
  public void preStart() {
    Subscribe<ORSet<String>> subscribe = new Subscribe<>(dataKey, getSelf());
    replicator.tell(subscribe, ActorRef.noSender());
  }

  @Override
  public void postStop() {
    tickTask.cancel();
  }
}
// #data-bot
