package sample.cluster.stats;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import sample.cluster.CborSerializable;

public final class StatsWorker extends AbstractBehavior<StatsWorker.Command> {

  interface Command extends CborSerializable {}
  public static final class Process implements Command {
    public final String word;
    public final ActorRef<Processed> replyTo;
    public Process(String word, ActorRef<Processed> replyTo) {
      this.word = word;
      this.replyTo = replyTo;
    }
  }
  private enum EvictCache implements Command {
    INSTANCE
  }
  public static final class Processed implements CborSerializable {
    public final String word;
    public final int length;
    public Processed(String word, int length) {
      this.word = word;
      this.length = length;
    }
  }

  private final Map<String, Integer> cache = new HashMap<String, Integer>();

  private StatsWorker(ActorContext<Command> context) {
    super(context);
  }

  public static Behavior<StatsWorker.Command> create() {
    return Behaviors.setup(context ->
        Behaviors.withTimers(timers -> {
          context.getLog().info("Worker starting up");
          timers.startTimerWithFixedDelay(EvictCache.INSTANCE, EvictCache.INSTANCE, Duration.ofSeconds(30));

          return new StatsWorker(context);
        })
    );
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(Process.class, this::process)
        .onMessageEquals(EvictCache.INSTANCE, this::evictCache)
        .build();
  }

  private Behavior<Command> evictCache() {
    cache.clear();
    return this;
  }

  private Behavior<Command> process(Process command) {
    getContext().getLog().info("Worker processing request [{}]", command.word);
    if (!cache.containsKey(command.word)) {
      int length = command.word.length();
      cache.put(command.word, length);
    }
    command.replyTo.tell(new Processed(command.word, cache.get(command.word)));
    return this;
  }
}
