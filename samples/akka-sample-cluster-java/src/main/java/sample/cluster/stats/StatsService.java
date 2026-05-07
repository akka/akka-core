package sample.cluster.stats;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.fasterxml.jackson.annotation.JsonCreator;
import sample.cluster.CborSerializable;

import java.util.Arrays;
import java.util.List;

public final class StatsService extends AbstractBehavior<StatsService.Command> {

  public interface Command extends CborSerializable {}
  public final static class ProcessText implements Command {
    public final String text;
    public final ActorRef<Response> replyTo;
    public ProcessText(String text, ActorRef<Response> replyTo) {
      this.text = text;
      this.replyTo = replyTo;
    }
  }
  public enum Stop implements Command {
    INSTANCE
  }

  interface Response extends CborSerializable { }
  public static final class JobResult implements Response {
    public final double meanWordLength;
    @JsonCreator
    public JobResult(double meanWordLength) {
      this.meanWordLength = meanWordLength;
    }
    @Override
    public String toString() {
      return "JobResult{" +
          "meanWordLength=" + meanWordLength +
          '}';
    }
  }
  public static final class JobFailed implements Response {
    public final String reason;
    @JsonCreator
    public JobFailed(String reason) {
      this.reason = reason;
    }
    @Override
    public String toString() {
      return "JobFailed{" +
          "reason='" + reason + '\'' +
          '}';
    }
  }

  public static Behavior<Command> create(ActorRef<StatsWorker.Process> workers) {
    return Behaviors.setup(context ->
        new StatsService(context, workers)
    );
  }

  private final ActorRef<StatsWorker.Process> workers;

  private StatsService(ActorContext<Command> context, ActorRef<StatsWorker.Process> workers) {
    super(context);
    this.workers = workers;
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(ProcessText.class, this::process)
        .onMessageEquals(Stop.INSTANCE, () -> Behaviors.stopped())
        .build();
  }

  private Behavior<Command> process(ProcessText command) {
    getContext().getLog().info("Delegating request");
    List<String> words = Arrays.asList(command.text.split(" "));
    getContext().spawnAnonymous(StatsAggregator.create(words, workers, command.replyTo));
    return this;
  }

}
