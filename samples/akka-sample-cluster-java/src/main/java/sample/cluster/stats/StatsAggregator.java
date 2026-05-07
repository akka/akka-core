package sample.cluster.stats;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl. ActorContext;
import akka.actor.typed.javadsl.Receive;

public class StatsAggregator extends AbstractBehavior<StatsAggregator.Event> {

  interface Event {}
  private enum Timeout implements Event {
    INSTANCE
  }
  private static class CalculationComplete implements Event {
    public final int length;
    public CalculationComplete(int length) {
      this.length = length;
    }
  }

  public static Behavior<Event> create(List<String> words, ActorRef<StatsWorker.Process> workers, ActorRef<StatsService.Response> replyTo) {
    return Behaviors.setup(context ->
      new StatsAggregator(context, words, workers, replyTo)
    );
  }

  private final int expectedResponses;
  private final ActorRef<StatsService.Response> replyTo;
  private final List<Integer> results = new ArrayList<>();

  private StatsAggregator(ActorContext<Event> context, List<String> words, ActorRef<StatsWorker.Process> workers, ActorRef<StatsService.Response> replyTo) {
    super(context);
    this.expectedResponses = words.size();
    this.replyTo = replyTo;
    getContext().setReceiveTimeout(Duration.ofSeconds(3), Timeout.INSTANCE);

    ActorRef<StatsWorker.Processed> responseAdapter =
        getContext().messageAdapter(StatsWorker.Processed.class, processed -> new CalculationComplete(processed.length));

    words.stream().forEach(word ->
        workers.tell(new StatsWorker.Process(word, responseAdapter))
    );
  }

  @Override
  public Receive<Event> createReceive() {
    return newReceiveBuilder()
        .onMessage(CalculationComplete.class, this::onCalculationComplete)
        .onMessageEquals(Timeout.INSTANCE, this::onTimeout)
        .build();
  }

  private Behavior<Event> onCalculationComplete(CalculationComplete event) {
    results.add(event.length);
    if (results.size() == expectedResponses) {
      int sum = results.stream().mapToInt(i -> i).sum();
      double meanWordLength = ((double) sum) / results.size();
      replyTo.tell(new StatsService.JobResult(meanWordLength));
      return Behaviors.stopped();
    } else {
      return this;
    }
  }

  private Behavior<Event> onTimeout() {
    replyTo.tell(new StatsService.JobFailed("Service unavailable, try again later"));
    return Behaviors.stopped();
  }
}
