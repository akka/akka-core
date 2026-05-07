package sample.cluster.stats;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;

import java.time.Duration;


public final class StatsClient {

  interface Event {}
  private enum Tick implements Event {
    INSTANCE
  }
  private static class ServiceResponse implements Event {
    public final StatsService.Response result;
    public ServiceResponse(StatsService.Response result) {
      this.result = result;
    }
  }

  public static Behavior<Event> create(ActorRef<StatsService.ProcessText> service) {
    return Behaviors.setup(context ->
      Behaviors.withTimers(timers -> {
        timers.startTimerWithFixedDelay(Tick.INSTANCE, Tick.INSTANCE, Duration.ofSeconds(2));
        ActorRef<StatsService.Response> responseAdapter =
            context.messageAdapter(StatsService.Response.class, ServiceResponse::new);

        return Behaviors.receive(Event.class)
            .onMessageEquals(Tick.INSTANCE, () -> {
              context.getLog().info("Sending process request");
              service.tell(new StatsService.ProcessText("this is the text that will be analyzed", responseAdapter));
              return Behaviors.same();
            }).onMessage(ServiceResponse.class, response -> {
              context.getLog().info("Service result: {}", response.result);
              return Behaviors.same();
            }).build();
      })
    );
  }

}
