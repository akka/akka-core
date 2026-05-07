package sample.killrweather.fog;

import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;

import java.time.Duration;
import java.util.List;

public final class Guardian {

  public static Behavior<Void> create(List<Integer> weatherPorts) {
    return Behaviors.setup(context -> {
      FogSettings settings = FogSettings.create(context.getSystem());

      for (int i = 1; i <= settings.weatherStations; i++) {
        String wsid = Integer.toString(i);
        // choose one of the HTTP API nodes to report to
        int weatherPort = weatherPorts.get(i % weatherPorts.size());

        context.spawn(
            Behaviors.supervise(
                WeatherStation.create(wsid, settings, weatherPort)
            ).onFailure(
                RuntimeException.class,
                SupervisorStrategy.restartWithBackoff(Duration.ofSeconds(1), Duration.ofSeconds(5), 0.5)
            ),
            "weather-station-" + wsid);

      }

      return Behaviors.empty();
    });
  }
}
