package sample.killrweather.fog;

import akka.actor.typed.ActorSystem;
import com.typesafe.config.Config;

import java.time.Duration;

public class FogSettings {
  public final int weatherStations;
  public final String host;
  public final Duration sampleInterval;

  public FogSettings(int weatherStations, String host, Duration sampleInterval) {
    this.weatherStations = weatherStations;
    this.host = host;
    this.sampleInterval = sampleInterval;
  }

  public static FogSettings create(ActorSystem<?> system) {
    return create(system.settings().config().getConfig("killrweather.fog"));
  }

  public static FogSettings create(Config config) {
    return new FogSettings(
        config.getInt("initial-weather-stations"),
        config.getString("weather-station.hostname"),
        config.getDuration("weather-station.sample-interval")
    );
  }
}
