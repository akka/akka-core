package sample.killrweather.fog

import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import com.typesafe.config.Config

import scala.concurrent.duration._

/**
  * In another terminal start the `Fog` (see Fog computing https://en.wikipedia.org/wiki/Fog_computing).
  * Starts the fog network, simulating devices and stations.
  * In the wild, each station would run its own system and be location-aware.
  */
object Fog {

  def main(args: Array[String]): Unit = {
    val weatherApiPorts = if (args.isEmpty) Seq(12553, 12554) else args.map(_.toInt).toSeq
    ActorSystem[Nothing](Guardian(weatherApiPorts), "Fog")
  }
}

object Guardian {

  def apply(weatherPorts: Seq[Int]): Behavior[Nothing] = {
    Behaviors.setup[Nothing] { context =>
      val settings = FogSettings(context.system)

      (1 until settings.weatherStations).foreach { n =>
        val wsid = n.toString
        // choose one of the HTTP API nodes to report to
        val weatherPort = weatherPorts(n % weatherPorts.size)

        context.spawn(
          Behaviors.supervise(
            WeatherStation(wsid, settings, weatherPort)
          ).onFailure[RuntimeException](SupervisorStrategy.restartWithBackoff(1.second, 5.seconds, 0.5)),
            s"weather-station-$wsid")
      }
      Behaviors.empty
    }
  }
}

object FogSettings {

  def apply(system: ActorSystem[_]): FogSettings = {
    apply(system.settings.config.getConfig("killrweather.fog"))
  }

  def apply(config: Config): FogSettings = {
    import akka.util.Helpers.Requiring

    val millis = (durationKey: String) =>
      config.getDuration(durationKey).toMillis.millis
        .requiring(_ > Duration.Zero, s"'$durationKey' must be > 0")

    FogSettings(
      weatherStations =  config.getInt("initial-weather-stations"),
      host = config.getString("weather-station.hostname"),
      sampleInterval = millis("weather-station.sample-interval")
    )
  }
}

final case class FogSettings(
    weatherStations: Int,
    host: String,
    sampleInterval: FiniteDuration)
