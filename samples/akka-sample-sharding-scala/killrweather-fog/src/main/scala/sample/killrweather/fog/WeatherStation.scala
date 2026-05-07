package sample.killrweather.fog

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.SystemMaterializer

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Random
import scala.util.Success

/**
  *  How many weather stations there are? Currently:
  *    "well over 10,000 manned and automatic surface weather stations,
  *    1,000 upper-air stations, 7,000 ships, 100 moored and 1,000 drifting buoys,
  *    hundreds of weather radars and 3,000 specially equipped commercial aircraft
  *    measure key parameters of the atmosphere, land and ocean surface every day.
  *    Add to these some 16 meteorological and 50 research satellites to get an idea
  *    of the size of the global network for meteorological, hydrological and other
  *    geophysical observations."
  *  - https://public.wmo.int/en/our-mandate/what-we-do/observations
  */
private[fog] object WeatherStation {

  type WeatherStationId = String

  sealed trait Command
  case object Sample extends Command
  private final case class ProcessSuccess(msg: String) extends Command
  private final case class ProcessFailure(e: Throwable) extends Command

  /** Starts a device and it's task to initiate reading data at a scheduled rate. */
  def apply(wsid: WeatherStationId, settings: FogSettings, httpPort: Int): Behavior[Command] =
    Behaviors.setup(ctx =>
      new WeatherStation(ctx, wsid, settings, httpPort).running
    )
}

/** Starts a device and it's task to initiate reading data at a scheduled rate. */
private class WeatherStation(context: ActorContext[WeatherStation.Command], wsid: WeatherStation.WeatherStationId, settings: FogSettings, httpPort: Int) {
  import WeatherStation._

  private val random = new Random()

  private val http = {
    import akka.actor.typed.scaladsl.adapter._
    Http(context.system.toClassic)
  }
  private val stationUrl = s"http://${settings.host}:${httpPort}/weather/$wsid"

  def running: Behavior[WeatherStation.Command] = {
    context.log.info(s"Started WeatherStation {} of total {} with weather port {}",
      wsid, settings.weatherStations, httpPort)

    Behaviors.setup[WeatherStation.Command] { context =>
      context.log.debug(s"Started {} data sampling.", wsid)

      Behaviors.withTimers { timers =>
        timers.startSingleTimer(Sample, Sample, settings.sampleInterval)

        Behaviors.receiveMessage {
          case Sample =>
            val value = 5 + 30 * random.nextDouble
            val eventTime = System.currentTimeMillis
            context.log.debug("Recording temperature measurement {}", value)
            recordTemperature(eventTime, value)
            Behaviors.same

          case ProcessSuccess(msg) =>
            context.log.debug("Successfully registered data: {}", msg)
            // trigger next sample only after we got a successful response
            timers.startSingleTimer(Sample, Sample, settings.sampleInterval)
            Behaviors.same

          case ProcessFailure(e) =>
            throw new RuntimeException("Failed to register data", e)
        }
      }
    }
  }

  private def recordTemperature(eventTime: Long, temperature: Double): Unit = {
    implicit val ec = context.executionContext
    implicit val materializer = SystemMaterializer(context.system).materializer

    // we could also use a class and a Json formatter like in the server
    // but since this is the only json we send this is a bit more concise
    import spray.json._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    val json = JsObject(
      "eventTime" -> JsNumber(eventTime),
      "dataType" -> JsString("temperature"),
      "value" -> JsNumber(temperature)
    )

    val futureResponseBody: Future[String] = http.singleRequest(Post(stationUrl, json))
      .flatMap (res =>
        Unmarshal(res).to[String].map(body =>
          if (res.status.isSuccess()) body
          else throw new RuntimeException(s"Failed to register data: $body")
        )
      )
    context.pipeToSelf(futureResponseBody) {
      case Success(s) => ProcessSuccess(s)
      case Failure(e) => ProcessFailure(e)
    }

  }

}
