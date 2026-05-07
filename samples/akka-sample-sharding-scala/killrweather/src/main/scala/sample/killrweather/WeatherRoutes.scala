package sample.killrweather

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.util.Timeout

/**
 * HTTP API for
 * 1. Receiving data from remote weather stations
 * 2. Receiving and responding to queries
 */
private[killrweather] final class WeatherRoutes(system: ActorSystem[_]) {

  private val sharding = ClusterSharding(system)

  // timeout used for asking the actor
  private implicit val timeout: Timeout =
    system.settings.config.getDuration("killrweather.routes.ask-timeout").toMillis.millis

  private def recordData(wsid: Long, data: WeatherStation.Data): Future[WeatherStation.DataRecorded] = {
    val ref = sharding.entityRefFor(WeatherStation.TypeKey, wsid.toString)
    ref.ask(WeatherStation.Record(data, System.currentTimeMillis, _))
  }

  private def query(
      wsid: Long,
      dataType: WeatherStation.DataType,
      function: WeatherStation.Function): Future[WeatherStation.QueryResult] = {
    val ref = sharding.entityRefFor(WeatherStation.TypeKey, wsid.toString)
    ref.ask(WeatherStation.Query(dataType, function, _))
  }

  // unmarshallers for the query parameters
  private val funcsFromName =
    WeatherStation.Function.All.map(function => function.toString.toLowerCase -> function).toMap
  private implicit val functionTypeUnmarshaller: Unmarshaller[String, WeatherStation.Function] =
    Unmarshaller.strict[String, WeatherStation.Function](text => funcsFromName(text.toLowerCase))

  private val dataTypesFromNames =
    WeatherStation.DataType.All.map(dataType => dataType.toString.toLowerCase -> dataType).toMap
  private implicit val dataTypeUnmarshaller: Unmarshaller[String, WeatherStation.DataType] =
    Unmarshaller.strict[String, WeatherStation.DataType](text => dataTypesFromNames(text.toLowerCase))

  // imports needed for the routes and entity json marshalling
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import akka.http.scaladsl.server.Directives._
  import JsonFormats._

  val weather: Route =
    path("weather" / LongNumber) { wsid =>
      concat(
        get {
          parameters("type".as[WeatherStation.DataType], "function".as[WeatherStation.Function]) {
            (dataType, function) =>
              complete(query(wsid, dataType, function))
          }
        },
        post {
          entity(as[WeatherStation.Data]) { data =>
            onSuccess(recordData(wsid, data)) { performed =>
              complete(StatusCodes.Accepted -> s"$performed from event time: ${data.eventTime}")
            }
          }
        })
    }

}
