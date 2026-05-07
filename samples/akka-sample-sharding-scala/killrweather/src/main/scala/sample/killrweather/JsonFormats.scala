package sample.killrweather

import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonFormat

/**
 * Formats to marshall and unmarshall objects to JSON in the HTTP API, for internode serialization CBOR and Jackson is used
 */
object JsonFormats {

  import spray.json.RootJsonFormat
  // import the default encoders for primitive types (Int, String, Lists etc)
  import spray.json.DefaultJsonProtocol._
  import spray.json.deserializationError

  /**
   * Given a set of possible case objects, create a format that accepts uses their toString representation in json
   */
  private final case class SimpleEnumFormat[T](possibleValues: Set[T]) extends JsonFormat[T] {
    val stringToValue: Map[String, T] = possibleValues.map(value => value.toString.toLowerCase -> value).toMap
    val valueToString: Map[T, String] = stringToValue.map { case (string, value) => value -> string }

    override def read(json: JsValue): T = json match {
      case JsString(text) =>
        stringToValue.get(text.toLowerCase) match {
          case Some(t) => t
          case None    => deserializationError(s"Possible values are ${stringToValue.keySet}, [$text] is not among them")
        }
      case surprise =>
        deserializationError(s"Expected a string value, got $surprise")
    }

    override def write(obj: T): JsValue =
      JsString(valueToString(obj))
  }

  implicit val functionFormat: JsonFormat[WeatherStation.Function] = SimpleEnumFormat(WeatherStation.Function.All)
  implicit val dataTypeFormat: JsonFormat[WeatherStation.DataType] = SimpleEnumFormat(WeatherStation.DataType.All)
  implicit val dataFormat: RootJsonFormat[WeatherStation.Data] = jsonFormat3(WeatherStation.Data.apply)

  implicit val dataIngestedFormat: RootJsonFormat[WeatherStation.DataRecorded] = jsonFormat1(
    WeatherStation.DataRecorded.apply)

  implicit val queryWindowFormat: RootJsonFormat[WeatherStation.TimeWindow] = jsonFormat3(
    WeatherStation.TimeWindow.apply)
  implicit val queryStatusFormat: RootJsonFormat[WeatherStation.QueryResult] = jsonFormat5(
    WeatherStation.QueryResult.apply)

}
