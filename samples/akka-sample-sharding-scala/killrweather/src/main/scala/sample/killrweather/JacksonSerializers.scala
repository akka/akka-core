package sample.killrweather

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

// these are needed to serialize Scala style ADT enums with sealed trait and object values with Jackson
// for sending objects between nodes, for the HTTP API See JsonFormats

class DataTypeJsonSerializer extends StdSerializer[WeatherStation.DataType](classOf[WeatherStation.DataType]) {
  override def serialize(value: WeatherStation.DataType, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    val strValue = value match {
      case WeatherStation.DataType.Dewpoint => "d"
      case WeatherStation.DataType.Temperature => "t"
      case WeatherStation.DataType.Pressure => "p"
    }
    gen.writeString(strValue)
  }
}

class DataTypeJsonDeserializer extends StdDeserializer[WeatherStation.DataType](classOf[WeatherStation.DataType]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): WeatherStation.DataType =
    p.getText match {
      case "d" => WeatherStation.DataType.Dewpoint
      case "t" => WeatherStation.DataType.Temperature
      case "p" => WeatherStation.DataType.Pressure
    }
}

class FunctionJsonSerializer extends StdSerializer[WeatherStation.Function](classOf[WeatherStation.Function]) {
  override def serialize(value: WeatherStation.Function, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    val strValue = value match {
      case WeatherStation.Function.Average => "a"
      case WeatherStation.Function.Current => "c"
      case WeatherStation.Function.HighLow => "h"
    }
    gen.writeString(strValue)
  }
}

class FunctionJsonDeserializer extends StdDeserializer[WeatherStation.Function](classOf[WeatherStation.Function]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): WeatherStation.Function =
    p.getText match {
      case "a" => WeatherStation.Function.Average
      case "c" => WeatherStation.Function.Current
      case "h" => WeatherStation.Function.HighLow
    }
}

