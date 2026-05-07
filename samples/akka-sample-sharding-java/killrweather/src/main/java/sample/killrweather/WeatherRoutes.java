package sample.killrweather;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.ExceptionHandler;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.serialization.jackson.JacksonObjectMapperProvider;
import com.fasterxml.jackson.databind.ObjectMapper;

import static akka.http.javadsl.server.Directives.*;
import static akka.http.javadsl.server.PathMatchers.*;


import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class WeatherRoutes {

  private final ClusterSharding sharding;
  private final Duration timeout;
  private final ObjectMapper objectMapper;
  private final Unmarshaller<HttpEntity, WeatherStation.Data> dataUnmarshaller;

  public WeatherRoutes(ActorSystem<?> system) {
    sharding = ClusterSharding.get(system);
    timeout = system.settings().config().getDuration("killrweather.routes.ask-timeout");
    // use a pre-configured object mapper from akka-jackson also for HTTP JSON
    // this lets us use the -parameters compiler argument to skip annotating field names on immutable classes
    objectMapper = JacksonObjectMapperProvider.get(system).getOrCreate("jackson-json", Optional.empty());
    dataUnmarshaller = Jackson.unmarshaller(objectMapper, WeatherStation.Data.class);
  }

  private CompletionStage<WeatherStation.DataRecorded> recordData(long wsid, WeatherStation.Data data) {
    EntityRef<WeatherStation.Command> ref = sharding.entityRefFor(WeatherStation.TypeKey, Long.toString(wsid));
    return ref.ask(replyTo -> new WeatherStation.Record(data, System.currentTimeMillis(), replyTo), timeout);
  }

  private CompletionStage<WeatherStation.QueryResult> query(long wsid, WeatherStation.DataType dataType, WeatherStation.Function function) {
    EntityRef<WeatherStation.Command> ref = sharding.entityRefFor(WeatherStation.TypeKey, Long.toString(wsid));
    return ref.ask(replyTo -> new WeatherStation.Query(dataType, function, replyTo), timeout);
  }

  // unmarshallers for the query parameters
  private final Unmarshaller<String, WeatherStation.Function> functionUnmarshaller = Unmarshaller.sync(text -> {
    String lcText = text.toLowerCase();
    switch(lcText) {
      case "current": return WeatherStation.Function.Current;
      case "highlow": return WeatherStation.Function.HighLow;
      case "average": return WeatherStation.Function.Average;
      default: throw new IllegalArgumentException("Unknown function " + lcText);
    }
  });
  private final Unmarshaller<String, WeatherStation.DataType> dataTypeUnmarshaller = Unmarshaller.sync(text -> {
    String lcText = text.toLowerCase();
    switch(lcText) {
      case "temperature": return WeatherStation.DataType.Temperature;
      case "dewpoint": return WeatherStation.DataType.DewPoint;
      case "pressure": return WeatherStation.DataType.Pressure;
      default: throw new IllegalArgumentException("Unknown data type " + lcText);
    }
  });



  public Route weather() {
    return path(segment("weather").slash().concat(longSegment()), wsid ->
      concat(
        get(() ->
          parameter(dataTypeUnmarshaller, "type", (dataType ->
            parameter(functionUnmarshaller, "function", (function ->
              completeOKWithFuture(query(wsid, dataType, function), Jackson.marshaller())
            ))
          ))
        ),
        post(() ->
          entity(dataUnmarshaller, data ->
            onSuccess(recordData(wsid, data), performed ->
                complete(StatusCodes.ACCEPTED, performed + " from event time: " + data.eventTime)
            )
          )
        )
      )
    );
  }

}
