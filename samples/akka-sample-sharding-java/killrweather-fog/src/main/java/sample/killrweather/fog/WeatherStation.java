package sample.killrweather.fog;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCode;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.serialization.jackson.JacksonObjectMapperProvider;
import akka.stream.SystemMaterializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletionStage;

public class WeatherStation extends AbstractBehavior<WeatherStation.Command> {

  interface Command {}
  enum Sample implements Command {
    INSTANCE
  }
  private static final class ProcessSuccess implements Command {
    final String msg;
    public ProcessSuccess(String msg) {
      this.msg = msg;
    }
  }
  private static final class ProcessFailure implements Command {
    final Throwable e;
    public ProcessFailure(Throwable e) {
      this.e = e;
    }
  }


  public static Behavior<Command> create(String wsid, FogSettings settings, int httpPort) {
    return Behaviors.setup(context ->
        Behaviors.withTimers(timers ->
          new WeatherStation(context, timers, wsid, settings, httpPort)
        )
    );
  }

  private final String wsid;
  private final FogSettings settings;
  private final TimerScheduler<Command> timers;
  private final Random random = new Random();
  private final Http http;
  private final String stationUrl;
  private final ObjectMapper objectMapper;

  private WeatherStation(ActorContext<Command> context, TimerScheduler<Command> timers, String wsid, FogSettings settings, int httpPort) {
    super(context);
    this.wsid = wsid;
    this.settings = settings;
    this.timers = timers;
    this.http = Http.get(Adapter.toClassic(context.getSystem()));
    stationUrl = "http://" + settings.host + ":" + httpPort + "/weather/" + wsid;
    objectMapper = JacksonObjectMapperProvider.get(getContext().getSystem()).getOrCreate("weather-station", Optional.empty());
    timers.startSingleTimer(Sample.INSTANCE, Sample.INSTANCE, settings.sampleInterval);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
      .onMessageEquals(Sample.INSTANCE, this::onSample)
        .onMessage(ProcessSuccess.class, this::onProcessSuccess)
        .onMessage(ProcessFailure.class, this::onProcessFailure)
        .build();
  }

  private Behavior<Command> onProcessSuccess(ProcessSuccess success) {
    getContext().getLog().info("Successfully registered data: {}", success.msg);
    timers.startSingleTimer(Sample.INSTANCE, Sample.INSTANCE, settings.sampleInterval);
    return this;
  }

  private Behavior<Command> onProcessFailure(ProcessFailure failure) {
    throw new RuntimeException("Failed to register data", failure.e);
  }

  private Behavior<Command> onSample() throws Exception {
    double value = 5 + 30 * random.nextDouble();
    long eventTime = System.currentTimeMillis();
    getContext().getLog().debug("Recording temperature measurement {}", value);
    recordTemperature(eventTime, value);
    return this;
  }

  private void recordTemperature(long eventTime, double value) throws Exception {
    Data data = new Data(eventTime, "temperature", value);

    // FIXME no Java API in Akka HTTP to do this using the marshalling infra, see akka-http#2128
    String json = objectMapper.writeValueAsString(data);
    CompletionStage<String> futureResponseBody =
        http.singleRequest(HttpRequest.POST(stationUrl).withEntity(ContentTypes.APPLICATION_JSON, json))
          .thenCompose(response ->
            Unmarshaller.entityToString().unmarshal(response.entity(), SystemMaterializer.get(getContext().getSystem()).materializer())
              .thenApply(body -> {
                if (response.status().isSuccess())
                  return body;
                else throw new RuntimeException("Failed to register data: " + body);
              })
          );

    getContext().pipeToSelf(futureResponseBody, (response, failure) -> {
      if (failure == null) {
        return new ProcessSuccess(response);
      } else {
        return new ProcessFailure(failure);
      }
    });

  }

}
