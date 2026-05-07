package sample.killrweather;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A sharded `WeatherStation` has a set of recorded datapoints
 * For each weather station common cumulative computations can be run:
 * aggregate, averages, high/low, topK (e.g. the top N highest temperatures).
 *
 * Note that since this station is not storing its state anywhere else than in JVM memory, if Akka Cluster Sharding
 * rebalances it - moves it to another node because of cluster nodes added removed etc - it will lose all its state.
 * For a sharded entity to have state that survives being stopped and started again it needs to be persistent,
 * for example by being an EventSourcedBehavior.
 */
final class WeatherStation extends AbstractBehavior<WeatherStation.Command> {

  // setup for using WeatherStations through Akka Cluster Sharding
  // these could also live elsewhere and the WeatherStation class be completely
  // oblivious to being used in sharding
  public static final EntityTypeKey<Command> TypeKey =
    EntityTypeKey.create(WeatherStation.Command.class, "WeatherStation");

  public static void initSharding(ActorSystem<?> system) {
    ClusterSharding.get(system).init(Entity.of(TypeKey, entityContext ->
      WeatherStation.create(entityContext.getEntityId())
    ));
  }

  // actor commands and responses
  interface Command extends CborSerializable {}

  public static final class Record implements Command {
    public final Data data;
    public final long processingTimestamp;
    public final ActorRef<DataRecorded> replyTo;
    public Record(Data data, long processingTimestamp, ActorRef<DataRecorded> replyTo) {
      this.data = data;
      this.processingTimestamp = processingTimestamp;
      this.replyTo = replyTo;
    }
  }
  public static final class DataRecorded implements CborSerializable {
    public final String wsid;
    @JsonCreator
    public DataRecorded(String wsid) {
      this.wsid = wsid;
    }

    @Override
    public String toString() {
      return "DataRecorded{" +
          "wsid='" + wsid + '\'' +
          '}';
    }
  }

  public static final class Query implements Command {
    public final DataType dataType;
    public final Function func;
    public final ActorRef<QueryResult> replyTo;
    public Query(DataType dataType, Function func, ActorRef<QueryResult> replyTo) {
      this.dataType = dataType;
      this.func = func;
      this.replyTo = replyTo;
    }
  }
  public static final class QueryResult implements CborSerializable {
    public final String wsid;
    public final WeatherStation.DataType dataType;
    public final WeatherStation.Function function;
    public final int readings;
    public final List<TimeWindow> value;
    @JsonCreator
    public QueryResult(String wsid, WeatherStation.DataType dataType, WeatherStation.Function function, int readings, List<TimeWindow> value) {
      this.wsid = wsid;
      this.dataType = dataType;
      this.function = function;
      this.readings = readings;
      this.value = value;
    }
  }



  // small domain model for querying and storing weather data

  enum Function {
    // readable names needed for the HTTP API JSON marshalling
    @JsonProperty("highlow")
    HighLow,
    @JsonProperty("average")
    Average,
    @JsonProperty("current")
    Current
  }
  enum DataType {
    @JsonProperty("temperature")
    Temperature,
    @JsonProperty("dewpoint")
    DewPoint,
    @JsonProperty("pressure")
    Pressure
  }

  public static class Data {
    /**
     * unix timestamp when collected
     */
    public final long eventTime;
    public final DataType dataType;
    /**
     * Air temperature (degrees Celsius)
     */
    public final double value;

    @JsonCreator
    public Data(long eventTime, DataType dataType, double value) {
      this.eventTime = eventTime;
      this.dataType = dataType;
      this.value = value;
    }
  }

  public final class TimeWindow {
    public final long start;
    public final long end;
    public final double value;
    public TimeWindow(long start, long end, double value) {
      this.start = start;
      this.end = end;
      this.value = value;
    }
  }

  public static Behavior<Command> create(String wsid) {
    return Behaviors.setup(context ->
        new WeatherStation(context, wsid)
    );
  }

  private static double average(List<Double> values) {
    return values.stream().mapToDouble(i -> i).average().getAsDouble();
  }

  private final String wsid;
  private final List<Data> values = new ArrayList<>();

  public WeatherStation(ActorContext<Command> context, String wsid) {
    super(context);
    this.wsid = wsid;
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(Record.class, this::onRecord)
        .onMessage(Query.class, this::onQuery)
        .onSignalEquals(PostStop.instance(), this::postStop)
        .build();
  }

  private Behavior<Command> onRecord(Record record) {
    values.add(record.data);
    if (getContext().getLog().isDebugEnabled()) {
      List<Double> dataForSameType = values.stream()
          .filter(d -> d.dataType == record.data.dataType)
          .map(d -> d.value)
          .collect(Collectors.toList());
      double averageForSameType = average(dataForSameType);
      getContext().getLog().debug("{} total readings from station {}, type {}, average {}, diff: processingTime - eventTime: {} ms",
          values.size(),
          wsid,
          record.data.dataType,
          averageForSameType,
          record.processingTimestamp - record.data.eventTime
      );
    }
    record.replyTo.tell(new DataRecorded(wsid));
    return this;
  }

  private Behavior<Command> onQuery(Query query) {
    List<Data> dataForType = values.stream().filter(d -> d.dataType == query.dataType).collect(Collectors.toList());
    final List<TimeWindow> queryResult;
    if (dataForType.isEmpty()) {
      queryResult = Collections.emptyList();
    } else {
      switch (query.func) {
        case Average:
          long start = dataForType.stream().findFirst().map(d -> d.eventTime).orElse(0L);
          long end = dataForType.isEmpty() ? 0 : dataForType.get(dataForType.size() - 1).eventTime;
          List<Double> valuesForType = dataForType.stream().map(d -> d.value).collect(Collectors.toList());
          queryResult = Collections.singletonList(new TimeWindow(start, end, average(valuesForType)));
          break;
        case HighLow:
          Data min = dataForType.stream().reduce((a, b) -> a.value < b.value ? a : b).get();
          Data max = dataForType.stream().reduce((a, b) -> a.value > b.value ? a : b).get();
          queryResult = Arrays.asList(
              new TimeWindow(min.eventTime, max.eventTime, min.value),
              new TimeWindow(min.eventTime, max.eventTime, max.value));
          break;
        case Current:
          // we know it is not empty from up above
          Data current = dataForType.get(dataForType.size() - 1);
          queryResult = Collections.singletonList(new TimeWindow(current.eventTime, current.eventTime, current.value));
          break;
        default:
          throw new IllegalArgumentException("Unknown operation " + query.func);
      }
    }
    query.replyTo.tell(new QueryResult(wsid, query.dataType, query.func, dataForType.size(), queryResult));
    return this;
  }

  private Behavior<Command> postStop() {
    getContext().getLog().info("Stopping, losing all recorded state for station {}", wsid);
    return this;
  }

}
