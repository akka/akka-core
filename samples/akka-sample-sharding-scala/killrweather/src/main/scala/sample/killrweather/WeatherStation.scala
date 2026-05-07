package sample.killrweather

import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize

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
private[killrweather] object WeatherStation {

  // setup for using WeatherStations through Akka Cluster Sharding
  // these could also live elsewhere and the WeatherStation class be completely
  // oblivious to being used in sharding
  val TypeKey: EntityTypeKey[WeatherStation.Command] =
    EntityTypeKey[WeatherStation.Command]("WeatherStation")

  def initSharding(system: ActorSystem[_]): Unit =
    ClusterSharding(system).init(Entity(TypeKey) { entityContext =>
      WeatherStation(entityContext.entityId)
    })

  // actor commands and responses
  sealed trait Command extends CborSerializable

  final case class Record(data: Data, processingTimestamp: Long, replyTo: ActorRef[DataRecorded]) extends Command
  final case class DataRecorded(wsid: String) extends CborSerializable

  final case class Query(dataType: DataType, func: Function, replyTo: ActorRef[QueryResult]) extends Command
  final case class QueryResult(
      wsid: String,
      dataType: DataType,
      func: Function,
      readings: Int,
      value: Vector[TimeWindow])
      extends CborSerializable

  // small domain model for querying and storing weather data

  // needed for CBOR serialization with Jackson
  @JsonSerialize(`using` = classOf[DataTypeJsonSerializer])
  @JsonDeserialize(`using` = classOf[DataTypeJsonDeserializer])
  sealed trait DataType
  object DataType {

    /** Temperature in celcius */
    case object Temperature extends DataType
    case object Dewpoint extends DataType
    case object Pressure extends DataType
    val All: Set[DataType] = Set(Temperature, Dewpoint, Pressure)
  }

  // needed for CBOR serialization with Jackson
  @JsonSerialize(`using` = classOf[FunctionJsonSerializer])
  @JsonDeserialize(`using` = classOf[FunctionJsonDeserializer])
  sealed trait Function
  object Function {
    case object HighLow extends Function
    case object Average extends Function
    case object Current extends Function
    val All: Set[Function] = Set(HighLow, Average, Current)
  }

  /**
   * Simplified weather measurement data, actual weather data event comprises many more data points.
   *
   * @param eventTime unix timestamp when collected
   * @param dataType type of data
   * @param value data point value
   */
  final case class Data(eventTime: Long, dataType: DataType, value: Double)
  final case class TimeWindow(start: Long, end: Long, value: Double)

  def apply(wsid: String): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("Starting weather station {}", wsid)

    running(context, wsid, Vector.empty)
  }

  private def average(values: Vector[Double]): Double =
    if (values.isEmpty) Double.NaN
    else values.sum / values.size

  private def running(context: ActorContext[Command], wsid: String, values: Vector[Data]): Behavior[Command] =
    Behaviors
      .receiveMessage[Command] {
        case Record(data, received, replyTo) =>
          val updated = values :+ data
          if (context.log.isDebugEnabled) {
            val averageForSameType = average(updated.filter(_.dataType == data.dataType).map(_.value))
            context.log.debug(
              "{} total readings from station {}, type {}, average {}, diff: processingTime - eventTime: {} ms",
              updated.size,
              wsid,
              data.dataType,
              averageForSameType,
              received - data.eventTime)
          }
          replyTo ! DataRecorded(wsid)
          running(context, wsid, updated) // store

        case Query(dataType, func, replyTo) =>
          val valuesForType = values.filter(_.dataType == dataType)
          val queryResult: Vector[TimeWindow] =
            if (valuesForType.isEmpty) Vector.empty
            else
              func match {
                case Function.Average =>
                  val start: Long = valuesForType.head.eventTime
                  val end: Long = valuesForType.last.eventTime
                  Vector(TimeWindow(start, end, average(valuesForType.map(_.value))))

                case Function.HighLow =>
                  val (start, min) = valuesForType.map(e => e.eventTime -> e.value).min
                  val (end, max) = valuesForType.map(e => e.eventTime -> e.value).max
                  Vector(TimeWindow(start, end, min), TimeWindow(start, end, max))

                case Function.Current =>
                  // we know it is not empty from above
                  Vector(valuesForType.lastOption.map(e => TimeWindow(e.eventTime, e.eventTime, e.value)).get)

              }
          replyTo ! QueryResult(wsid, dataType, func, valuesForType.size, queryResult)
          Behaviors.same

      }
      .receiveSignal {
        case (_, PostStop) =>
          context.log.info("Stopping, losing all recorded state for station {}", wsid)
          Behaviors.same
      }
}
