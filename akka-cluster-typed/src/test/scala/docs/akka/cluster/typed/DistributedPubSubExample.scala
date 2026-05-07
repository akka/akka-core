/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.typed

import scala.util.{ Failure, Success, Try }
import scala.concurrent.duration._

import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MemberStatus
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.cluster.typed.{ Cluster, Join }
import akka.event.Logging
import com.typesafe.config.{ Config, ConfigFactory }

object Ontology {

  type DataKey = String
  type Schema = String
  type Version = Long
  type DataSourceType = String
  type DataSinkType = String

  sealed trait DataApi
  trait Status extends DataApi
  sealed trait ProvisionCommand extends DataApi
  final case class ProvisionDataType(key: DataKey, schema: Schema, onBehalfOf: ActorRef[DataApi])
      extends ProvisionCommand
  final case class ProvisionDataSource(ds: DataSourceType, provisioned: DataType, onBehalfOf: ActorRef[DataApi])
      extends ProvisionCommand
  final case class ProvisionDataSink(ds: DataSinkType, provisioned: DataType, onBehalfOf: ActorRef[DataApi])
      extends ProvisionCommand
  final case class CreateSubscriber(key: DataKey) extends ProvisionCommand
  sealed trait ProvisionStatus extends Status {
    def key: DataKey
  }
  final case class ProvisionSuccess(key: DataKey, ref: ActorRef[DataEvent]) extends ProvisionStatus
  final case class ProvisionFailure(key: DataKey, reason: String) extends ProvisionStatus

  sealed trait DataOntology extends DataApi
  sealed trait DataEvent extends DataOntology {
    def key: DataKey
  }
  final case class StartIngestion(key: DataKey, source: Option[DataSource], sink: Option[DataSink]) extends DataEvent
  final case class StopIngestion(key: DataKey) extends DataEvent
  final case class IngestionStarted(key: DataKey, path: String) extends DataEvent
  final case class IngestionStopped(key: DataKey) extends DataEvent
  final case class DataEnvelope(key: DataKey, event: AnyRef) extends DataEvent
  final case class DataType(key: DataKey, schema: Schema, version: Version) extends DataEvent
  sealed trait DataResource {
    def datatype: DataType
  }
  final case class DataSource(datatype: DataType) extends DataResource
  final case class DataSink(datatype: DataType) extends DataResource

  sealed trait RegistrationCommand extends DataApi
  final case class RegisterDataType(
      key: DataKey,
      schema: Schema,
      replyTo: ActorRef[DataApi],
      onBehalfOf: ActorRef[DataApi])
      extends RegistrationCommand
  sealed trait RegistrationStatus extends Status
  final case class DataTypeExists(key: DataKey) extends RegistrationStatus
  final case class RegistrationSuccess(dataType: DataType) extends RegistrationStatus
  final case class RegistrationFailure(key: DataKey, reason: Throwable) extends RegistrationStatus
  final case class RegistrationAck(status: RegistrationStatus, onBehalfOf: ActorRef[DataApi]) extends DataApi

  val RegistrationTopic = "registration"
  val IngestionTopic = "ingestion"

}

object Publisher {

  import Ontology._

  /**
   * Handles new data type setup: validates schema, registers valid data types, publishes new ones to subscribers
   */
  object RegistrationService {

    def apply(): Behavior[AnyRef] = {
      //#publisher
      Behaviors.setup[AnyRef] { context =>
        import akka.cluster.pubsub.DistributedPubSub
        import akka.cluster.pubsub.DistributedPubSubMediator
        val mediator = DistributedPubSub(context.system).mediator

        var registry: Map[DataKey, DataType] = Map.empty

        def register(key: DataKey, schema: Schema): RegistrationStatus =
          registry.get(key) match {
            case Some(_) =>
              DataTypeExists(key)
            case None =>
              validate(schema) match {
                case Success(vs) =>
                  val created = DataType(key, vs, 0)
                  registry += (key -> created)

                  mediator ! DistributedPubSubMediator.Publish(RegistrationTopic, created)
                  RegistrationSuccess(created)
                case Failure(e) =>
                  RegistrationFailure(key, e)
              }
          }

        def validate(schema: Schema): Try[Schema] = {
          Success(schema) // called, stubbed
        }

        Behaviors.receiveMessage {
          case RegisterDataType(key, schema, replyTo, onBehalfOf) =>
            val status = register(key, schema)
            replyTo ! RegistrationAck(status, onBehalfOf)
            Behaviors.same
          case _ =>
            Behaviors.unhandled
        }
      }
      //#publisher

    }
  }

}

object Ingestion {
  import Ontology._

  def apply(dt: DataType, mediator: akka.actor.ActorRef): Behavior[DataEvent] = {
    //#destination
    Behaviors.setup { context =>
      // register to the path
      import akka.actor.typed.scaladsl.adapter._
      mediator ! DistributedPubSubMediator.Put(context.self.toClassic)

      idle(dt, mediator)
    }
    //#destination
  }

  private def idle(dt: DataType, mediator: akka.actor.ActorRef): Behavior[DataEvent] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage {
        case StartIngestion(key, _, sink) if key == dt.key =>
          context.log.info("Processing events for {} storing.", dt.key)
          mediator ! DistributedPubSubMediator.Publish(
            IngestionTopic,
            IngestionStarted(key, context.self.path.toStringWithoutAddress))
          active(key, sink, mediator)

        case _ =>
          idle(dt, mediator)
      }
    }

  /** Would normally be typed more specifically. */
  private def active(key: DataKey, sink: Option[DataSink], mediator: akka.actor.ActorRef): Behavior[DataEvent] =
    //#publisher
    Behaviors.setup { context =>
      Behaviors.receiveMessagePartial[DataEvent] {
        case e: DataEnvelope if e.key == key =>
          // fanout to:
          // validate, slice, dice, re-route, store raw to blob, store pre-aggregated/timeseries to Cs*, etc.
          context.log.debug("Storing to {}.", sink)
          Behaviors.same

        case StopIngestion(k) if k == key =>
          mediator ! DistributedPubSubMediator.Publish(IngestionTopic, IngestionStopped(key))
          // cleanup and graceful shutdown
          Behaviors.stopped
      }
    }
  //#publisher

}

object Subscriber {
  import Ontology._

  def apply(key: DataKey, mediator: akka.actor.ActorRef): Behavior[DataEvent] = {

    //#subscriber
    Behaviors.setup[DataEvent] { context =>
      import akka.actor.typed.scaladsl.adapter._

      mediator ! DistributedPubSubMediator.Subscribe(RegistrationTopic, context.self.toClassic)
      mediator ! DistributedPubSubMediator.Subscribe(IngestionTopic, context.self.toClassic)

      Behaviors.receiveMessagePartial {
        case dt: DataType if dt.key == key =>
          // do some capacity planning
          // allocate some shards
          // provision a source and sink
          // start a new ML stream...it's a data platform wonderland
          wonderland()

        case IngestionStarted(k, path) if k == key =>
          //#send
          // simulate data sent from various data sources:
          (1 to 100).foreach { n =>
            mediator ! DistributedPubSubMediator.Send(
              path,
              msg = DataEnvelope(key, s"hello-$key-$n"),
              localAffinity = true)
          }
          //#send
          andThen(key, mediator)

      }
    }
    //#subscriber
  }

  private def wonderland(): Behavior[DataEvent] = {
    Behaviors.same
  }

  private def andThen(key: DataKey, mediator: akka.actor.ActorRef): Behavior[DataEvent] = {
    // for the example, shutdown
    mediator ! DistributedPubSubMediator.Publish(IngestionTopic, IngestionStopped(key))
    Behaviors.stopped
  }
}

object DataService {
  import Ontology._

  def apply(mediator: akka.actor.ActorRef): Behavior[DataApi] = {
    Behaviors.setup { context =>
      val registration = context.spawn(Publisher.RegistrationService(), "data-registration")

      Behaviors.receiveMessagePartial {
        case ProvisionDataType(key, schema, onBehalfOf) =>
          context.log.info("Sending provision request for {} to registration.", key)
          registration ! RegisterDataType(key, schema, context.self, onBehalfOf)
          Behaviors.same

        case RegistrationAck(status: RegistrationSuccess, onBehalfOf) =>
          context.log.info(
            "New data type was registered, provisioning destination for {}.",
            context.self.path / status.dataType.key)

          val ingestion = context.spawn(Ingestion(status.dataType, mediator), status.dataType.key)
          ingestion ! StartIngestion(status.dataType.key, None, None) // stubbed source/sink from another service
          onBehalfOf ! ProvisionSuccess(status.dataType.key, ingestion)
          Behaviors.same

      }
    }
  }

}

object DataPlatform {
  import Ontology._

  def apply(): Behavior[ProvisionCommand] = {
    Behaviors.setup { context =>
      //#mediator
      val mediator = DistributedPubSub(context.system).mediator
      //#mediator
      val service = context.spawn(DataService(mediator), "data")

      Behaviors.receiveMessagePartial {
        case CreateSubscriber(key) =>
          // subscribe to provisioning for this data type
          context.spawnAnonymous(Subscriber(key, mediator))
          Behaviors.same

        case cmd: ProvisionCommand =>
          service ! cmd
          Behaviors.same

      }
    }
  }
}

object DistributedPubSubExample {
  import akka.actor.testkit.typed.scaladsl.TestProbe
  import Ontology._

  val config: Config = ConfigFactory.parseString("""
        akka.actor.provider = "cluster"
        akka.cluster.pub-sub.max-delta-elements = 500
        akka.cluster.jmx.enabled = off
        akka.remote.artery.canonical.hostname = 127.0.0.1
        akka.remote.artery.canonical.port = 0
        akka.loglevel = INFO
        akka.loggers = ["akka.testkit.TestEventListener"]
    """)

  def createCluster(nodes: List[ActorSystem[_]]): Unit = {
    val clusterUp = (nodes: List[ActorSystem[_]], expected: Int) =>
      nodes.forall { s =>
        Cluster(s).state.members.count(_.status == MemberStatus.up) == expected
      }

    val awaitClusterUp = (nodes: List[ActorSystem[_]], expected: Int) =>
      while (!clusterUp(nodes, expected)) {
        Thread.sleep(1000)
      }

    val seed = nodes.head
    val joinTo = Cluster(seed).selfMember.address

    Cluster(seed).manager ! Join(joinTo)
    awaitClusterUp(List(seed), 1)
    nodes.tail.foreach(Cluster(_).manager ! Join(joinTo))
    awaitClusterUp(nodes, nodes.size)
  }

  def main(args: Array[String]): Unit = {

    val system = ActorSystem[ProvisionCommand](DataPlatform(), "DataPlatform", config)
    val system2 = ActorSystem[ProvisionCommand](DataPlatform(), system.name, config)
    val system3 = ActorSystem[ProvisionCommand](DataPlatform(), system.name, config)
    val nodes = List(system, system2, system3)

    import akka.actor.typed.scaladsl.adapter._
    val log = Logging(system.toClassic.eventStream, system.name)

    createCluster(nodes)
    log.info(s"Cluster up with {} nodes. Starting platform.", nodes.size)

    // provision subscribers
    val key = "DataTypeA"
    nodes.foreach(_ ! CreateSubscriber(key))

    // provision new data type
    val platformProbe = TestProbe[DataApi]()(system)
    val mediator = DistributedPubSub(system).mediator
    mediator ! DistributedPubSubMediator.Subscribe(IngestionTopic, platformProbe.ref.toClassic)

    system ! ProvisionDataType(key, "dummy-schema", platformProbe.ref)
    val provisioned = platformProbe.expectMessageType[ProvisionSuccess](5.seconds)
    log.info(s"Platform provisioned for new data type {}.", provisioned)

    // send and receive in cluster, then for example shutdown
    val started = platformProbe.expectMessageType[IngestionStarted](5.seconds)
    log.info(s"Platform ingestion started for {}.", started.key)

    val stopped = platformProbe.expectMessageType[IngestionStopped](5.seconds)
    log.info(s"Platform ingestion completed for {}.", stopped.key)
    require(Set(started, stopped).forall(_.key == key))

    nodes.foreach(_.terminate())
    log.info("Systems terminating.")
  }

}
