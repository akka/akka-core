package sample.sharding.kafka

import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior, Terminated }
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import akka.http.scaladsl._
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.management.scaladsl.AkkaManagement
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Future
import scala.util.{ Failure, Success }

sealed trait Command
case object NodeMemberUp extends Command
final case class ShardingStarted(region: ActorRef[UserEvents.Command]) extends Command
final case class BindingFailed(reason: Throwable) extends Command

object Main {
  def main(args: Array[String]): Unit = {

    def isInt(s: String): Boolean = s.matches("""\d+""")

    args.toList match {
      case single :: Nil if isInt(single) =>
        val nr = single.toInt
        init(2550 + nr, 8550 + nr, 8080 + nr)
      case portString :: managementPort :: frontEndPort :: Nil
          if isInt(portString) && isInt(managementPort) && isInt(frontEndPort) =>
        init(portString.toInt, managementPort.toInt, frontEndPort.toInt)
      case _ =>
        throw new IllegalArgumentException("usage: <remotingPort> <managementPort> <frontEndPort>")
    }
  }

  def init(remotingPort: Int, akkaManagementPort: Int, frontEndPort: Int): Unit = {
    ActorSystem(Behaviors.setup[Command] { ctx =>
      AkkaManagement(ctx.system.toClassic).start()
      val cluster = Cluster(ctx.system)
      val upAdapter = ctx.messageAdapter[SelfUp](_ => NodeMemberUp)
      cluster.subscriptions ! Subscribe(upAdapter, classOf[SelfUp])
      val settings = ProcessorSettings("kafka-to-sharding-processor", ctx.system.toClassic)
      ctx.pipeToSelf(UserEvents.init(ctx.system, settings)) {
        case Success(extractor) => ShardingStarted(extractor)
        case Failure(ex)        => throw ex
      }
      starting(ctx, None, joinedCluster = false, settings)
    }, "KafkaToSharding", config(remotingPort, akkaManagementPort))

    def start(
        ctx: ActorContext[Command],
        region: ActorRef[UserEvents.Command],
        settings: ProcessorSettings): Behavior[Command] = {
      import ctx.executionContext
      ctx.log.info("Sharding started and joined cluster. Starting event processor")
      val eventProcessor = ctx.spawn[Nothing](UserEventsKafkaProcessor(region, settings), "kafka-event-processor")
      val binding: Future[Http.ServerBinding] = startGrpc(frontEndPort, region)(ctx.system)
      binding.onComplete {
        case Failure(t) =>
          ctx.self ! BindingFailed(t)
        case _ =>
      }
      running(ctx, binding, eventProcessor)
    }

    def starting(
        ctx: ActorContext[Command],
        sharding: Option[ActorRef[UserEvents.Command]],
        joinedCluster: Boolean,
        settings: ProcessorSettings): Behavior[Command] = Behaviors.receive[Command] {
      case (ctx, ShardingStarted(region)) if joinedCluster =>
        ctx.log.info("Sharding has started")
        start(ctx, region, settings)
      case (_, ShardingStarted(region)) =>
        ctx.log.info("Sharding has started")
        starting(ctx, Some(region), joinedCluster, settings)
      case (ctx, NodeMemberUp) if sharding.isDefined =>
        ctx.log.info("Member has joined the cluster")
        start(ctx, sharding.get, settings)
      case (_, NodeMemberUp) =>
        ctx.log.info("Member has joined the cluster")
        starting(ctx, sharding, joinedCluster = true, settings)
      case (_, BindingFailed(reason)) =>
        ctx.log.error("Failed to bind gRPC server", reason)
        Behaviors.stopped
    }

    def running(
        ctx: ActorContext[Command],
        binding: Future[Http.ServerBinding],
        processor: ActorRef[Nothing]): Behavior[Command] =
      Behaviors
        .receiveMessagePartial[Command] {
          case BindingFailed(t) =>
            ctx.log.error("Failed to bind front end", t)
            Behaviors.stopped
        }
        .receiveSignal {
          case (_, Terminated(`processor`)) =>
            ctx.log.warn("Kafka event processor stopped. Shutting down")
            binding.map(_.unbind())(ctx.executionContext)
            Behaviors.stopped
        }

    def startGrpc(frontEndPort: Int, region: ActorRef[UserEvents.Command])(
        implicit system: ActorSystem[_]): Future[Http.ServerBinding] = {
      val service: HttpRequest => Future[HttpResponse] =
        UserServiceHandler(new UserGrpcService(system, region))(system.toClassic)
      // FIXME Scala 3 does not accept typed system to Http apply here for some reason
      Http().newServerAt(interface = "127.0.0.1", port = frontEndPort).bind(service)
    }

    def config(port: Int, managementPort: Int): Config =
      ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port = $port
      akka.management.http.port = $managementPort
       """).withFallback(ConfigFactory.load())

  }
}
