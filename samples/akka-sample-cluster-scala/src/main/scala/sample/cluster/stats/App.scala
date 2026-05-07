package sample.cluster.stats

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.Routers
import akka.cluster.typed.Cluster
import com.typesafe.config.ConfigFactory

object App {

  val StatsServiceKey = ServiceKey[StatsService.ProcessText]("StatsService")

  private object RootBehavior {
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      val cluster = Cluster(ctx.system)
      if (cluster.selfMember.hasRole("compute")) {
        // on every compute node there is one service instance that delegates to N local workers
        val numberOfWorkers =
          ctx.system.settings.config.getInt("stats-service.workers-per-node")
        val workers = ctx
          .spawn(
            Routers
              .pool(numberOfWorkers)(StatsWorker().narrow[StatsWorker.Process])
              // the worker has a per word cache, so send the same word to the same local worker child
              .withConsistentHashingRouting(1, _.word),
            "WorkerRouter"
          )
        val service = ctx.spawn(StatsService(workers), "StatsService")

        // published through the receptionist to the other nodes in the cluster
        ctx.system.receptionist ! Receptionist
          .Register(StatsServiceKey, service)
      }
      if (cluster.selfMember.hasRole("client")) {
        val serviceRouter =
          ctx.spawn(Routers.group(App.StatsServiceKey), "ServiceRouter")
        ctx.spawn(StatsClient(serviceRouter), "Client")
      }
      Behaviors.empty[Nothing]
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup("compute", 25251)
      startup("compute", 25252)
      startup("compute", 0)
      startup("client", 0)
    } else {
      require(args.length == 2, "Usage: role port")
      startup(args(0), args(1).toInt)
    }
  }

  private def startup(role: String, port: Int): Unit = {

    // Override the configuration of the port when specified as program argument
    val config = ConfigFactory
      .parseString(s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("stats"))

    ActorSystem[Nothing](RootBehavior(), "ClusterSystem", config)
  }
}
