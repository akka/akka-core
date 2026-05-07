package sample.cluster.stats;


import akka.actor.typed.ActorSystem;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.PoolRouter;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.cluster.typed.Cluster;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class App {

  static final ServiceKey<StatsService.ProcessText> STATS_SERVICE_KEY =
      ServiceKey.create(StatsService.ProcessText.class, "StatsService");

  private static class RootBehavior {
    static Behavior<Void> create() {
      return Behaviors.setup(context -> {
        Cluster cluster = Cluster.get(context.getSystem());

        if (cluster.selfMember().hasRole("compute")) {
          // on every compute node there is one service instance that delegates to N local workers
          final int numberOfWorkers = context.getSystem().settings().config().getInt("stats-service.workers-per-node");
          // The worker has a per word cache, so send the same word to the same local worker child
          Behavior<StatsWorker.Process> workerPoolBehavior =
              Routers.pool(numberOfWorkers, StatsWorker.create().<StatsWorker.Process>narrow())
                .withConsistentHashingRouting(1, process -> process.word);
          ActorRef<StatsWorker.Process> workers =
              context.spawn(workerPoolBehavior, "WorkerRouter");
          ActorRef<StatsService.Command> service =
              context.spawn(StatsService.create(workers.narrow()), "StatsService");

          // published through the receptionist to the other nodes in the cluster
          context.getSystem().receptionist().tell(Receptionist.register(STATS_SERVICE_KEY, service.narrow()));
        }
        if (cluster.selfMember().hasRole("client")) {
          ActorRef<StatsService.ProcessText> serviceRouter =
              context.spawn(Routers.group(STATS_SERVICE_KEY), "ServiceRouter");
          context.spawn(StatsClient.create(serviceRouter), "Client");
        }

        return Behaviors.empty();
      });
    }
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      startup("compute", 25251);
      startup("compute", 25252);
      startup("compute", 0);
      startup("client", 0);
    } else {
      if (args.length != 2)
        throw new IllegalArgumentException("Usage: role port");
      startup(args[0], Integer.parseInt(args[1]));
    }
  }

  private static void startup(String role, int port) {

      // Override the configuration of the port
      Map<String, Object> overrides = new HashMap<>();
      overrides.put("akka.remote.artery.canonical.port", port);
      overrides.put("akka.cluster.roles", Collections.singletonList(role));

      Config config = ConfigFactory.parseMap(overrides)
          .withFallback(ConfigFactory.load("stats"));

      ActorSystem<Void> system = ActorSystem.create(RootBehavior.create(), "ClusterSystem", config);
  }
}
