package sample.killrweather;

import akka.actor.CoordinatedShutdown;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Adapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.server.Route;
import akka.stream.Materializer;
import akka.stream.SystemMaterializer;

import java.net.InetSocketAddress;
import java.time.Duration;

import static akka.Done.done;

final class WeatherHttpServer {

  public static void start(Route routes, int port, ActorSystem<?> system) {
    akka.actor.ActorSystem classicActorSystem = Adapter.toClassic(system);

    Materializer materializer = SystemMaterializer.get(system).materializer();

    Http.get(classicActorSystem).bindAndHandle(
        routes.flow(classicActorSystem, materializer),
        ConnectHttp.toHost("localhost", port),
        materializer
    ).whenComplete((binding, failure) -> {
      if (failure == null) {
        final InetSocketAddress address = binding.localAddress();
        system.log().info(
            "WeatherServer online at http://{}:{}/",
            address.getHostString(),
            address.getPort());

        CoordinatedShutdown.get(classicActorSystem).addTask(
            CoordinatedShutdown.PhaseServiceRequestsDone(),
            "http-graceful-terminate",
            () ->
              binding.terminate(Duration.ofSeconds(10)).thenApply(terminated -> {
                system.log().info( "WeatherServer http://{}:{}/ graceful shutdown completed",
                    address.getHostString(),
                    address.getPort()
                );
                return done();
              })
        );
      } else {
        system.log().error("Failed to bind HTTP endpoint, terminating system", failure);
        system.terminate();
      }
    });
  }
}
