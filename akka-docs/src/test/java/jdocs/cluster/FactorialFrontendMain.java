/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.TimeUnit;

public class FactorialFrontendMain {

  public static void main(String[] args) {
    final int upToN = 200;

    final Config config =
        ConfigFactory.parseString("akka.cluster.roles = [frontend]")
            .withFallback(ConfigFactory.load("factorial"));

    final ActorSystem system = ActorSystem.create("ClusterSystem", config);
    system.log().info("Factorials will start when 2 backend members in the cluster.");
    // #registerOnUp
    Cluster.get(system)
        .registerOnMemberUp(
            new Runnable() {
              @Override
              public void run() {
                system.actorOf(
                    Props.create(FactorialFrontend.class, upToN, true), "factorialFrontend");
              }
            });
    // #registerOnUp

    // #registerOnRemoved
    Cluster.get(system)
        .registerOnMemberRemoved(
            new Runnable() {
              @Override
              public void run() {
                // exit JVM when ActorSystem has been terminated
                final Runnable exit =
                    new Runnable() {
                      @Override
                      public void run() {
                        System.exit(0);
                      }
                    };
                system.registerOnTermination(exit);

                // shut down ActorSystem
                system.terminate();

                // In case ActorSystem shutdown takes longer than 10 seconds,
                // exit the JVM forcefully anyway.
                // We must spawn a separate thread to not block current thread,
                // since that would have blocked the shutdown of the ActorSystem.
                new Thread() {
                  @Override
                  public void run() {
                    try {
                      system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                      System.exit(-1);
                    }
                  }
                }.start();
              }
            });
    // #registerOnRemoved

  }
}
