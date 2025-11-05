/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;

public class StatsSampleOneMasterClientMain {

  public static void main(String[] args) {
    // note that client is not a compute node, role not defined
    ActorSystem system = ActorSystem.create("ClusterSystem", ConfigFactory.load("stats2"));
    system.actorOf(Props.create(StatsSampleClient.class, "/user/statsServiceProxy"), "client");
  }
}
