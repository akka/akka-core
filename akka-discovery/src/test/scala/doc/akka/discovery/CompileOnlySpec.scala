/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package doc.akka.discovery

import akka.actor.ActorSystem

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object CompileOnlySpec {

  //#loading
  import akka.discovery.Discovery

  val system = ActorSystem()
  val serviceDiscovery = Discovery(system).discovery
  //#loading

  //#basic
  import akka.discovery.Lookup

  serviceDiscovery.lookup(Lookup("akka.io"), 1.second)
  // Convenience for a Lookup with only a serviceName
  serviceDiscovery.lookup("akka.io", 1.second)
  //#basic

  //#full
  import akka.discovery.Lookup
  import akka.discovery.ServiceDiscovery.Resolved

  val lookup: Future[Resolved] =
    serviceDiscovery.lookup(Lookup("akka.io").withPortName("remoting").withProtocol("tcp"), 1.second)
  //#full

  // compiler
  lookup.foreach(println)

}
