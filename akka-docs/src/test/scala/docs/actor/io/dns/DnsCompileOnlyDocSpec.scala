/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor.io.dns

import akka.actor.{ ActorRef, ActorSystem }
import akka.io.dns.DnsProtocol
import akka.io.dns.DnsProtocol.Srv
import akka.pattern.ask
import akka.io.{ Dns, IO }
import akka.util.Timeout

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration._

@nowarn("msg=deprecated")
@nowarn("msg=never used") // sample snippets
object DnsCompileOnlyDocSpec {

  implicit val system: ActorSystem = ActorSystem()
  implicit val timeout: Timeout = Timeout(1.second)

  val actorRef: ActorRef = ???
  //#resolve
  val initial: Option[Dns.Resolved] = Dns(system).cache.resolve("google.com")(system, actorRef)
  val cached: Option[Dns.Resolved] = Dns(system).cache.cached("google.com")
  //#resolve

  {
    //#actor-api-inet-address
    val resolved: Future[Dns.Resolved] = (IO(Dns) ? Dns.Resolve("google.com")).mapTo[Dns.Resolved]
    //#actor-api-inet-address

  }

  {
    //#actor-api-async
    val resolved: Future[DnsProtocol.Resolved] =
      (IO(Dns) ? DnsProtocol.Resolve("google.com")).mapTo[DnsProtocol.Resolved]
    //#actor-api-async
  }

  {
    //#srv
    val resolved: Future[DnsProtocol.Resolved] =
      (IO(Dns) ? DnsProtocol.Resolve("your-service", Srv)).mapTo[DnsProtocol.Resolved]
    //#srv
  }

}
