/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

import akka.NotUsed
import akka.actor.ActorRef
import akka.stream.WatchedActorTerminatedException
import akka.stream.scaladsl.Flow

import scala.annotation.nowarn

@nowarn("msg=never used") // sample snippets
object Watch {

  def someActor(): ActorRef = ???

  def watchExample(): Unit = {
    //#watch
    val ref: ActorRef = someActor()
    val flow: Flow[String, String, NotUsed] =
      Flow[String].watch(ref).recover {
        case _: WatchedActorTerminatedException => s"$ref terminated"
      }
    //#watch
  }

}
