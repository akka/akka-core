/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.testkit

import java.util.UUID

import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike

//#imports
import akka.persistence.testkit.scaladsl.PersistenceInit

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

//#imports

class PersistenceInitSpec extends ScalaTestWithActorTestKit(s"""
  akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
  akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
  akka.persistence.snapshot-store.local.dir = "target/snapshot-${UUID.randomUUID().toString}"
  """) with AnyWordSpecLike {

  "PersistenceInit" should {
    "initialize plugins" in {
      //#init
      val timeout = 5.seconds
      val done: Future[Done] = PersistenceInit.initializeDefaultPlugins(system, timeout)
      Await.result(done, timeout)
      //#init
    }
  }
}
