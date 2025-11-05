/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.coordination

import scala.concurrent.Future

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.coordination.lease.LeaseSettings
import akka.coordination.lease.scaladsl.Lease
import akka.coordination.lease.scaladsl.LeaseProvider
import akka.testkit.AkkaSpec

//#lease-example
class SampleLease(settings: LeaseSettings) extends Lease(settings) {

  override def acquire(): Future[Boolean] = {
    Future.successful(true)
  }

  override def acquire(leaseLostCallback: Option[Throwable] => Unit): Future[Boolean] = {
    Future.successful(true)
  }

  override def release(): Future[Boolean] = {
    Future.successful(true)
  }

  override def checkLease(): Boolean = {
    true
  }
}
//#lease-example

object LeaseDocSpec {

  def config =
    ConfigFactory.parseString("""
      jdocs-lease.lease-class = "jdocs.coordination.LeaseDocTest$SampleLease"
      #lease-config
      akka.actor.provider = cluster
      docs-lease {
        lease-class = "docs.coordination.SampleLease"
        heartbeat-timeout = 100s
        heartbeat-interval = 1s
        lease-operation-timeout = 1s
        # Any lease specific configuration
      }
      #lease-config
    """.stripMargin)

  def blackhole(stuff: Any*): Unit = {
    stuff.toString
    ()
  }
  def doSomethingImportant(leaseLostReason: Option[Throwable]): Unit = {
    leaseLostReason.map(_.toString)
    ()
  }
}

class LeaseDocSpec extends AkkaSpec(LeaseDocSpec.config) {
  import LeaseDocSpec._

  "A docs lease" should {
    "scala lease be loadable from scala" in {

      //#lease-usage
      val lease = LeaseProvider(system).getLease("<name of the lease>", "docs-lease", "owner")
      val acquired: Future[Boolean] = lease.acquire()
      val stillAcquired: Boolean = lease.checkLease()
      val released: Future[Boolean] = lease.release()
      //#lease-usage

      //#lost-callback
      lease.acquire(leaseLostReason => doSomethingImportant(leaseLostReason))
      //#lost-callback

      //#cluster-owner
      val owner = Cluster(system).selfAddress.hostPort
      //#cluster-owner

      // remove compiler warnings
      blackhole(acquired, stillAcquired, released, owner)

    }

    "java lease be loadable from scala" in {
      val lease = LeaseProvider(system).getLease("<name of the lease>", "jdocs-lease", "owner")
      val acquired: Future[Boolean] = lease.acquire()
      val stillAcquired: Boolean = lease.checkLease()
      val released: Future[Boolean] = lease.release()
      lease.acquire(leaseLostReason => doSomethingImportant(leaseLostReason))

      blackhole(acquired, stillAcquired, released)
    }
  }

}
