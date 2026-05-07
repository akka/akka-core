/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future
import scala.concurrent.Promise

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.coordination.lease.scaladsl.Lease
import akka.event.Logging
import akka.testkit.TestProbe
import scala.jdk.CollectionConverters._

object TestLeaseExt extends ExtensionId[TestLeaseExt] with ExtensionIdProvider {
  override def get(system: ActorSystem): TestLeaseExt = super.get(system)
  override def get(system: ClassicActorSystemProvider): TestLeaseExt = super.get(system)
  override def lookup = TestLeaseExt
  override def createExtension(system: ExtendedActorSystem): TestLeaseExt = new TestLeaseExt(system)
}

class TestLeaseExt(val system: ExtendedActorSystem) extends Extension {

  private val testLeases = new ConcurrentHashMap[String, TestLease]()

  def getTestLease(name: String): TestLease = {
    val lease = testLeases.get(name)
    if (lease == null)
      throw new IllegalStateException(
        s"Test lease $name has not been set yet. Current leases ${testLeases.keys().asScala.toList}")
    lease
  }

  def setTestLease(name: String, lease: TestLease): Unit =
    testLeases.put(name, lease)

}

object TestLease {
  final case class AcquireReq(owner: String)
  final case class ReleaseReq(owner: String)

  val config = ConfigFactory.parseString(s"""
    test-lease {
      lease-class = ${classOf[TestLease].getName}
    }
    """.stripMargin)
}

class TestLease(settings: LeaseSettings, system: ExtendedActorSystem) extends Lease(settings) {
  import TestLease._

  val log = Logging(system, classOf[TestLease])
  val probe = TestProbe()(system)

  val initialPromise = Promise[Boolean]()

  private val nextAcquireResult = new AtomicReference[Future[Boolean]](initialPromise.future)
  private val nextCheckLeaseResult = new AtomicReference[Boolean](false)
  private val currentCallBack = new AtomicReference[Option[Throwable] => Unit](_ => ())

  log.info("Creating lease {}", settings)

  TestLeaseExt(system).setTestLease(settings.leaseName, this)

  def setNextAcquireResult(next: Future[Boolean]): Unit =
    nextAcquireResult.set(next)

  def setNextCheckLeaseResult(value: Boolean): Unit =
    nextCheckLeaseResult.set(value)

  def getCurrentCallback(): Option[Throwable] => Unit = currentCallBack.get()

  override def acquire(): Future[Boolean] = {
    log.info("acquire, current response " + nextAcquireResult)
    probe.ref ! AcquireReq(settings.ownerName)
    nextAcquireResult.get()
  }

  override def release(): Future[Boolean] = {
    probe.ref ! ReleaseReq(settings.ownerName)
    Future.successful(true)
  }

  override def checkLease(): Boolean = nextCheckLeaseResult.get

  override def acquire(callback: Option[Throwable] => Unit): Future[Boolean] = {
    currentCallBack.set(callback)
    acquire()
  }

}
