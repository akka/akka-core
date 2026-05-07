/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.coordination.lease.TestLease
import akka.coordination.lease.TestLeaseExt
import akka.testkit.AkkaSpec
import akka.testkit.TestException
import akka.testkit.TestProbe

class ImportantSingleton(lifeCycleProbe: ActorRef) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    log.info("Important Singleton Starting")
    lifeCycleProbe ! "preStart"
  }

  override def postStop(): Unit = {
    log.info("Important Singleton Stopping")
    lifeCycleProbe ! "postStop"
  }

  override def receive: Receive = {
    case msg =>
      sender() ! msg
  }
}

class ClusterSingletonLeaseSpec extends AkkaSpec(ConfigFactory.parseString("""
     akka.loglevel = INFO
     akka.actor.provider = cluster

     akka.cluster.singleton {
       use-lease = "test-lease"
       lease-retry-interval = 2000ms
     }
     akka.remote.artery.canonical.port = 0
  """).withFallback(TestLease.config)) {
  import TestLease.{ AcquireReq, ReleaseReq }

  val cluster = Cluster(system)
  val testLeaseExt = TestLeaseExt(system)

  override protected def atStartup(): Unit = {
    cluster.join(cluster.selfAddress)
    awaitAssert {
      cluster.selfMember.status shouldEqual MemberStatus.Up
    }
  }

  def extSystem: ExtendedActorSystem = system.asInstanceOf[ExtendedActorSystem]

  val counter = new AtomicInteger()

  def nextName() = s"important-${counter.getAndIncrement()}"

  val shortDuration = 50.millis

  val leaseOwner = cluster.selfMember.address.hostPort

  def nextSettings() = ClusterSingletonManagerSettings(system).withSingletonName(nextName())

  def leaseNameFor(settings: ClusterSingletonManagerSettings): String =
    s"ClusterSingletonLeaseSpec-singleton-akka://ClusterSingletonLeaseSpec/user/${settings.singletonName}"

  "A singleton with lease" should {

    "not start until lease is available" in {
      val probe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(probe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      } // allow singleton manager to create the lease
      probe.expectNoMessage(shortDuration)
      testLease.initialPromise.complete(Success(true))
      probe.expectMsg("preStart")
    }

    "do not start if lease acquire returns false" in {
      val probe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(probe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      } // allow singleton manager to create the lease
      probe.expectNoMessage(shortDuration)
      testLease.initialPromise.complete(Success(false))
      probe.expectNoMessage(shortDuration)
    }

    "retry trying to get lease if acquire returns false" in {
      val singletonProbe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(singletonProbe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      } // allow singleton manager to create the lease
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      singletonProbe.expectNoMessage(shortDuration)
      val nextResponse = Promise[Boolean]()
      testLease.setNextAcquireResult(nextResponse.future)
      testLease.initialPromise.complete(Success(false))
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      singletonProbe.expectNoMessage(shortDuration)
      nextResponse.complete(Success(true))
      singletonProbe.expectMsg("preStart")
    }

    "do not start if lease acquire fails" in {
      val probe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(probe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      } // allow singleton manager to create the lease
      probe.expectNoMessage(shortDuration)
      testLease.initialPromise.failure(TestException("no lease for you"))
      probe.expectNoMessage(shortDuration)
    }

    "retry trying to get lease if acquire returns fails" in {
      val singletonProbe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(singletonProbe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      } // allow singleton manager to create the lease
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      singletonProbe.expectNoMessage(shortDuration)
      val nextResponse = Promise[Boolean]()
      testLease.setNextAcquireResult(nextResponse.future)
      testLease.initialPromise.failure(TestException("no lease for you"))
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      singletonProbe.expectNoMessage(shortDuration)
      nextResponse.complete(Success(true))
      singletonProbe.expectMsg("preStart")
    }

    "stop singleton if the lease fails periodic check" in {
      val lifecycleProbe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(lifecycleProbe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      }
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      testLease.initialPromise.complete(Success(true))
      lifecycleProbe.expectMsg("preStart")
      val callback = testLease.getCurrentCallback()
      callback(None)
      lifecycleProbe.expectMsg("postStop")
      testLease.probe.expectMsg(ReleaseReq(leaseOwner))

      // should try and reacquire lease
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      lifecycleProbe.expectMsg("preStart")
    }

    "release lease when leaving oldest" in {
      val singletonProbe = TestProbe()
      val settings = nextSettings()
      system.actorOf(
        ClusterSingletonManager.props(Props(new ImportantSingleton(singletonProbe.ref)), PoisonPill, settings),
        settings.singletonName)
      val testLease = awaitAssert {
        testLeaseExt.getTestLease(leaseNameFor(settings))
      } // allow singleton manager to create the lease
      singletonProbe.expectNoMessage(shortDuration)
      testLease.probe.expectMsg(AcquireReq(leaseOwner))
      testLease.initialPromise.complete(Success(true))
      singletonProbe.expectMsg("preStart")
      cluster.leave(cluster.selfAddress)
      testLease.probe.expectMsg(ReleaseReq(leaseOwner))
    }
  }
}
