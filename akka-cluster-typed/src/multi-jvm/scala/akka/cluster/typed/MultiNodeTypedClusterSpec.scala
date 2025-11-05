/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions

import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

import akka.actor.ActorIdentity
import akka.actor.Address
import akka.actor.Identify
import akka.actor.Scheduler
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.SpawnProtocol
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ClusterEvent
import akka.cluster.MemberStatus
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit.WatchedByCoroner
import akka.util.Timeout

trait MultiNodeTypedClusterSpec extends Suite with STMultiNodeSpec with WatchedByCoroner with Matchers {
  self: MultiNodeSpec =>

  override def initialParticipants: Int = roles.size

  implicit def typedSystem: ActorSystem[Nothing] = system.toTyped
  implicit def scheduler: Scheduler = system.scheduler

  private val cachedAddresses = new ConcurrentHashMap[RoleName, Address]

  // TODO: Add support for typed to multi node test kit
  def cluster: Cluster = Cluster(system.toTyped)

  def clusterView: ClusterEvent.CurrentClusterState = cluster.state

  override def expectedTestDuration: FiniteDuration = 60.seconds

  /**
   * Lookup the Address for the role.
   *
   * Implicit conversion from RoleName to Address.
   *
   * It is cached, which has the implication that stopping
   * and then restarting a role (jvm) with another address is not
   * supported.
   */
  implicit def address(role: RoleName): Address = {
    cachedAddresses.get(role) match {
      case null =>
        val address = node(role).address
        cachedAddresses.put(role, address)
        address
      case address => address
    }
  }

  def formCluster(first: RoleName, rest: RoleName*): Unit = {
    runOn(first) {
      cluster.manager ! Join(cluster.selfMember.address)
      awaitAssert(cluster.state.members.exists { m =>
        m.uniqueAddress == cluster.selfMember.uniqueAddress && m.status == MemberStatus.Up
      } should be(true))
    }
    enterBarrier(first.name + "-joined")

    rest.foreach { node =>
      runOn(node) {
        cluster.manager ! Join(address(first))
        awaitAssert(cluster.state.members.exists { m =>
          m.uniqueAddress == cluster.selfMember.uniqueAddress && m.status == MemberStatus.Up
        } should be(true))
      }
    }
    enterBarrier("all-joined")
  }

  private lazy val spawnActor =
    system.actorOf(PropsAdapter(SpawnProtocol()), "testSpawn").toTyped[SpawnProtocol.Command]
  def spawn[T](behavior: Behavior[T], name: String): ActorRef[T] = {
    import akka.testkit.TestDuration
    implicit val timeout: Timeout = testKitSettings.DefaultTimeout.duration.dilated
    val f: Future[ActorRef[T]] = spawnActor.ask(SpawnProtocol.Spawn(behavior, name, Props.empty, _))

    Await.result(f, timeout.duration * 2)
  }

  def identify[A](name: String, r: RoleName): ActorRef[A] = {
    import akka.actor.typed.scaladsl.adapter._
    val sel = system.actorSelection(node(r) / "user" / "testSpawn" / name)
    sel.tell(Identify(None), testActor)
    expectMsgType[ActorIdentity].ref.get.toTyped
  }

}
