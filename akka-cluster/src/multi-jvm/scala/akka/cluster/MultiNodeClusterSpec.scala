/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ Canceled, Outcome, Suite }
import org.scalatest.exceptions.TestCanceledException

import akka.actor.{ Actor, ActorRef, ActorSystem, Address, Deploy, PoisonPill, Props, RootActorPath }
import akka.cluster.ClusterEvent.{ MemberEvent, MemberRemoved }
import akka.event.Logging.ErrorLevel
import akka.remote.DefaultFailureDetectorRegistry
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeSpec, STMultiNodeSpec }
import akka.remote.testkit.MultiNodeConfig
import akka.serialization.jackson.CborSerializable
import akka.testkit._
import akka.testkit.TestEvent._

object MultiNodeClusterSpec {

  def clusterConfigWithFailureDetectorPuppet: Config =
    ConfigFactory
      .parseString("akka.cluster.failure-detector.implementation-class = akka.cluster.FailureDetectorPuppet")
      .withFallback(clusterConfig)

  def clusterConfig(failureDetectorPuppet: Boolean): Config =
    if (failureDetectorPuppet) clusterConfigWithFailureDetectorPuppet else clusterConfig

  def clusterConfig: Config =
    ConfigFactory.parseString("""
    akka.actor.provider = cluster
    akka.actor.warn-about-java-serializer-usage = off
    akka.cluster {
      jmx.enabled                         = off
      gossip-interval                     = 200 ms
      leader-actions-interval             = 200 ms
      unreachable-nodes-reaper-interval   = 500 ms
      periodic-tasks-initial-delay        = 300 ms
      publish-stats-interval              = 0 s # always, when it happens
      failure-detector.heartbeat-interval = 500 ms
      run-coordinated-shutdown-when-down = off

      sharding {
        retry-interval = 200ms
        waiting-for-state-timeout = 200ms
      }
    }
    akka.loglevel = INFO
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.loggers = ["akka.testkit.TestEventListener"]
    akka.test {
      single-expect-default = 5 s
    }

    """)

  // sometimes we need to coordinate test shutdown with messages instead of barriers
  object EndActor {
    case object SendEnd extends CborSerializable
    case object End extends CborSerializable
    case object EndAck extends CborSerializable
  }

  class EndActor(testActor: ActorRef, target: Option[Address]) extends Actor {
    import EndActor._
    def receive: Receive = {
      case SendEnd =>
        target.foreach { t =>
          context.actorSelection(RootActorPath(t) / self.path.elements) ! End
        }
      case End =>
        testActor.forward(End)
        sender() ! EndAck
      case EndAck =>
        testActor.forward(EndAck)
    }
  }
}

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiNodeClusterSpec(multiNodeconfig: MultiNodeConfig)
    extends MultiNodeSpec(multiNodeconfig)
    with Suite
    with STMultiNodeSpec
    with WatchedByCoroner {

  override def initialParticipants: Int = roles.size

  private val cachedAddresses = new ConcurrentHashMap[RoleName, Address]

  override protected def atStartup(): Unit = {
    startCoroner()
    muteLog()
    super.atStartup()
  }

  override protected def afterTermination(): Unit = {
    super.afterTermination()
    stopCoroner()
  }

  override def expectedTestDuration = 60.seconds

  def muteLog(sys: ActorSystem = system): Unit = {
    if (!sys.log.isDebugEnabled) {
      Seq(
        ".*Cluster Node.* - registered cluster JMX MBean.*",
        ".*Cluster Node.* - is starting up.*",
        ".*Shutting down cluster Node.*",
        ".*Cluster node successfully shut down.*",
        ".*Using a dedicated scheduler for cluster.*").foreach { s =>
        sys.eventStream.publish(Mute(EventFilter.info(pattern = s)))
      }

      muteDeadLetters(
        classOf[ClusterHeartbeatSender.Heartbeat],
        classOf[ClusterHeartbeatSender.HeartbeatRsp],
        classOf[GossipEnvelope],
        classOf[GossipStatus],
        classOf[InternalClusterAction.Tick],
        classOf[akka.actor.PoisonPill],
        classOf[akka.dispatch.sysmsg.DeathWatchNotification])(sys)

    }
  }

  def muteMarkingAsUnreachable(sys: ActorSystem = system): Unit =
    if (!sys.log.isDebugEnabled)
      sys.eventStream.publish(Mute(EventFilter.error(pattern = ".*Marking.* as UNREACHABLE.*")))

  def muteMarkingAsReachable(sys: ActorSystem = system): Unit =
    if (!sys.log.isDebugEnabled)
      sys.eventStream.publish(Mute(EventFilter.info(pattern = ".*Marking.* as REACHABLE.*")))

  override def afterAll(): Unit = {
    if (!log.isDebugEnabled) {
      muteDeadLetters()()
      system.eventStream.setLogLevel(ErrorLevel)
    }
    super.afterAll()
  }

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

  // Cluster tests are written so that if previous step (test method) failed
  // it will most likely not be possible to run next step. This ensures
  // fail fast of steps after the first failure.
  private var failed = false
  override protected def withFixture(test: NoArgTest): Outcome =
    if (failed) {
      Canceled(new TestCanceledException("Previous step failed", 0))
    } else {
      val out = super.withFixture(test)
      if (!out.isSucceeded)
        failed = true
      out
    }

  def clusterView: ClusterReadView = cluster.readView

  /**
   * Get the cluster node to use.
   */
  def cluster: Cluster = Cluster(system)

  /**
   * Use this method for the initial startup of the cluster node.
   */
  def startClusterNode(): Unit = {
    if (clusterView.members.isEmpty) {
      cluster.join(myself)
      awaitAssert(clusterView.members.map(_.address) should contain(address(myself)))
    } else
      clusterView.self
  }

  /**
   * Initialize the cluster of the specified member
   * nodes (roles) and wait until all joined and `Up`.
   * First node will be started first  and others will join
   * the first.
   */
  def awaitClusterUp(roles: RoleName*): Unit = {
    runOn(roles.head) {
      // make sure that the node-to-join is started before other join
      startClusterNode()
    }
    enterBarrier(roles.head.name + "-started")
    if (roles.tail.contains(myself)) {
      cluster.join(roles.head)
    }
    if (roles.contains(myself)) {
      awaitMembersUp(numberOfMembers = roles.length)
    }
    enterBarrier(roles.map(_.name).mkString("-") + "-joined")
  }

  /**
   * Join the specific node within the given period by sending repeated join
   * requests at periodic intervals until we succeed.
   */
  def joinWithin(joinNode: RoleName, max: Duration = remainingOrDefault, interval: Duration = 1.second): Unit = {
    def memberInState(member: Address, status: Seq[MemberStatus]): Boolean =
      clusterView.members.exists { m =>
        (m.address == member) && status.contains(m.status)
      }

    cluster.join(joinNode)
    awaitCond(
      {
        if (memberInState(joinNode, List(MemberStatus.Up)) &&
            memberInState(myself, List(MemberStatus.Joining, MemberStatus.Up)))
          true
        else {
          cluster.join(joinNode)
          false
        }
      },
      max,
      interval)
  }

  /**
   * Assert that the member addresses match the expected addresses in the
   * sort order used by the cluster.
   */
  def assertMembers(gotMembers: Iterable[Member], expectedAddresses: Address*): Unit = {
    import Member.addressOrdering
    val members = gotMembers.toIndexedSeq
    members.size should ===(expectedAddresses.length)
    expectedAddresses.sorted.zipWithIndex.foreach { case (a, i) => members(i).address should ===(a) }
  }

  /**
   * Note that this can only be used for a cluster with all members
   * in Up status, i.e. use `awaitMembersUp` before using this method.
   * The reason for that is that the cluster leader is preferably a
   * member with status Up or Leaving and that information can't
   * be determined from the `RoleName`.
   */
  def assertLeader(nodesInCluster: RoleName*): Unit =
    if (nodesInCluster.contains(myself)) assertLeaderIn(nodesInCluster.to(immutable.Seq))

  /**
   * Assert that the cluster has elected the correct leader
   * out of all nodes in the cluster. First
   * member in the cluster ring is expected leader.
   *
   * Note that this can only be used for a cluster with all members
   * in Up status, i.e. use `awaitMembersUp` before using this method.
   * The reason for that is that the cluster leader is preferably a
   * member with status Up or Leaving and that information can't
   * be determined from the `RoleName`.
   */
  def assertLeaderIn(nodesInCluster: immutable.Seq[RoleName]): Unit =
    if (nodesInCluster.contains(myself)) {
      nodesInCluster.length should not be (0)
      val expectedLeader = roleOfLeader(nodesInCluster)
      val leader = clusterView.leader
      val isLeader = leader == Some(clusterView.selfAddress)
      assert(
        isLeader == isNode(expectedLeader),
        "expectedLeader [%s], got leader [%s], members [%s]".format(expectedLeader, leader, clusterView.members))
      clusterView.status should (be(MemberStatus.Up).or(be(MemberStatus.Leaving)))
    }

  /**
   * Wait until the expected number of members has status Up has been reached.
   * Also asserts that nodes in the 'canNotBePartOfMemberRing' are *not* part of the cluster ring.
   */
  def awaitMembersUp(
      numberOfMembers: Int,
      canNotBePartOfMemberRing: Set[Address] = Set.empty,
      timeout: FiniteDuration = 25.seconds): Unit = {
    within(timeout) {
      if (!canNotBePartOfMemberRing.isEmpty) // don't run this on an empty set
        awaitAssert(canNotBePartOfMemberRing.foreach(a => clusterView.members.map(_.address) should not contain (a)))
      awaitAssert(clusterView.members.size should ===(numberOfMembers))
      awaitAssert(clusterView.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up)))
      // clusterView.leader is updated by LeaderChanged, await that to be updated also
      val expectedLeader = clusterView.members.collectFirst {
        case m if m.dataCenter == cluster.settings.SelfDataCenter => m.address
      }
      awaitAssert(clusterView.leader should ===(expectedLeader))
    }
  }

  def awaitMemberRemoved(toBeRemovedAddress: Address, timeout: FiniteDuration = 25.seconds): Unit = within(timeout) {
    if (toBeRemovedAddress == cluster.selfAddress) {
      enterBarrier("registered-listener")

      cluster.leave(toBeRemovedAddress)
      enterBarrier("member-left")

      awaitCond(cluster.isTerminated, remaining)
      enterBarrier("member-shutdown")
    } else {
      val exitingLatch = TestLatch()

      val awaiter = system.actorOf(Props(new Actor {
        def receive = {
          case MemberRemoved(m, _) if m.address == toBeRemovedAddress =>
            exitingLatch.countDown()
          case _ =>
          // ignore
        }
      }).withDeploy(Deploy.local))
      cluster.subscribe(awaiter, classOf[MemberEvent])
      enterBarrier("registered-listener")

      // in the meantime member issues leave
      enterBarrier("member-left")

      // verify that the member is EXITING
      try Await.result(exitingLatch, timeout)
      catch {
        case cause: Exception =>
          throw new AssertionError(s"Member ${toBeRemovedAddress} was not removed within ${timeout}!", cause)
      }
      awaiter ! PoisonPill // you've done your job, now die

      enterBarrier("member-shutdown")
      markNodeAsUnavailable(toBeRemovedAddress)
    }

    enterBarrier("member-totally-shutdown")
  }

  def awaitAllReachable(): Unit =
    awaitAssert(clusterView.unreachableMembers should ===(Set.empty))

  /**
   * Wait until the specified nodes have seen the same gossip overview.
   */
  def awaitSeenSameState(addresses: Address*): Unit =
    awaitAssert((addresses.toSet.diff(clusterView.seenBy)) should ===(Set.empty))

  /**
   * Leader according to the address ordering of the roles.
   * Note that this can only be used for a cluster with all members
   * in Up status, i.e. use `awaitMembersUp` before using this method.
   * The reason for that is that the cluster leader is preferably a
   * member with status Up or Leaving and that information can't
   * be determined from the `RoleName`.
   */
  def roleOfLeader(nodesInCluster: immutable.Seq[RoleName] = roles): RoleName = {
    nodesInCluster.length should not be (0)
    nodesInCluster.sorted.head
  }

  /**
   * Sort the roles in the address order used by the cluster node ring.
   */
  implicit val clusterOrdering: Ordering[RoleName] = new Ordering[RoleName] {
    import Member.addressOrdering
    def compare(x: RoleName, y: RoleName) = addressOrdering.compare(address(x), address(y))
  }

  def roleName(addr: Address): Option[RoleName] = roles.find(address(_) == addr)

  /**
   * Marks a node as available in the failure detector if
   * [[akka.cluster.FailureDetectorPuppet]] is used as
   * failure detector.
   */
  def markNodeAsAvailable(address: Address): Unit =
    failureDetectorPuppet(address).foreach(_.markNodeAsAvailable())

  /**
   * Marks a node as unavailable in the failure detector if
   * [[akka.cluster.FailureDetectorPuppet]] is used as
   * failure detector.
   */
  def markNodeAsUnavailable(address: Address): Unit = {
    if (isFailureDetectorPuppet) {
      // before marking it as unavailable there should be at least one heartbeat
      // to create the FailureDetectorPuppet in the FailureDetectorRegistry
      cluster.failureDetector.heartbeat(address)
      failureDetectorPuppet(address).foreach(_.markNodeAsUnavailable())
    }
  }

  private def isFailureDetectorPuppet: Boolean =
    cluster.settings.FailureDetectorImplementationClass == classOf[FailureDetectorPuppet].getName

  private def failureDetectorPuppet(address: Address): Option[FailureDetectorPuppet] =
    cluster.failureDetector match {
      case reg: DefaultFailureDetectorRegistry[Address] =>
        reg.failureDetector(address).collect { case p: FailureDetectorPuppet => p }
      case _ => None
    }

}
