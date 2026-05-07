/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.lang.management.ManagementFactory

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Identify
import akka.actor.Props
import akka.actor.RootActorPath
import akka.actor.Terminated
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.CurrentInternalStats
import akka.cluster.ClusterEvent.InitialStateAsSnapshot
import akka.cluster.ClusterEvent.MemberDowned
import akka.cluster.ClusterEvent.MemberEvent
import akka.remote.DefaultFailureDetectorRegistry
import akka.remote.PhiAccrualFailureDetector
import akka.remote.RARP
import akka.remote.artery.ArterySettings.AeronUpd
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._
import akka.testkit.TestEvent._
import akka.util.Helpers.ConfigOps
import akka.util.Helpers.Requiring

/**
 * This test is intended to be used as long running stress test
 * of cluster membership features. Number of nodes and duration of
 * the test steps can be configured. The test scenario is organized as
 * follows:
 * 1. join nodes in various ways up to the configured total number of nodes
 * 2. exercise concurrent joining and shutdown of nodes repeatedly
 * 3. gossip without any changes to the membership
 * 4. leave and shutdown nodes in various ways
 * 5. while nodes are removed remote death watch is also exercised
 *
 * By default it uses 13 nodes.
 * Example of sbt command line parameters to double that:
 * `-DMultiJvm.akka.cluster.Stress.nrOfNodes=26 -Dmultinode.Dakka.test.cluster-stress-spec.nr-of-nodes-factor=2`
 */
private[cluster] object StressMultiJvmSpec extends MultiNodeConfig {

  val totalNumberOfNodes =
    System.getProperty("MultiJvm.akka.cluster.Stress.nrOfNodes") match {
      case null  => 10
      case value => value.toInt.requiring(_ >= 10, "nrOfNodes should be >= 10")
    }

  for (n <- 1 to totalNumberOfNodes) role("node-" + n)

  // Note that this test uses default configuration,
  // not MultiNodeClusterSpec.clusterConfig
  commonConfig(ConfigFactory.parseString("""
    akka.test.cluster-stress-spec {
      infolog = off
      # scale the nr-of-nodes* settings with this factor
      nr-of-nodes-factor = 1
      # not scaled
      nr-of-seed-nodes = 3
      nr-of-nodes-joining-to-seed-initially = 1
      nr-of-nodes-joining-one-by-one-small = 1
      nr-of-nodes-joining-one-by-one-large = 1
      nr-of-nodes-joining-to-one = 1
      nr-of-nodes-leaving-one-by-one-small = 1
      nr-of-nodes-leaving-one-by-one-large = 1
      nr-of-nodes-leaving = 1
      nr-of-nodes-shutdown-one-by-one-small = 1
      nr-of-nodes-shutdown-one-by-one-large = 1
      nr-of-nodes-partition = 2
      nr-of-nodes-shutdown = 2
      nr-of-nodes-join-remove = 2
      # not scaled
      # scale the *-duration settings with this factor
      duration-factor = 1
      join-remove-duration = 90s
      idle-gossip-duration = 10s
      expected-test-duration = 600s
      # scale convergence within timeouts with this factor
      convergence-within-factor = 1.0
    }

    akka.actor.provider = cluster
    akka.cluster {
      failure-detector.acceptable-heartbeat-pause =  3s
      downing-provider-class = akka.cluster.sbr.SplitBrainResolverProvider
      split-brain-resolver {
          stable-after = 10s
      }
      publish-stats-interval = 1s
    }
    akka.loggers = ["akka.testkit.TestEventListener"]
    akka.loglevel = INFO
    akka.actor.default-dispatcher.fork-join-executor {
      parallelism-min = 8
      parallelism-max = 8
    }

    # test is using Java serialization and not priority to rewrite
    akka.actor.allow-java-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    """))

  testTransport(on = true)

  class Settings(conf: Config) {
    private val testConfig = conf.getConfig("akka.test.cluster-stress-spec")
    import testConfig._

    val infolog = getBoolean("infolog")
    val nFactor = getInt("nr-of-nodes-factor")
    val numberOfSeedNodes = getInt("nr-of-seed-nodes") // not scaled by nodes factor
    val numberOfNodesJoiningToSeedNodesInitially = getInt("nr-of-nodes-joining-to-seed-initially") * nFactor
    val numberOfNodesJoiningOneByOneSmall = getInt("nr-of-nodes-joining-one-by-one-small") * nFactor
    val numberOfNodesJoiningOneByOneLarge = getInt("nr-of-nodes-joining-one-by-one-large") * nFactor
    val numberOfNodesJoiningToOneNode = getInt("nr-of-nodes-joining-to-one") * nFactor
    // remaining will join to seed nodes
    val numberOfNodesJoiningToSeedNodes = (totalNumberOfNodes - numberOfSeedNodes -
    numberOfNodesJoiningToSeedNodesInitially - numberOfNodesJoiningOneByOneSmall -
    numberOfNodesJoiningOneByOneLarge - numberOfNodesJoiningToOneNode)
      .requiring(_ >= 0, s"too many configured nr-of-nodes-joining-*, total should be <= ${totalNumberOfNodes}")
    val numberOfNodesLeavingOneByOneSmall = getInt("nr-of-nodes-leaving-one-by-one-small") * nFactor
    val numberOfNodesLeavingOneByOneLarge = getInt("nr-of-nodes-leaving-one-by-one-large") * nFactor
    val numberOfNodesLeaving = getInt("nr-of-nodes-leaving") * nFactor
    val numberOfNodesShutdownOneByOneSmall = getInt("nr-of-nodes-shutdown-one-by-one-small") * nFactor
    val numberOfNodesShutdownOneByOneLarge = getInt("nr-of-nodes-shutdown-one-by-one-large") * nFactor
    val numberOfNodesShutdown = getInt("nr-of-nodes-shutdown") * nFactor
    val numberOfNodesPartition = getInt("nr-of-nodes-partition") * nFactor
    val numberOfNodesJoinRemove = getInt("nr-of-nodes-join-remove") // not scaled by nodes factor

    val dFactor = getInt("duration-factor")
    val joinRemoveDuration = testConfig.getMillisDuration("join-remove-duration") * dFactor
    val idleGossipDuration = testConfig.getMillisDuration("idle-gossip-duration") * dFactor
    val expectedTestDuration = testConfig.getMillisDuration("expected-test-duration") * dFactor
    val convergenceWithinFactor = getDouble("convergence-within-factor")

    require(
      numberOfSeedNodes + numberOfNodesJoiningToSeedNodesInitially + numberOfNodesJoiningOneByOneSmall +
      numberOfNodesJoiningOneByOneLarge + numberOfNodesJoiningToOneNode + numberOfNodesJoiningToSeedNodes <= totalNumberOfNodes,
      s"specified number of joining nodes <= ${totalNumberOfNodes}")

    // don't shutdown the 3 nodes hosting the master actors
    require(
      numberOfNodesLeavingOneByOneSmall + numberOfNodesLeavingOneByOneLarge + numberOfNodesLeaving +
      numberOfNodesShutdownOneByOneSmall + numberOfNodesShutdownOneByOneLarge + numberOfNodesShutdown <= totalNumberOfNodes - 3,
      s"specified number of leaving/shutdown nodes <= ${totalNumberOfNodes - 3}")

    require(
      numberOfNodesJoinRemove <= totalNumberOfNodes,
      s"nr-of-nodes-join-remove should be <= ${totalNumberOfNodes}")

    override def toString: String = {
      testConfig.withFallback(ConfigFactory.parseString(s"nrOfNodes=${totalNumberOfNodes}")).root.render
    }
  }

  implicit class FormattedDouble(val d: Double) extends AnyVal {
    def form: String = "%.2f".format(d)
  }

  final case class ClusterResult(address: Address, duration: Duration, clusterStats: GossipStats)

  final case class AggregatedClusterResult(title: String, duration: Duration, clusterStats: GossipStats)

  /**
   * Central aggregator of cluster statistics and metrics.
   * Reports the result via log periodically and when all
   * expected results has been collected. It shuts down
   * itself when expected results has been collected.
   */
  class ClusterResultAggregator(title: String, expectedResults: Int, settings: Settings)
      extends Actor
      with ActorLogging {
    import settings.infolog
    private var reportTo: Option[ActorRef] = None
    private var results = Vector.empty[ClusterResult]
    private var phiValuesObservedByNode = {
      import akka.cluster.Member.addressOrdering
      immutable.SortedMap.empty[Address, immutable.SortedSet[PhiValue]]
    }
    private var clusterStatsObservedByNode = {
      import akka.cluster.Member.addressOrdering
      immutable.SortedMap.empty[Address, CurrentInternalStats]
    }

    def receive = {
      case PhiResult(from, phiValues) => phiValuesObservedByNode += from -> phiValues
      case StatsResult(from, stats)   => clusterStatsObservedByNode += from -> stats
      case ReportTick =>
        if (infolog)
          log.info(s"[${title}] in progress\n\n${formatPhi}\n\n${formatStats}")
      case r: ClusterResult =>
        results :+= r
        if (results.size == expectedResults) {
          val aggregated = AggregatedClusterResult(title, maxDuration, totalGossipStats)
          if (infolog)
            log.info(
              s"[${title}] completed in [${aggregated.duration.toMillis}] ms\n${aggregated.clusterStats}\n\n${formatPhi}\n\n${formatStats}")
          reportTo.foreach { _ ! aggregated }
          context.stop(self)
        }
      case _: CurrentClusterState =>
      case ReportTo(ref)          => reportTo = ref
    }

    def maxDuration = results.map(_.duration).max

    def totalGossipStats = results.foldLeft(GossipStats()) { _ :+ _.clusterStats }

    def format(opt: Option[Double]) = opt match {
      case None    => "N/A"
      case Some(x) => x.form
    }

    def formatPhi: String = {
      if (phiValuesObservedByNode.isEmpty) ""
      else {
        val lines =
          for {
            (monitor, phiValues) <- phiValuesObservedByNode
            phi <- phiValues
          } yield formatPhiLine(monitor, phi.address, phi)

        lines.mkString(formatPhiHeader + "\n", "\n", "")
      }
    }

    def formatPhiHeader: String = "[Monitor]\t[Subject]\t[count]\t[count phi > 1.0]\t[max phi]"

    def formatPhiLine(monitor: Address, subject: Address, phi: PhiValue): String =
      s"${monitor}\t${subject}\t${phi.count}\t${phi.countAboveOne}\t${phi.max.form}"

    def formatStats: String = {
      def f(stats: CurrentInternalStats) = {
        import stats.gossipStats._
        import stats.vclockStats._
        s"ClusterStats($receivedGossipCount, $mergeCount, $sameCount, $newerCount, $olderCount, $versionSize, $seenLatest)"
      }
      clusterStatsObservedByNode
        .map { case (monitor, stats) => s"${monitor}\t${f(stats)}" }
        .mkString("ClusterStats(gossip, merge, same, newer, older, vclockSize, seenLatest)\n", "\n", "")
    }

  }

  /**
   * Keeps cluster statistics and metrics reported by
   * ClusterResultAggregator. Logs the list of historical
   * results when a new AggregatedClusterResult is received.
   */
  class ClusterResultHistory extends Actor with ActorLogging {
    var history = Vector.empty[AggregatedClusterResult]

    def receive = {
      case result: AggregatedClusterResult =>
        history :+= result
        log.info("Cluster result history\n" + formatHistory)
    }

    def formatHistory: String =
      (formatHistoryHeader +: (history.map(formatHistoryLine))).mkString("\n")

    def formatHistoryHeader: String = "[Title]\t[Duration (ms)]\t[GossipStats(gossip, merge, same, newer, older)]"

    def formatHistoryLine(result: AggregatedClusterResult): String =
      s"${result.title}\t${result.duration.toMillis}\t${result.clusterStats}"

  }

  /**
   * Collect phi values of the failure detector and report to the
   * central ClusterResultAggregator.
   */
  class PhiObserver extends Actor with ActorLogging {
    val cluster = Cluster(context.system)
    var reportTo: Option[ActorRef] = None
    val emptyPhiByNode: Map[Address, PhiValue] =
      Map.empty[Address, PhiValue].withDefault(address => PhiValue(address, 0, 0, 0.0))
    var phiByNode = emptyPhiByNode
    var nodes = Set.empty[Address]

    def phi(address: Address): Double = cluster.failureDetector match {
      case reg: DefaultFailureDetectorRegistry[Address] =>
        reg.failureDetector(address) match {
          case Some(fd: PhiAccrualFailureDetector) => fd.phi
          case _                                   => 0.0
        }
      case _ => 0.0
    }

    import context.dispatcher
    val checkPhiTask = context.system.scheduler.scheduleWithFixedDelay(1.second, 1.second, self, PhiTick)

    // subscribe to MemberEvent, re-subscribe when restart
    override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])
    override def postStop(): Unit = {
      cluster.unsubscribe(self)
      checkPhiTask.cancel()
      super.postStop()
    }

    def receive = {
      case PhiTick =>
        nodes.foreach { node =>
          val previous = phiByNode(node)
          val φ = phi(node)
          if (φ > 0 || cluster.failureDetector.isMonitoring(node)) {
            val aboveOne = if (!φ.isInfinite && φ > 1.0) 1 else 0
            phiByNode += node -> PhiValue(
              node,
              previous.countAboveOne + aboveOne,
              previous.count + 1,
              math.max(previous.max, φ))
          }
        }
        val phiSet = immutable.SortedSet.empty[PhiValue] ++ phiByNode.values
        reportTo.foreach { _ ! PhiResult(cluster.selfAddress, phiSet) }
      case state: CurrentClusterState => nodes = state.members.map(_.address)
      case memberEvent: MemberEvent   => nodes += memberEvent.member.address
      case ReportTo(ref) =>
        reportTo.foreach(context.unwatch)
        reportTo = ref
        reportTo.foreach(context.watch)
      case Terminated(ref) =>
        reportTo match {
          case Some(`ref`) => reportTo = None
          case _           =>
        }
      case Reset =>
        phiByNode = emptyPhiByNode
        nodes = Set.empty[Address]
        cluster.unsubscribe(self)
        cluster.subscribe(self, classOf[MemberEvent])

    }
  }

  class StatsObserver extends Actor {
    private val cluster = Cluster(context.system)
    private var reportTo: Option[ActorRef] = None
    private var startStats: Option[GossipStats] = None

    override def preStart(): Unit = cluster.subscribe(self, classOf[CurrentInternalStats])
    override def postStop(): Unit = cluster.unsubscribe(self)

    def receive = {
      case CurrentInternalStats(gossipStats, vclockStats) =>
        val diff = startStats match {
          case None        => { startStats = Some(gossipStats); gossipStats }
          case Some(start) => gossipStats :- start
        }
        val res = StatsResult(cluster.selfAddress, CurrentInternalStats(diff, vclockStats))
        reportTo.foreach { _ ! res }
      case ReportTo(ref) =>
        reportTo.foreach(context.unwatch)
        reportTo = ref
        reportTo.foreach(context.watch)
      case Terminated(ref) =>
        reportTo match {
          case Some(`ref`) => reportTo = None
          case _           =>
        }
      case Reset =>
        startStats = None
      case _: CurrentClusterState => // not interesting here
    }
  }

  /**
   * Used for remote death watch testing
   */
  class Watchee extends Actor {
    def receive = Actor.emptyBehavior
  }

  case object Begin
  case object End
  case object RetryTick
  case object ReportTick
  case object PhiTick
  final case class PhiResult(from: Address, phiValues: immutable.SortedSet[PhiValue])
  final case class PhiValue(address: Address, countAboveOne: Int, count: Int, max: Double) extends Ordered[PhiValue] {
    import akka.cluster.Member.addressOrdering
    def compare(that: PhiValue) = addressOrdering.compare(this.address, that.address)
  }
  final case class ReportTo(ref: Option[ActorRef])
  final case class StatsResult(from: Address, stats: CurrentInternalStats)
  case object Reset

  class MeasureDurationUntilDown extends Actor with ActorLogging {
    private val startTime = System.nanoTime()
    private val cluster = Cluster(context.system)
    cluster.subscribe(self, InitialStateAsSnapshot, classOf[MemberDowned])

    override def receive: Receive = {
      case MemberDowned(m) =>
        if (m.uniqueAddress == cluster.selfUniqueAddress)
          log.info("Downed [{}] after [{} ms]", cluster.selfAddress, (System.nanoTime() - startTime).nanos.toMillis)
      case _: CurrentClusterState =>
    }
  }

}

class StressMultiJvmNode1 extends StressSpec
class StressMultiJvmNode2 extends StressSpec
class StressMultiJvmNode3 extends StressSpec
class StressMultiJvmNode4 extends StressSpec
class StressMultiJvmNode5 extends StressSpec
class StressMultiJvmNode6 extends StressSpec
class StressMultiJvmNode7 extends StressSpec
class StressMultiJvmNode8 extends StressSpec
class StressMultiJvmNode9 extends StressSpec
class StressMultiJvmNode10 extends StressSpec

abstract class StressSpec extends MultiNodeClusterSpec(StressMultiJvmSpec) with BeforeAndAfterEach with ImplicitSender {

  import StressMultiJvmSpec._

  val settings = new Settings(system.settings.config)
  import settings._

  val identifyProbe = TestProbe()

  var step = 0
  var nbrUsedRoles = 0

  override def beforeEach(): Unit = { step += 1 }

  override def expectedTestDuration: FiniteDuration = settings.expectedTestDuration

  override def shutdownTimeout: FiniteDuration = 30.seconds.dilated

  override def verifySystemShutdown: Boolean = true

  override def muteLog(sys: ActorSystem = system): Unit = {
    super.muteLog(sys)
    sys.eventStream.publish(Mute(EventFilter[RuntimeException](pattern = ".*Simulated exception.*")))
    muteDeadLetters(classOf[AggregatedClusterResult], classOf[StatsResult], classOf[PhiResult], RetryTick.getClass)(sys)
  }

  override protected def afterTermination(): Unit = {
    SharedMediaDriverSupport.stopMediaDriver(StressMultiJvmSpec)
    super.afterTermination()
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      if (SharedMediaDriverSupport.isMediaDriverRunningByThisNode)
        println("Abrupt exit of JVM without closing media driver. This should not happen and may cause test failure.")
    }
  })

  def isAeronUdpTransport: Boolean = RARP(system).provider.remoteSettings.Artery.Transport == AeronUpd

  def jvmInfo(): String = {
    val runtime = ManagementFactory.getRuntimeMXBean
    val os = ManagementFactory.getOperatingSystemMXBean
    val threads = ManagementFactory.getThreadMXBean
    val mem = ManagementFactory.getMemoryMXBean
    val heap = mem.getHeapMemoryUsage

    val sb = new StringBuilder

    sb.append("Operating system: ")
      .append(os.getName)
      .append(", ")
      .append(os.getArch)
      .append(", ")
      .append(os.getVersion)
    sb.append("\n")
    sb.append("JVM: ")
      .append(runtime.getVmName)
      .append(" ")
      .append(runtime.getVmVendor)
      .append(" ")
      .append(runtime.getVmVersion)
    sb.append("\n")
    sb.append("Processors: ").append(os.getAvailableProcessors)
    sb.append("\n")
    sb.append("Load average: ").append(os.getSystemLoadAverage)
    sb.append("\n")
    sb.append("Thread count: ")
      .append(threads.getThreadCount)
      .append(" (")
      .append(threads.getPeakThreadCount)
      .append(")")
    sb.append("\n")
    sb.append("Heap: ")
      .append((heap.getUsed.toDouble / 1024 / 1024).form)
      .append(" (")
      .append((heap.getInit.toDouble / 1024 / 1024).form)
      .append(" - ")
      .append((heap.getMax.toDouble / 1024 / 1024).form)
      .append(")")
      .append(" MB")
    sb.append("\n")

    import scala.jdk.CollectionConverters._
    val args = runtime.getInputArguments.asScala.filterNot(_.contains("classpath")).mkString("\n  ")
    sb.append("Args:\n  ").append(args)
    sb.append("\n")

    sb.toString
  }

  val seedNodes = roles.take(numberOfSeedNodes)

  def latestGossipStats = cluster.readView.latestStats.gossipStats

  override def cluster: Cluster = {
    super.cluster
  }

  def createResultAggregator(title: String, expectedResults: Int, includeInHistory: Boolean): Unit = {
    runOn(roles.head) {
      val aggregator = system.actorOf(
        Props(classOf[ClusterResultAggregator], title, expectedResults, settings).withDeploy(Deploy.local),
        name = "result" + step)
      if (includeInHistory && infolog) aggregator ! ReportTo(Some(clusterResultHistory))
      else aggregator ! ReportTo(None)
    }
    enterBarrier("result-aggregator-created-" + step)
    runOn(roles.take(nbrUsedRoles): _*) {
      val resultAggregator = identifyClusterResultAggregator()
      phiObserver ! ReportTo(resultAggregator)
      statsObserver ! Reset
      statsObserver ! ReportTo(resultAggregator)
    }
    enterBarrier("result-aggregator-identified-" + step)
  }

  def identifyClusterResultAggregator(): Option[ActorRef] = {
    system.actorSelection(node(roles.head) / "user" / ("result" + step)).tell(Identify(step), identifyProbe.ref)
    identifyProbe.expectMsgType[ActorIdentity].ref
  }

  lazy val clusterResultHistory =
    if (settings.infolog) system.actorOf(Props[ClusterResultHistory](), "resultHistory")
    else system.deadLetters

  lazy val phiObserver = system.actorOf(Props[PhiObserver](), "phiObserver")

  lazy val statsObserver = system.actorOf(Props[StatsObserver](), "statsObserver")

  def awaitClusterResult(): Unit = {
    runOn(roles.head) {
      identifyClusterResultAggregator() match {
        case Some(r) =>
          watch(r)
          expectMsgPF() { case Terminated(ac) if ac.path == r.path => true }
        case None => // ok, already terminated
      }
    }
    enterBarrier("cluster-result-done-" + step)
  }

  def joinOneByOne(numberOfNodes: Int): Unit = {
    (0 until numberOfNodes).foreach { _ =>
      joinOne()
      nbrUsedRoles += 1
      step += 1
    }
  }

  def convergenceWithin(base: FiniteDuration, nodes: Int): FiniteDuration =
    (base.toMillis * convergenceWithinFactor * nodes).millis

  def joinOne(): Unit = within(5.seconds + convergenceWithin(2.seconds, nbrUsedRoles + 1)) {
    val currentRoles = roles.take(nbrUsedRoles + 1)
    val title = s"join one to ${nbrUsedRoles} nodes cluster"
    createResultAggregator(title, expectedResults = currentRoles.size, includeInHistory = true)
    runOn(currentRoles: _*) {
      reportResult {
        runOn(currentRoles.last) {
          cluster.join(roles.head)
        }
        awaitMembersUp(currentRoles.size, timeout = remainingOrDefault)
      }

    }
    awaitClusterResult()
    enterBarrier("join-one-" + step)
  }

  def joinSeveral(numberOfNodes: Int, toSeedNodes: Boolean): Unit =
    within(10.seconds + convergenceWithin(3.seconds, nbrUsedRoles + numberOfNodes)) {
      val currentRoles = roles.take(nbrUsedRoles + numberOfNodes)
      val joiningRoles = currentRoles.takeRight(numberOfNodes)
      val title =
        s"join ${numberOfNodes} to ${if (toSeedNodes) "seed nodes" else "one node"}, in ${nbrUsedRoles} nodes cluster"
      createResultAggregator(title, expectedResults = currentRoles.size, includeInHistory = true)
      runOn(currentRoles: _*) {
        reportResult {
          runOn(joiningRoles: _*) {
            if (toSeedNodes) cluster.joinSeedNodes(seedNodes.toIndexedSeq.map(address))
            else cluster.join(roles.head)
          }
          awaitMembersUp(currentRoles.size, timeout = remainingOrDefault)
        }

      }
      awaitClusterResult()
      enterBarrier("join-several-" + step)
    }

  def removeOneByOne(numberOfNodes: Int, shutdown: Boolean): Unit = {
    (0 until numberOfNodes).foreach { _ =>
      removeOne(shutdown)
      nbrUsedRoles -= 1
      step += 1
    }
  }

  def removeOne(shutdown: Boolean): Unit = within(25.seconds + convergenceWithin(3.seconds, nbrUsedRoles - 1)) {
    val currentRoles = roles.take(nbrUsedRoles - 1)
    val title = s"${if (shutdown) "shutdown" else "remove"} one from ${nbrUsedRoles} nodes cluster"
    createResultAggregator(title, expectedResults = currentRoles.size, includeInHistory = true)
    val removeRole = roles(nbrUsedRoles - 1)
    val removeAddress = address(removeRole)
    runOn(removeRole) {
      system.actorOf(Props[Watchee](), "watchee")
      if (!shutdown) cluster.leave(myself)
    }
    enterBarrier("watchee-created-" + step)
    runOn(roles.head) {
      system.actorSelection(node(removeRole) / "user" / "watchee").tell(Identify("watchee"), identifyProbe.ref)
      val watchee = identifyProbe.expectMsgType[ActorIdentity].ref.get
      watch(watchee)
    }
    enterBarrier("watch-estabilished-" + step)

    runOn(currentRoles: _*) {
      reportResult {
        runOn(roles.head) {
          if (shutdown) {
            if (infolog)
              log.info("Shutting down [{}]", removeAddress)
            testConductor.exit(removeRole, 0).await
          }
        }
        awaitMembersUp(currentRoles.size, timeout = remainingOrDefault)
        awaitAllReachable()
      }
    }

    runOn(roles.head) {
      val expectedPath = RootActorPath(removeAddress) / "user" / "watchee"
      expectMsgPF() {
        case Terminated(ac) if ac.path == expectedPath => true
      }
    }
    enterBarrier("watch-verified-" + step)

    awaitClusterResult()
    enterBarrier("remove-one-" + step)
  }

  def removeSeveral(numberOfNodes: Int, shutdown: Boolean): Unit =
    within(25.seconds + convergenceWithin(5.seconds, nbrUsedRoles - numberOfNodes)) {
      val currentRoles = roles.take(nbrUsedRoles - numberOfNodes)
      val removeRoles = roles.slice(currentRoles.size, nbrUsedRoles)
      val title = s"${if (shutdown) "shutdown" else "leave"} ${numberOfNodes} in ${nbrUsedRoles} nodes cluster"
      createResultAggregator(title, expectedResults = currentRoles.size, includeInHistory = true)
      runOn(removeRoles: _*) {
        if (!shutdown) cluster.leave(myself)
      }
      runOn(currentRoles: _*) {
        reportResult {
          runOn(roles.head) {
            if (shutdown) removeRoles.foreach { r =>
              if (infolog)
                log.info("Shutting down [{}]", address(r))
              testConductor.exit(r, 0).await
            }
          }
          awaitMembersUp(currentRoles.size, timeout = remainingOrDefault)
          awaitAllReachable()
        }
      }
      awaitClusterResult()
      enterBarrier("remove-several-" + step)
    }

  def partitionSeveral(numberOfNodes: Int): Unit =
    within(25.seconds + convergenceWithin(5.seconds, nbrUsedRoles - numberOfNodes)) {
      val currentRoles = roles.take(nbrUsedRoles - numberOfNodes)
      val removeRoles = roles.slice(currentRoles.size, nbrUsedRoles)
      val title = s"partition ${numberOfNodes} in ${nbrUsedRoles} nodes cluster"
      createResultAggregator(title, expectedResults = currentRoles.size, includeInHistory = true)

      runOn(roles.head) {
        for (x <- currentRoles; y <- removeRoles) {
          testConductor.blackhole(x, y, Direction.Both).await
        }
      }
      enterBarrier("partition-several-blackhole")

      runOn(currentRoles: _*) {
        reportResult {
          val startTime = System.nanoTime()
          awaitMembersUp(currentRoles.size, timeout = remainingOrDefault)
          system.log.info(
            "Removed [{}] members after [{} ms].",
            removeRoles.size,
            (System.nanoTime() - startTime).nanos.toMillis)
          awaitAllReachable()
        }
      }
      runOn(removeRoles: _*) {
        system.actorOf(Props[MeasureDurationUntilDown]())
        awaitAssert {
          cluster.isTerminated should ===(true)
        }
      }
      awaitClusterResult()
      enterBarrier("partition-several-" + step)
    }

  def reportResult[T](thunk: => T): T = {
    val startTime = System.nanoTime
    val startStats = clusterView.latestStats.gossipStats

    val returnValue = thunk

    identifyClusterResultAggregator().foreach {
      _ ! ClusterResult(cluster.selfAddress, (System.nanoTime - startTime).nanos, latestGossipStats :- startStats)
    }

    returnValue
  }

  def exerciseJoinRemove(title: String, duration: FiniteDuration): Unit = {
    val activeRoles = roles.take(numberOfNodesJoinRemove)
    val loopDuration = 10.seconds + convergenceWithin(4.seconds, nbrUsedRoles + activeRoles.size)
    val rounds = ((duration - loopDuration).toMillis / loopDuration.toMillis).max(1).toInt
    val usedRoles = roles.take(nbrUsedRoles)
    val usedAddresses = usedRoles.map(address(_)).toSet

    @tailrec def loop(
        counter: Int,
        previousAS: Option[ActorSystem],
        allPreviousAddresses: Set[Address]): Option[ActorSystem] = {
      if (counter > rounds) previousAS
      else {
        val t = title + " round " + counter
        runOn(usedRoles: _*) {
          phiObserver ! Reset
          statsObserver ! Reset
        }
        createResultAggregator(t, expectedResults = nbrUsedRoles, includeInHistory = true)
        val (nextAS, nextAddresses) = within(loopDuration) {
          reportResult {
            val nextAS =
              if (activeRoles contains myself) {
                previousAS.foreach { as =>
                  TestKit.shutdownActorSystem(as)
                }
                val sys = ActorSystem(system.name, MultiNodeSpec.configureNextPortIfFixed(system.settings.config))
                muteLog(sys)
                Cluster(sys).joinSeedNodes(seedNodes.toIndexedSeq.map(address))
                Some(sys)
              } else previousAS
            runOn(usedRoles: _*) {
              awaitMembersUp(
                nbrUsedRoles + activeRoles.size,
                canNotBePartOfMemberRing = allPreviousAddresses,
                timeout = remainingOrDefault)
              awaitAllReachable()
            }
            val nextAddresses = clusterView.members.map(_.address).diff(usedAddresses)
            runOn(usedRoles: _*) {
              nextAddresses.size should ===(numberOfNodesJoinRemove)
            }

            enterBarrier("join-remove-" + step)
            (nextAS, nextAddresses)
          }
        }
        awaitClusterResult()

        step += 1
        loop(counter + 1, nextAS, nextAddresses)
      }
    }

    loop(1, None, Set.empty).foreach { as =>
      TestKit.shutdownActorSystem(as)
    }
    within(loopDuration) {
      runOn(usedRoles: _*) {
        awaitMembersUp(nbrUsedRoles, timeout = remainingOrDefault)
        awaitAllReachable()
        phiObserver ! Reset
        statsObserver ! Reset
      }
    }
    enterBarrier("join-remove-shutdown-" + step)

  }

  def idleGossip(title: String): Unit = {
    createResultAggregator(title, expectedResults = nbrUsedRoles, includeInHistory = true)
    reportResult {
      clusterView.members.size should ===(nbrUsedRoles)
      Thread.sleep(idleGossipDuration.toMillis)
      clusterView.members.size should ===(nbrUsedRoles)
    }
    awaitClusterResult()
  }

  "A cluster under stress" must {

    "log settings" taggedAs LongRunningTest in {
      if (infolog) {
        log.info("StressSpec JVM:\n{}", jvmInfo())
        runOn(roles.head) {
          log.info("StressSpec settings:\n{}", settings)
        }
      }
      enterBarrier("after-" + step)
    }

    // Aeron UDP with embedded driver seems too heavy to get to pass
    // note: there must be one test step before pending, otherwise afterTermination will not run
    if (isAeronUdpTransport) pending

    "join seed nodes" taggedAs LongRunningTest in within(30 seconds) {

      val otherNodesJoiningSeedNodes =
        roles.slice(numberOfSeedNodes, numberOfSeedNodes + numberOfNodesJoiningToSeedNodesInitially)
      val size = seedNodes.size + otherNodesJoiningSeedNodes.size

      createResultAggregator("join seed nodes", expectedResults = size, includeInHistory = true)

      runOn((seedNodes ++ otherNodesJoiningSeedNodes): _*) {
        reportResult {
          cluster.joinSeedNodes(seedNodes.toIndexedSeq.map(address))
          awaitMembersUp(size, timeout = remainingOrDefault)
        }
      }

      awaitClusterResult()

      nbrUsedRoles += size
      enterBarrier("after-" + step)
    }

    "join nodes one-by-one to small cluster" taggedAs LongRunningTest in {
      joinOneByOne(numberOfNodesJoiningOneByOneSmall)
      enterBarrier("after-" + step)
    }

    "join several nodes to one node" taggedAs LongRunningTest in {
      joinSeveral(numberOfNodesJoiningToOneNode, toSeedNodes = false)
      nbrUsedRoles += numberOfNodesJoiningToOneNode
      enterBarrier("after-" + step)
    }

    "join several nodes to seed nodes" taggedAs LongRunningTest in {
      if (numberOfNodesJoiningToSeedNodes > 0) {
        joinSeveral(numberOfNodesJoiningToSeedNodes, toSeedNodes = true)
        nbrUsedRoles += numberOfNodesJoiningToSeedNodes
      }
      enterBarrier("after-" + step)
    }

    "join nodes one-by-one to large cluster" taggedAs LongRunningTest in {
      joinOneByOne(numberOfNodesJoiningOneByOneLarge)
      enterBarrier("after-" + step)
    }

    "exercise join/remove/join/remove" taggedAs LongRunningTest in {
      exerciseJoinRemove("exercise join/remove", joinRemoveDuration)
      enterBarrier("after-" + step)
    }

    "gossip when idle" taggedAs LongRunningTest in {
      idleGossip("idle gossip")
      enterBarrier("after-" + step)
    }

    "down partitioned nodes" taggedAs LongRunningTest in {
      partitionSeveral(numberOfNodesPartition)
      nbrUsedRoles -= numberOfNodesPartition
      enterBarrier("after-" + step)
    }

    "leave nodes one-by-one from large cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesLeavingOneByOneLarge, shutdown = false)
      enterBarrier("after-" + step)
    }

    "shutdown nodes one-by-one from large cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesShutdownOneByOneLarge, shutdown = true)
      enterBarrier("after-" + step)
    }

    "leave several nodes" taggedAs LongRunningTest in {
      removeSeveral(numberOfNodesLeaving, shutdown = false)
      nbrUsedRoles -= numberOfNodesLeaving
      enterBarrier("after-" + step)
    }

    "shutdown several nodes" taggedAs LongRunningTest in {
      removeSeveral(numberOfNodesShutdown, shutdown = true)
      nbrUsedRoles -= numberOfNodesShutdown
      enterBarrier("after-" + step)
    }

    "shutdown nodes one-by-one from small cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesShutdownOneByOneSmall, shutdown = true)
      enterBarrier("after-" + step)
    }

    "leave nodes one-by-one from small cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesLeavingOneByOneSmall, shutdown = false)
      enterBarrier("after-" + step)
    }

    "log jvm info" taggedAs LongRunningTest in {
      if (infolog) {
        log.info("StressSpec JVM:\n{}", jvmInfo())
      }
      enterBarrier("after-" + step)
    }
  }
}
