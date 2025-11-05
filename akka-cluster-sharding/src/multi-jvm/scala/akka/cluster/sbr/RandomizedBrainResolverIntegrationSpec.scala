/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.pattern.ask
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.testkit.LongRunningTest
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.util.Timeout

/*
 * Depends on akka private classes so needs to be in this package
 */
object RandomizedSplitBrainResolverIntegrationSpec extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
  val node6 = role("node6")
  val node7 = role("node7")
  val node8 = role("node8")
  val node9 = role("node9")

  commonConfig(ConfigFactory.parseString(s"""
    akka {
      loglevel = INFO
      cluster {
        downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        split-brain-resolver {
          stable-after = 10s

          active-strategy = lease-majority
          lease-majority {
            lease-implementation = test-lease
            release-after = 20s
          }
        }

        #failure-detector.acceptable-heartbeat-pause = 10s

        # speedup timeout
        sharding.handoff-timeout = 10 s

        # this is starting singleton more aggressively than default (15)
        singleton.min-number-of-hand-over-retries = 10
      }
      actor.provider = cluster
    }

    test-lease {
      lease-class = akka.cluster.sbr.SbrTestLeaseActorClient
      heartbeat-interval = 1s
      heartbeat-timeout = 120s
      lease-operation-timeout = 3s
    }

    test.random-seed = ${System.currentTimeMillis()}

    akka.testconductor.barrier-timeout = 120 s
    akka.cluster.run-coordinated-shutdown-when-down = off
    """))

  testTransport(on = true)

}

class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode1 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode2 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode3 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode4 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode5 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode6 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode7 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode8 extends RandomizedSplitBrainResolverIntegrationSpec
class RandomizedSplitBrainResolverIntegrationSpecMultiJvmNode9 extends RandomizedSplitBrainResolverIntegrationSpec

class RandomizedSplitBrainResolverIntegrationSpec
    extends MultiNodeClusterSpec(RandomizedSplitBrainResolverIntegrationSpec)
    with ImplicitSender
    with BeforeAndAfterEach {
  import GlobalRegistry._
  import GremlinController._
  import RandomizedSplitBrainResolverIntegrationSpec._

  // counter for unique naming for each test
  var c = 0
  // to be shutdown in afterEach
  var disposableSys: DisposableSys = _

  override def expectedTestDuration = 3.minutes

  object DisposableSys {
    def apply(scenario: Scenario): DisposableSys = {
      disposableSys = new DisposableSys(scenario)
      disposableSys
    }
  }

  override def afterEach(): Unit = {
    if (disposableSys ne null)
      disposableSys.shutdownSys()
  }

  class DisposableSys(scenario: Scenario) {

    c += 1

    val sys: ActorSystem = {
      val sys = ActorSystem(system.name + "-" + c, MultiNodeSpec.configureNextPortIfFixed(system.settings.config))
      val gremlinController = sys.actorOf(GremlinController.props, "gremlinController")
      system.actorOf(GremlinControllerProxy.props(gremlinController), s"gremlinControllerProxy-$c")
      sys
    }

    val singletonProbe = TestProbe()
    val shardingProbe = TestProbe()
    runOn(node1) {
      system.actorOf(GlobalRegistry.props(singletonProbe.ref, true), s"singletonRegistry-$c")
      system.actorOf(GlobalRegistry.props(shardingProbe.ref, true), s"shardingRegistry-$c")
      if (scenario.usingLease)
        system.actorOf(SbrTestLeaseActor.props, s"lease-${sys.name}")
    }
    enterBarrier("registry-started")

    system.actorSelection(node(node1) / "user" / s"singletonRegistry-$c") ! Identify(None)
    val singletonRegistry: ActorRef = expectMsgType[ActorIdentity].ref.get
    system.actorSelection(node(node1) / "user" / s"shardingRegistry-$c") ! Identify(None)
    val shardingRegistry: ActorRef = expectMsgType[ActorIdentity].ref.get

    if (scenario.usingLease) {
      system.actorSelection(node(node1) / "user" / s"lease-${sys.name}") ! Identify(None)
      val leaseRef: ActorRef = expectMsgType[ActorIdentity].ref.get
      SbrTestLeaseActorClientExt(sys).getActorLeaseClient().setActorLeaseRef(leaseRef)
    }
    enterBarrier("registry-located")

    lazy val region = ClusterSharding(sys).shardRegion(s"Entity-$c")

    def shutdownSys(): Unit = {
      TestKit.shutdownActorSystem(sys, 10.seconds, verifySystemShutdown = true)
    }

    def gremlinControllerProxy(at: RoleName): ActorRef = {
      system.actorSelection(node(at) / "user" / s"gremlinControllerProxy-$c") ! Identify(None)
      expectMsgType[ActorIdentity].ref.get
    }

    def sysAddress(at: RoleName): Address = {
      implicit val timeout: Timeout = 3.seconds
      Await.result((gremlinControllerProxy(at) ? GetAddress).mapTo[Address], timeout.duration)
    }

    def blackhole(from: RoleName, to: RoleName): Unit = {
      implicit val timeout: Timeout = 3.seconds
      import system.dispatcher
      val f = for {
        target <- (gremlinControllerProxy(to) ? GetAddress).mapTo[Address]
        done <- gremlinControllerProxy(from) ? BlackholeNode(target)
      } yield done
      Await.ready(f, timeout.duration * 2)
      log.info("Blackhole {} <-> {}", from.name, to.name)
    }

    def passThrough(from: RoleName, to: RoleName): Unit = {
      implicit val timeout: Timeout = 3.seconds
      import system.dispatcher
      val f = for {
        target <- (gremlinControllerProxy(to) ? GetAddress).mapTo[Address]
        done <- gremlinControllerProxy(from) ? PassThroughNode(target)
      } yield done
      Await.ready(f, timeout.duration * 2)
      log.info("PassThrough {} <-> {}", from.name, to.name)
    }

    def join(from: RoleName, to: RoleName, awaitUp: Boolean): Unit = {
      runOn(from) {
        Cluster(sys).join(sysAddress(to))
        createSingleton()
        startSharding()
        if (awaitUp)
          awaitMemberUp()
      }
      enterBarrier(from.name + s"-joined-$c")
    }

    def awaitMemberUp(): Unit =
      within(10.seconds) {
        awaitAssert {
          Cluster(sys).state.members.exists { m =>
            m.address == Cluster(sys).selfAddress && m.status == MemberStatus.Up
          } should be(true)
        }
      }

    def createSingleton(): ActorRef = {
      sys.actorOf(
        ClusterSingletonManager.props(
          singletonProps = SingletonActor.props(singletonRegistry),
          terminationMessage = PoisonPill,
          settings = ClusterSingletonManagerSettings(system)),
        name = "singletonRegistry")
    }

    def startSharding(): Unit = {
      ClusterSharding(sys).start(
        typeName = s"Entity-$c",
        entityProps = SingletonActor.props(shardingRegistry),
        settings = ClusterShardingSettings(system),
        extractEntityId = SingletonActor.extractEntityId,
        extractShardId = SingletonActor.extractShardId)
    }

    def verify(): Unit = {
      val nodes = roles.take(scenario.numberOfNodes)

      def sendToSharding(expectReply: Boolean): Unit = {
        runOn(nodes: _*) {
          if (!Cluster(sys).isTerminated) {
            val probe = TestProbe()(sys)
            for (i <- 0 until 10) {
              region.tell(i, probe.ref)
              if (expectReply)
                probe.expectMsg(3.seconds, i)
            }
          }
        }
      }

      runOn(nodes: _*) {
        log.info("Running {} {} in round {}", myself.name, Cluster(sys).selfUniqueAddress, c)
      }
      val randomSeed = sys.settings.config.getLong("test.random-seed")
      val random = new Random(randomSeed)
      enterBarrier(s"log-startup-$c")

      within(3.minutes) {

        join(nodes.head, nodes.head, awaitUp = true) // oldest
        join(nodes.last, nodes.head, awaitUp = true) // next oldest
        for (n <- nodes.tail.dropRight(1))
          join(n, nodes.head, awaitUp = false)
        runOn(nodes: _*) {
          awaitMemberUp()
        }
        enterBarrier(s"all-up-$c")

        singletonProbe.expectNoMessage(1.second)
        shardingProbe.expectNoMessage(10.millis)

        sendToSharding(expectReply = true)

        enterBarrier(s"initialized-$c")
        runOn(nodes: _*) {
          log.info("Initialized {} {} in round {}", myself.name, Cluster(sys).selfUniqueAddress, c)
        }

        runOn(node1) {
          val cleanSplit = random.nextBoolean()
          val healCleanSplit = cleanSplit && random.nextBoolean()
          val side1 = nodes.take(1 + random.nextInt(nodes.size - 1))
          val side2 = nodes.drop(side1.size)

          // The test is limited to one flaky step, see issue #29185.
          val numberOfFlaky = if (cleanSplit) 0 else 1
          val healLastFlaky = numberOfFlaky > 0 && random.nextBoolean()
          val flaky: Map[Int, (RoleName, List[RoleName])] =
            (0 until numberOfFlaky).map { i =>
              val from = nodes(random.nextInt(nodes.size))
              val targets = nodes.filterNot(_ == from)
              val to = (0 to random.nextInt(math.min(5, targets.size))).map(j => targets(j)).toList
              i -> (from -> to)
            }.toMap

          val delays = (0 until 10).map(_ => 2 + random.nextInt(13))

          log.info(
            s"Generated $scenario with random seed [$randomSeed] in round [$c]: " +
            s"cleanSplit [$cleanSplit], healCleanSplit [$healCleanSplit] " +
            (if (cleanSplit)
               s"side1 [${side1.map(_.name).mkString(", ")}], side2 [${side2.map(_.name).mkString(", ")}] "
             else " ") +
            s", flaky [${flaky.map { case (_, (from, to)) => from.name -> to.map(_.name).mkString("(", ", ", ")") }.mkString("; ")}] " +
            s", healLastFlaky [$healLastFlaky] " +
            s", delays [${delays.mkString(", ")}]")

          var delayIndex = 0
          def nextDelay(): Unit = {
            Thread.sleep(delays(delayIndex) * 1000)
            delayIndex += 1
          }

          if (cleanSplit) {
            for (n1 <- side1; n2 <- side2)
              blackhole(n1, n2)

            nextDelay()
          }

          flaky.foreach {
            case (i, (from, to)) =>
              if (i != 0) {
                // heal previous flakiness
                val (prevFrom, prevTo) = flaky(i - 1)
                for (n <- prevTo)
                  passThrough(prevFrom, n)
              }

              for (n <- to)
                blackhole(from, n)

              nextDelay()
          }

          if (healLastFlaky) {
            val (prevFrom, prevTo) = flaky(flaky.size - 1)
            for (n <- prevTo)
              passThrough(prevFrom, n)

            nextDelay()
          }

          if (healCleanSplit) {
            for (n1 <- side1; n2 <- side2)
              passThrough(n1, n2)
          }
        }
        enterBarrier(s"scenario-done-$c")

        runOn(nodes: _*) {
          sendToSharding(expectReply = false)
          singletonProbe.expectNoMessage(10.seconds)
          shardingProbe.expectNoMessage(10.millis)

          var loopLimit = 20
          while (loopLimit != 0 && !Cluster(sys).isTerminated && Cluster(sys).state.unreachable.nonEmpty) {
            sendToSharding(expectReply = false)
            singletonProbe.expectNoMessage(5.seconds)
            shardingProbe.expectNoMessage(10.millis)
            loopLimit -= 1
          }
        }
        enterBarrier(s"terminated-or-unreachable-removed-$c")

        runOn(nodes: _*) {
          (Cluster(sys).isTerminated || Cluster(sys).state.unreachable.isEmpty) should ===(true)
          within(30.seconds) {
            awaitAssert {
              sendToSharding(expectReply = true)
            }
          }
          singletonProbe.expectNoMessage(5.seconds)
          shardingProbe.expectNoMessage(10.millis)
          if (!Cluster(sys).isTerminated)
            log.info(s"Survived ${Cluster(sys).state.members.size} members in round $c")
        }

        enterBarrier(s"verified-$c")
      }
      enterBarrier(s"after-$c")
    }

  }

  private val leaseMajorityConfig = ConfigFactory.parseString("""akka.cluster.split-brain-resolver {
        active-strategy = lease-majority
      }""")

  case class Scenario(cfg: Config, numberOfNodes: Int) {

    val activeStrategy: String = cfg.getString("akka.cluster.split-brain-resolver.active-strategy")

    override def toString: String =
      s"Scenario($activeStrategy, $numberOfNodes)"

    def usingLease: Boolean = activeStrategy.contains("lease")
  }

  val scenarios =
    List(Scenario(leaseMajorityConfig, 3), Scenario(leaseMajorityConfig, 5), Scenario(leaseMajorityConfig, 9))

  "SplitBrainResolver with lease" must {

    for (scenario <- scenarios) {
      scenario.toString taggedAs (LongRunningTest) in {
        // temporarily disabled for aeron-udp in multi-node: https://github.com/akka/akka/pull/30706/
        val arteryConfig = system.settings.config.getConfig("akka.remote.artery")
        if (arteryConfig.getInt("canonical.port") == 6000 &&
            arteryConfig.getString("transport") == "aeron-udp") {
          pending
        }
        DisposableSys(scenario).verify()
      }
    }
  }

}
