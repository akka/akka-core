/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import org.scalatest.BeforeAndAfterEach

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.ClusterSettings.DefaultDataCenter
import akka.cluster.Member
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
object SplitBrainResolverIntegrationSpec extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
  val node6 = role("node6")
  val node7 = role("node7")
  val node8 = role("node8")
  val node9 = role("node9")

  commonConfig(ConfigFactory.parseString("""
    akka {
      loglevel = INFO
      cluster {
        downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        split-brain-resolver.active-strategy = keep-majority
        split-brain-resolver.stable-after = 10s

        sharding.handoff-timeout = 5s
      }

      actor.provider = cluster
    }

    akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    akka.coordinated-shutdown.terminate-actor-system = off
    akka.cluster.run-coordinated-shutdown-when-down = off
    """))

  testTransport(on = true)

}

class SplitBrainResolverIntegrationSpecMultiJvmNode1 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode2 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode3 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode4 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode5 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode6 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode7 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode8 extends SplitBrainResolverIntegrationSpec
class SplitBrainResolverIntegrationSpecMultiJvmNode9 extends SplitBrainResolverIntegrationSpec

class SplitBrainResolverIntegrationSpec
    extends MultiNodeClusterSpec(SplitBrainResolverIntegrationSpec)
    with ImplicitSender
    with BeforeAndAfterEach {
  import GlobalRegistry._
  import GremlinController._
  import SplitBrainResolverIntegrationSpec._

  override def initialParticipants = roles.size

  override def afterEach(): Unit = {
    if (disposableSys ne null)
      disposableSys.shutdownSys()
  }

  // counter for unique naming for each test
  var c = 0
  // to be shutdown in afterEach
  var disposableSys: DisposableSys = _

  override def expectedTestDuration = 10.minutes

  object DisposableSys {
    def apply(scenario: Scenario): DisposableSys = {
      disposableSys = new DisposableSys(scenario)
      disposableSys
    }
  }

  class DisposableSys(scenario: Scenario) {

    c += 1

    val sys: ActorSystem = {
      val dcName = scenario.dcDecider(myself)

      val sys = ActorSystem(
        system.name + "-" + c,
        MultiNodeSpec.configureNextPortIfFixed(
          scenario.cfg
            .withValue("akka.cluster.multi-data-center.self-data-center", ConfigValueFactory.fromAnyRef(dcName))
            .withFallback(system.settings.config)))
      val gremlinController = sys.actorOf(GremlinController.props, "gremlinController")
      system.actorOf(GremlinControllerProxy.props(gremlinController), s"gremlinControllerProxy-$c")
      sys
    }

    val singletonProbe = TestProbe()
    val shardingProbe = TestProbe()
    runOn(node1) {
      system.actorOf(GlobalRegistry.props(singletonProbe.ref, false), s"singletonRegistry-$c")
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

    def awaitAllMembersUp(nodes: RoleName*): Unit = {
      val addresses = nodes.map(sysAddress).toSet
      within(15.seconds) {
        awaitAssert {
          Cluster(sys).state.members.map(_.address) should ===(addresses)
          Cluster(sys).state.members.foreach {
            _.status should ===(MemberStatus.Up)
          }
        }
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
      val side1 = roles.take(scenario.side1Size)
      val side2 = roles.drop(scenario.side1Size).take(scenario.side2Size)

      def singletonRegisterKey(node: RoleName): String =
        "/user/singletonRegistry/singleton-" + scenario.dcDecider(node)

      runOn(side1 ++ side2: _*) {
        log.info("Running {} {} in round {}", myself.name, Cluster(sys).selfUniqueAddress, c)
      }
      enterBarrier(s"log-startup-$c")

      within(90.seconds) {

        join(side1.head, side1.head, awaitUp = true) // oldest
        join(side2.head, side1.head, awaitUp = true) // next oldest
        for (n <- side1.tail ++ side2.tail)
          join(n, side1.head, awaitUp = false)
        runOn(side1 ++ side2: _*) {
          awaitAllMembersUp(side1 ++ side2: _*)
        }
        enterBarrier(s"all-up-$c")

        runOn(node1) {
          singletonProbe.within(25.seconds) {
            singletonProbe.expectMsg(Register(singletonRegisterKey(node1), sysAddress(node1)))
          }
          shardingProbe.expectNoMessage(100.millis)
        }

        runOn(side1 ++ side2: _*) {
          val probe = TestProbe()(sys)
          for (i <- 0 until 10) {
            region.tell(i, probe.ref)
            probe.expectMsg(5.seconds, i)
          }
        }

        enterBarrier(s"initialized-$c")
        runOn(side1 ++ side2: _*) {
          log.info("Initialized {} {} in round {}", myself.name, Cluster(sys).selfUniqueAddress, c)
        }

        runOn(node1) {
          for (n1 <- side1; n2 <- side2)
            blackhole(n1, n2)
        }
        enterBarrier(s"blackhole-$c")

        val resolvedExpected = scenario.expected match {
          case KeepLeader =>
            import Member.addressOrdering
            val address = (side1 ++ side2).map(sysAddress).min
            if (side1.exists(sysAddress(_) == address)) KeepSide1
            else if (side2.exists(sysAddress(_) == address)) KeepSide2
            else ShutdownBoth
          case other => other
        }

        resolvedExpected match {
          case ShutdownBoth =>
            runOn(side1 ++ side2: _*) {
              awaitCond(Cluster(sys).isTerminated, max = 30.seconds)
            }
            enterBarrier(s"sys-terminated-$c")
            runOn(node1) {
              singletonProbe.within(20.seconds) {
                singletonProbe.expectMsg(Unregister(singletonRegisterKey(side1.head), sysAddress(side1.head)))
              }
              shardingProbe.expectNoMessage(100.millis)
            }

          case KeepSide1 =>
            runOn(side1: _*) {
              val expectedAddresses = side1.map(sysAddress).toSet
              within(remaining - 3.seconds) {
                awaitAssert {
                  val probe = TestProbe()(sys)
                  for (i <- 0 until 10) {
                    region.tell(i, probe.ref)
                    probe.expectMsg(2.seconds, i)
                  }

                  Cluster(sys).state.members.map(_.address) should be(expectedAddresses)
                }
              }
            }
            runOn(side2: _*) {
              awaitCond(Cluster(sys).isTerminated, max = 30.seconds)
            }
            enterBarrier(s"cluster-shutdown-verified-$c")
            singletonProbe.expectNoMessage(1.second)
            shardingProbe.expectNoMessage(100.millis)

          case KeepSide2 =>
            runOn(side1: _*) {
              awaitCond(Cluster(sys).isTerminated, max = 30.seconds)
            }
            enterBarrier(s"sys-terminated-$c")
            runOn(node1) {
              singletonProbe.within(30.seconds) {
                singletonProbe.expectMsg(Unregister(singletonRegisterKey(side1.head), sysAddress(side1.head)))
                singletonProbe.expectMsg(Register(singletonRegisterKey(side2.head), sysAddress(side2.head)))
              }
              shardingProbe.expectNoMessage(100.millis)
            }
            runOn(side2: _*) {
              val expectedAddresses = side2.map(sysAddress).toSet
              within(remaining - 3.seconds) {
                awaitAssert {
                  val probe = TestProbe()(sys)
                  for (i <- 0 until 10) {
                    region.tell(i, probe.ref)
                    probe.expectMsg(2.seconds, i)
                  }

                  Cluster(sys).state.members.map(_.address) should be(expectedAddresses)
                }
              }
            }

          case KeepAll =>
            runOn((side1 ++ side2): _*) {
              val expectedAddresses = (side1 ++ side2).map(sysAddress).toSet
              within(remaining - 3.seconds) {
                awaitAssert {
                  val probe = TestProbe()(sys)
                  for (i <- 0 until 10) {
                    region.tell(i, probe.ref)
                    probe.expectMsg(2.seconds, i)
                  }

                  Cluster(sys).state.members.map(_.address) should be(expectedAddresses)
                }
              }
              Cluster(sys).isTerminated should be(false)
            }
            enterBarrier(s"cluster-intact-verified-$c")

          case KeepLeader => throw new IllegalStateException // already resolved to other case
        }

        enterBarrier(s"verified-$c")
      }
      enterBarrier(s"after-$c")
    }

  }

  private val staticQuorumConfig = ConfigFactory.parseString("""akka.cluster.split-brain-resolver {
        active-strategy = static-quorum
        static-quorum.quorum-size = 5
      }""")

  private val keepMajorityConfig = ConfigFactory.parseString("""akka.cluster.split-brain-resolver {
        active-strategy = keep-majority
      }""")
  private val keepOldestConfig = ConfigFactory.parseString("""akka.cluster.split-brain-resolver {
        active-strategy = keep-oldest
      }""")
  private val downAllConfig = ConfigFactory.parseString("""akka.cluster.split-brain-resolver {
        active-strategy = down-all
      }""")
  private val leaseMajorityConfig = ConfigFactory.parseString("""akka.cluster.split-brain-resolver {
        active-strategy = lease-majority
        lease-majority {
          lease-implementation = test-lease
          acquire-lease-delay-for-minority = 3s
          release-after = 20s          
        }
      }
      test-lease {
        lease-class = akka.cluster.sbr.SbrTestLeaseActorClient
        heartbeat-interval = 1s
        heartbeat-timeout = 120s
        lease-operation-timeout = 3s
      }
      """)

  sealed trait Expected
  case object KeepSide1 extends Expected
  case object KeepSide2 extends Expected
  case object ShutdownBoth extends Expected
  case object KeepLeader extends Expected
  case object KeepAll extends Expected

  val defaultDcDecider: RoleName => DataCenter = _ => DefaultDataCenter

  case class Scenario(
      cfg: Config,
      side1Size: Int,
      side2Size: Int,
      expected: Expected,
      dcDecider: RoleName => DataCenter = defaultDcDecider // allows to set the dc per indexed node
  ) {

    val activeStrategy: String = cfg.getString("akka.cluster.split-brain-resolver.active-strategy")

    override def toString: String = {
      s"$expected when using $activeStrategy and side1=$side1Size and side2=$side2Size" +
      (if (dcDecider ne defaultDcDecider) "with multi-DC" else "")
    }

    def usingLease: Boolean = activeStrategy.contains("lease")
  }

  val scenarios = List(
    Scenario(staticQuorumConfig, 1, 2, ShutdownBoth),
    Scenario(staticQuorumConfig, 4, 4, ShutdownBoth),
    Scenario(staticQuorumConfig, 5, 4, KeepSide1),
    Scenario(staticQuorumConfig, 1, 5, KeepSide2),
    Scenario(staticQuorumConfig, 4, 5, KeepSide2),
    Scenario(keepMajorityConfig, 2, 1, KeepSide1),
    Scenario(keepMajorityConfig, 1, 2, KeepSide2),
    Scenario(keepMajorityConfig, 4, 5, KeepSide2),
    Scenario(keepMajorityConfig, 4, 4, KeepLeader),
    Scenario(keepOldestConfig, 3, 3, KeepSide1),
    Scenario(keepOldestConfig, 1, 1, KeepSide1),
    Scenario(keepOldestConfig, 1, 2, KeepSide2), // because down-if-alone
    Scenario(keepMajorityConfig, 3, 2, KeepAll, {
      case `node1` | `node2` | `node3` => "dcA"
      case _                           => "dcB"
    }),
    Scenario(downAllConfig, 1, 2, ShutdownBoth),
    Scenario(leaseMajorityConfig, 4, 5, KeepSide2))

  "Cluster SplitBrainResolver" must {

    for (scenario <- scenarios) {
      scenario.toString taggedAs LongRunningTest in {
        // temporarily disabled for aeron-udp in multi-node: https://github.com/akka/akka-core/pull/30706/
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
