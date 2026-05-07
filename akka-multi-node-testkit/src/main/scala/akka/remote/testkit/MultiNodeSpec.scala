/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testkit

import akka.actor.RootActorPath
import akka.actor._
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.remote.RemoteTransportException
import akka.remote.testconductor.RoleName
import akka.remote.testconductor.TestConductor
import akka.remote.testconductor.TestConductorExt
import akka.testkit.TestEvent._
import akka.testkit.TestKit
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject
import io.netty.channel.ChannelException

import java.net.InetAddress
import java.net.InetSocketAddress
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Awaitable
import scala.language.implicitConversions
import scala.util.control.NonFatal

/**
 * Configure the role names and participants of the test, including configuration settings.
 */
abstract class MultiNodeConfig {

  private var _commonConf: Option[Config] = None
  private var _nodeConf = Map[RoleName, Config]()
  private var _roles = Vector[RoleName]()
  private var _deployments = Map[RoleName, immutable.Seq[String]]()
  private var _allDeploy = Vector[String]()
  private var _testTransport = false

  /**
   * Register a common base config for all test participants, if so desired.
   */
  def commonConfig(config: Config): Unit = _commonConf = Some(config)

  /**
   * Register a config override for a specific participant.
   */
  def nodeConfig(roles: RoleName*)(configs: Config*): Unit = {
    val c = configs.reduceLeft(_.withFallback(_))
    _nodeConf ++= roles.map { _ -> c }
  }

  /**
   * Include for verbose debug logging
   * @param on when `true` debug Config is returned, otherwise config with info logging
   */
  def debugConfig(on: Boolean): Config =
    if (on)
      ConfigFactory.parseString("""
        akka.loglevel = DEBUG
        akka.remote {
          log-received-messages = on
          log-sent-messages = on
        }
        akka.remote.artery {
          log-received-messages = on
          log-sent-messages = on
        }
        akka.actor.debug {
          receive = on
          fsm = on
        }
        """)
    else
      ConfigFactory.empty

  /**
   * Construct a RoleName and return it, to be used as an identifier in the
   * test. Registration of a role name creates a role which then needs to be
   * filled.
   */
  def role(name: String): RoleName = {
    if (_roles.exists(_.name == name)) throw new IllegalArgumentException("non-unique role name " + name)
    val r = RoleName(name)
    _roles :+= r
    r
  }

  def deployOn(role: RoleName, deployment: String): Unit =
    _deployments += role -> ((_deployments.get(role).getOrElse(Vector())) :+ deployment)

  def deployOnAll(deployment: String): Unit = _allDeploy :+= deployment

  /**
   * To be able to use `blackhole`, `passThrough`, and `throttle` you must
   * activate the failure injector and throttler transport adapters by
   * specifying `testTransport(on = true)` in your MultiNodeConfig.
   */
  def testTransport(on: Boolean): Unit = _testTransport = on

  private[testkit] lazy val myself: RoleName = {
    require(_roles.size > MultiNodeSpec.selfIndex, "not enough roles declared for this test")
    _roles(MultiNodeSpec.selfIndex)
  }

  private[akka] def config: Config = {
    val transportConfig =
      if (_testTransport) ConfigFactory.parseString("""
           akka.remote.artery.advanced.test-mode = on
        """)
      else ConfigFactory.empty

    val configs = _nodeConf
        .get(myself)
        .toList ::: _commonConf.toList ::: transportConfig :: MultiNodeSpec.nodeConfig :: MultiNodeSpec.baseConfig :: Nil
    configs.reduceLeft(_.withFallback(_))
  }

  private[testkit] def deployments(node: RoleName): immutable.Seq[String] =
    (_deployments.get(node).getOrElse(Nil)) ++ _allDeploy

  private[testkit] def roles: immutable.Seq[RoleName] = _roles

}

object MultiNodeSpec {

  /**
   * Number of nodes node taking part in this test.
   *
   * {{{
   * -Dmultinode.max-nodes=4
   * }}}
   */
  val maxNodes: Int = Option(Integer.getInteger("multinode.max-nodes"))
    .getOrElse(throw new IllegalStateException("need system property multinode.max-nodes to be set"))

  require(maxNodes > 0, "multinode.max-nodes must be greater than 0")

  /**
   * Name (or IP address; must be resolvable using InetAddress.getByName)
   * of the host this node is running on.
   *
   * {{{
   * -Dmultinode.host=host.example.com
   * }}}
   *
   * InetAddress.getLocalHost.getHostAddress is used if empty or "localhost"
   * is defined as system property "multinode.host".
   */
  val selfName: String = Option(System.getProperty("multinode.host")) match {
    case None       => throw new IllegalStateException("need system property multinode.host to be set")
    case Some("")   => InetAddress.getLocalHost.getHostAddress
    case Some(host) => host
  }

  require(selfName != "", "multinode.host must not be empty")

  /**
   * TCP Port number to be used when running tests on TCP. 0 means a random port.
   *
   * {{{
   * -Dmultinode.port=0
   * }}}
   */
  val tcpPort: Int = Integer.getInteger("multinode.port", 0)

  require(tcpPort >= 0 && tcpPort < 65535, "multinode.port is out of bounds: " + tcpPort)

  /**
   * UDP Port number to be used when running tests on UDP. 0 means a random port.
   *
   * {{{
   * -Dmultinode.udp.port=0
   * }}}
   */
  val udpPort: Option[Int] =
    Option(System.getProperty("multinode.udp.port")).map { _ =>
      Integer.getInteger("multinode.udp.port", 0)
    }

  require(udpPort.getOrElse(1) >= 0 && udpPort.getOrElse(1) < 65535, "multinode.udp.port is out of bounds: " + udpPort)

  /**
   * Port number of this node.
   *
   * This is defined in function of property `multinode.protocol`.
   * If set to 'udp', udpPort will be used. If unset or any other value, it will default to tcpPort.
   */
  val selfPort: Int =
    System.getProperty("multinode.protocol") match {
      case "udp" => udpPort.getOrElse(0)
      case _     => tcpPort
    }

  /**
   * Name (or IP address; must be resolvable using InetAddress.getByName)
   * of the host that the server node is running on.
   *
   * {{{
   * -Dmultinode.server-host=server.example.com
   * }}}
   */
  val serverName: String = Option(System.getProperty("multinode.server-host"))
    .getOrElse(throw new IllegalStateException("need system property multinode.server-host to be set"))

  require(serverName != "", "multinode.server-host must not be empty")

  /**
   * Port number of the node that's running the server system. Defaults to 4711.
   *
   * {{{
   * -Dmultinode.server-port=4711
   * }}}
   */
  val serverPort: Int = Integer.getInteger("multinode.server-port", 4711)

  require(serverPort > 0 && serverPort < 65535, "multinode.server-port is out of bounds: " + serverPort)

  /**
   * Index of this node in the roles sequence. The TestConductor
   * is started in “controller” mode on selfIndex 0, i.e. there you can inject
   * failures and shutdown other nodes etc.
   *
   * {{{
   * -Dmultinode.index=0
   * }}}
   */
  val selfIndex = Option(Integer.getInteger("multinode.index"))
    .getOrElse(throw new IllegalStateException("need system property multinode.index to be set"))

  require(selfIndex >= 0 && selfIndex < maxNodes, "multinode.index is out of bounds: " + selfIndex)

  private[testkit] val nodeConfig = mapToConfig(
    Map(
      "akka.actor.provider" -> "remote",
      "akka.remote.artery.canonical.hostname" -> selfName,
      "akka.remote.artery.canonical.port" -> selfPort))

  private[testkit] val baseConfig: Config =
    ConfigFactory.parseString("""
      akka {
        loggers = ["akka.testkit.TestEventListener"]
        loglevel = "WARNING"
        stdout-loglevel = "WARNING"
        coordinated-shutdown.terminate-actor-system = off
        coordinated-shutdown.run-by-actor-system-terminate = off
        coordinated-shutdown.run-by-jvm-shutdown-hook = off
        actor {
          default-dispatcher {
            executor = "fork-join-executor"
            fork-join-executor {
              parallelism-min = 8
              parallelism-factor = 2.0
              parallelism-max = 8
            }
          }
        }
      }
      """)

  private def mapToConfig(map: Map[String, Any]): Config = {
    import scala.jdk.CollectionConverters._
    ConfigFactory.parseMap(map.asJava)
  }

  // Multi node tests on kubernetes require fixed ports to be mapped and exposed
  // This method change the port bindings to avoid conflicts
  // Please note that with the current setup only port 5000 and 5001 (or 6000 and 6001 when using UDP)
  // are exposed in kubernetes
  def configureNextPortIfFixed(config: Config): Config = {
    val arteryPortConfig = getNextPortString("akka.remote.artery.canonical.port", config)
    ConfigFactory.parseString(s"""{
      $arteryPortConfig
      }""").withFallback(config)
  }

  private def getNextPortString(key: String, config: Config): String = {
    val port = config.getInt(key)
    if (port != 0)
      s"""$key = ${port + 1}"""
    else ""
  }
}

/**
 * Note: To be able to run tests with everything ignored or excluded by tags
 * you must not use `testconductor`, or helper methods that use `testconductor`,
 * from the constructor of your test class. Otherwise the controller node might
 * be shutdown before other nodes have completed and you will see errors like:
 * `AskTimeoutException: sending to terminated ref breaks promises`. Using lazy
 * val is fine.
 */
abstract class MultiNodeSpec(
    val myself: RoleName,
    _system: ActorSystem,
    _roles: immutable.Seq[RoleName],
    deployments: RoleName => Seq[String])
    extends TestKit(_system)
    with MultiNodeSpecCallbacks {

  import MultiNodeSpec._

  /**
   * Constructor for using arbitrary logic to create the actor system used in
   * the multi node spec (the `Config` passed to the creator must be used in
   * the created actor system for the multi node tests to work)
   */
  def this(config: MultiNodeConfig, actorSystemCreator: Config => ActorSystem) =
    this(config.myself, actorSystemCreator(ConfigFactory.load(config.config)), config.roles, config.deployments)

  def this(config: MultiNodeConfig) =
    this(config, {
      val name = TestKitUtils.testNameFromCallStack(classOf[MultiNodeSpec], "".r)
      config =>
        try {
          ActorSystem(name, config)
        } catch {
          // Retry creating the system once as when using port = 0 two systems may try and use the same one.
          // RTE is for aeron, CE for netty
          case _: RemoteTransportException => ActorSystem(name, config)
          case _: ChannelException         => ActorSystem(name, config)
        }
    })

  val log: LoggingAdapter = Logging(system, this)(_.getClass.getName)

  /**
   * Enrich `.await()` onto all Awaitables, using remaining duration from the innermost
   * enclosing `within` block or QueryTimeout.
   */
  implicit def awaitHelper[T](w: Awaitable[T]): AwaitHelper[T] = new AwaitHelper(w)
  class AwaitHelper[T](w: Awaitable[T]) {
    def await: T = Await.result(w, remainingOr(testConductor.Settings.QueryTimeout.duration))
  }

  final override def multiNodeSpecBeforeAll(): Unit = {
    atStartup()
  }

  final override def multiNodeSpecAfterAll(): Unit = {
    // wait for all nodes to remove themselves before we shut the conductor down
    if (selfIndex == 0) {
      testConductor.removeNode(myself)
      within(testConductor.Settings.BarrierTimeout.duration) {
        awaitCond({
          // Await.result(testConductor.getNodes, remaining).filterNot(_ == myself).isEmpty
          testConductor.getNodes.await.forall(_ == myself)
        }, message = s"Nodes not shutdown: ${testConductor.getNodes.await}")
      }
    }
    shutdown(system, duration = shutdownTimeout)
    afterTermination()
  }

  def shutdownTimeout: FiniteDuration = 15.seconds.dilated

  /**
   * Override this and return `true` to assert that the
   * shutdown of the `ActorSystem` was done properly.
   */
  def verifySystemShutdown: Boolean = false

  /*
   * Test Class Interface
   */

  /**
   * Override this method to do something when the whole test is starting up.
   */
  protected def atStartup(): Unit = ()

  /**
   * Override this method to do something when the whole test is terminating.
   */
  protected def afterTermination(): Unit = ()

  /**
   * All registered roles
   */
  def roles: immutable.Seq[RoleName] = _roles

  /**
   * TO BE DEFINED BY USER: Defines the number of participants required for starting the test. This
   * might not be equals to the number of nodes available to the test.
   *
   * Must be a `def`:
   * {{{
   * def initialParticipants = 5
   * }}}
   */
  def initialParticipants: Int
  require(
    initialParticipants > 0,
    "initialParticipants must be a 'def' or early initializer, and it must be greater zero")
  require(initialParticipants <= maxNodes, "not enough nodes to run this test")

  /**
   * Access to the barriers, failure injection, etc. The extension will have
   * been started either in Conductor or Player mode when the constructor of
   * MultiNodeSpec finishes, i.e. do not call the start*() methods yourself!
   */
  var testConductor: TestConductorExt = null

  /**
   * Execute the given block of code only on the given nodes (names according
   * to the `roleMap`).
   */
  def runOn(nodes: RoleName*)(thunk: => Unit): Unit = {
    if (isNode(nodes: _*)) {
      thunk
    }
  }

  /**
   * Verify that the running node matches one of the given nodes
   */
  def isNode(nodes: RoleName*): Boolean = nodes contains myself

  /**
   * Enter the named barriers in the order given. Use the remaining duration from
   * the innermost enclosing `within` block or the default `BarrierTimeout`.
   */
  def enterBarrier(name: String*): Unit =
    testConductor.enter(
      Timeout.durationToTimeout(remainingOr(testConductor.Settings.BarrierTimeout.duration)),
      name.to(immutable.Seq))

  /**
   * Enter the named barriers in the order given. Use the remaining duration from
   * the innermost enclosing `within` block or the passed `max` timeout.
   *
   * Note that the `max` timeout is scaled using Duration.dilated,
   * which uses the configuration entry "akka.test.timefactor".
   */
  def enterBarrier(max: FiniteDuration, name: String*): Unit =
    testConductor.enter(Timeout.durationToTimeout(remainingOr(max.dilated)), name.to(immutable.Seq))

  /**
   * Query the controller for the transport address of the given node (by role name) and
   * return that as an ActorPath for easy composition:
   *
   * {{{
   * val serviceA = system.actorSelection(node("master") / "user" / "serviceA")
   * }}}
   */
  def node(role: RoleName): ActorPath = RootActorPath(testConductor.getAddressFor(role).await)

  def muteDeadLetters(messageClasses: Class[_]*)(sys: ActorSystem = system): Unit =
    if (!sys.log.isDebugEnabled) {
      def mute(clazz: Class[_]): Unit =
        sys.eventStream.publish(Mute(DeadLettersFilter(clazz)(occurrences = Int.MaxValue)))
      if (messageClasses.isEmpty) mute(classOf[AnyRef])
      else messageClasses.foreach(mute)
    }

  /*
   * Implementation (i.e. wait for start etc.)
   */

  private val controllerAddr = new InetSocketAddress(serverName, serverPort)

  protected def attachConductor(tc: TestConductorExt): Unit = {
    val timeout = tc.Settings.BarrierTimeout.duration
    val startFuture =
      if (selfIndex == 0) tc.startController(initialParticipants, myself, controllerAddr)
      else tc.startClient(myself, controllerAddr)
    try Await.result(startFuture, timeout)
    catch {
      case NonFatal(x) => throw new RuntimeException("failure while attaching new conductor", x)
    }
    testConductor = tc
  }

  attachConductor(TestConductor(system))

  // now add deployments, if so desired

  // Cannot be final because of https://github.com/scala/bug/issues/4440
  private case class Replacement(tag: String, role: RoleName) {
    lazy val addr = node(role).address.toString
  }

  private val replacements = roles.map(r => Replacement("@" + r.name + "@", r))

  protected def injectDeployments(sys: ActorSystem, role: RoleName): Unit = {
    val deployer = sys.asInstanceOf[ExtendedActorSystem].provider.deployer
    deployments(role).foreach { str =>
      val deployString = replacements.foldLeft(str) {
        case (base, r @ Replacement(tag, _)) =>
          base.indexOf(tag) match {
            case -1 => base
            case _ =>
              val replaceWith = try r.addr
              catch {
                case NonFatal(e) =>
                  // might happen if all test cases are ignored (excluded) and
                  // controller node is finished/exited before r.addr is run
                  // on the other nodes
                  val unresolved = "akka://unresolved-replacement-" + r.role.name
                  log.warning(unresolved + " due to: " + e.getMessage)
                  unresolved
              }
              base.replace(tag, replaceWith)
          }
      }
      import scala.jdk.CollectionConverters._
      ConfigFactory.parseString(deployString).root.asScala.foreach {
        case (key, value: ConfigObject) => deployer.parseConfig(key, value.toConfig).foreach(deployer.deploy)
        case (key, x) =>
          throw new IllegalArgumentException(s"key $key must map to deployment section, not simple value $x")
      }
    }
  }

  injectDeployments(system, myself)

  protected val myAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

  // useful to see which jvm is running which role, used by LogRoleReplace utility
  log.info("Role [{}] started with address [{}]", myself.name, myAddress)

  /**
   * This method starts a new ActorSystem with the same configuration as the
   * previous one on the current node, including deployments. It also creates
   * a new TestConductor client and registers itself with the conductor so
   * that it is possible to use barriers etc. normally after this method has
   * been called.
   *
   * NOTICE: you MUST start a new system before trying to enter a barrier or
   * otherwise using the TestConductor after having terminated this node’s
   * system.
   */
  protected def startNewSystem(): ActorSystem = {
    val config = ConfigFactory
      .parseString(s"akka.remote.artery.canonical{port=${myAddress.port.get}\nhostname=${myAddress.host.get}}")
      .withFallback(system.settings.config)
    val sys = ActorSystem(system.name, config)
    injectDeployments(sys, myself)
    attachConductor(TestConductor(sys))
    sys
  }
}

/**
 * Use this to hook MultiNodeSpec into your test framework lifecycle, either by having your test extend MultiNodeSpec
 * and call these methods or by creating a trait that calls them and then mixing that trait with your test together
 * with MultiNodeSpec.
 *
 * Example trait for MultiNodeSpec with ScalaTest
 *
 * {{{
 * trait STMultiNodeSpec extends MultiNodeSpecCallbacks with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
 *   override def beforeAll() = multiNodeSpecBeforeAll()
 *   override def afterAll() = multiNodeSpecAfterAll()
 * }
 * }}}
 */
trait MultiNodeSpecCallbacks {

  /**
   * Call this before the start of the test run. NOT before every test case.
   */
  def multiNodeSpecBeforeAll(): Unit

  /**
   * Call this after the all test cases have run. NOT after every test case.
   */
  def multiNodeSpecAfterAll(): Unit
}
