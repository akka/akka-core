/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.routing

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.{ tailrec, varargs }
import scala.collection.immutable

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.japi.Util.immutableSeq
import akka.remote.RemoteScope
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee
import akka.routing.Group
import akka.routing.Pool
import akka.routing.Resizer
import akka.routing.Routee
import akka.routing.Router
import akka.routing.RouterActor
import akka.routing.RouterConfig
import akka.routing.RouterPoolActor
import akka.routing.RoutingLogic
import akka.util.HashCode
import scala.jdk.CollectionConverters._

object ClusterRouterGroupSettings {

  @varargs
  def apply(
      totalInstances: Int,
      routeesPaths: immutable.Seq[String],
      allowLocalRoutees: Boolean,
      useRoles: String*): ClusterRouterGroupSettings =
    new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles.toSet)

  // For backwards compatibility, useRoles is the combination of use-roles and use-role
  def fromConfig(config: Config): ClusterRouterGroupSettings =
    new ClusterRouterGroupSettings(
      totalInstances = ClusterRouterSettingsBase.getMaxTotalNrOfInstances(config),
      routeesPaths = immutableSeq(config.getStringList("routees.paths")),
      allowLocalRoutees = config.getBoolean("cluster.allow-local-routees"),
      useRoles = config.getStringList("cluster.use-roles").asScala.toSet ++ ClusterRouterSettingsBase.useRoleOption(
          config.getString("cluster.use-role")))

  def apply(
      totalInstances: Int,
      routeesPaths: immutable.Seq[String],
      allowLocalRoutees: Boolean,
      useRoles: Set[String]): ClusterRouterGroupSettings =
    new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles)

  def unapply(settings: ClusterRouterGroupSettings): Option[(Int, immutable.Seq[String], Boolean, Set[String])] =
    Some((settings.totalInstances, settings.routeesPaths, settings.allowLocalRoutees, settings.useRoles))
}

/**
 * `totalInstances` of cluster router must be > 0
 */
@SerialVersionUID(1L)
final class ClusterRouterGroupSettings(
    val totalInstances: Int,
    val routeesPaths: immutable.Seq[String],
    val allowLocalRoutees: Boolean,
    val useRoles: Set[String])
    extends Product
    with Serializable
    with ClusterRouterSettingsBase {

  override def hashCode(): Int = {
    var seed = HashCode.SEED
    seed = HashCode.hash(seed, totalInstances)
    seed = HashCode.hash(seed, routeesPaths)
    seed = HashCode.hash(seed, allowLocalRoutees)
    seed = HashCode.hash(seed, useRoles)
    seed
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ClusterRouterGroupSettings]
  override def productArity: Int = 4
  override def productElement(n: Int): Any = n match {
    case 0 => totalInstances
    case 1 => routeesPaths
    case 2 => allowLocalRoutees
    case 3 => useRoles
  }

  override def equals(obj: Any): Boolean =
    obj match {
      case that: ClusterRouterGroupSettings =>
        this.totalInstances.equals(that.totalInstances) &&
        this.routeesPaths.equals(that.routeesPaths) &&
        this.allowLocalRoutees == that.allowLocalRoutees &&
        this.useRoles.equals(that.useRoles)
      case _ => false
    }

  override def toString: String =
    s"ClusterRouterGroupSettings($totalInstances,$routeesPaths,$allowLocalRoutees,$useRoles)"

  /**
   * Java API
   */
  def this(
      totalInstances: Int,
      routeesPaths: java.lang.Iterable[String],
      allowLocalRoutees: Boolean,
      useRoles: java.util.Set[String]) =
    this(totalInstances, immutableSeq(routeesPaths), allowLocalRoutees, useRoles.asScala.toSet)

  if (totalInstances <= 0) throw new IllegalArgumentException("totalInstances of cluster router must be > 0")
  if ((routeesPaths eq null) || routeesPaths.isEmpty || routeesPaths.head == "")
    throw new IllegalArgumentException("routeesPaths must be defined")

  routeesPaths.foreach {
    case RelativeActorPath(_) => // good
    case p =>
      throw new IllegalArgumentException(s"routeesPaths [$p] is not a valid actor path without address information")
  }

  def withUseRoles(useRoles: Set[String]): ClusterRouterGroupSettings =
    new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles)

  @varargs
  def withUseRoles(useRoles: String*): ClusterRouterGroupSettings =
    new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles.toSet)

  /**
   * Java API
   */
  def withUseRoles(useRoles: java.util.Set[String]): ClusterRouterGroupSettings =
    new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles.asScala.toSet)
}

object ClusterRouterPoolSettings {

  def apply(
      totalInstances: Int,
      maxInstancesPerNode: Int,
      allowLocalRoutees: Boolean,
      useRoles: Set[String]): ClusterRouterPoolSettings =
    new ClusterRouterPoolSettings(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles)

  @varargs
  def apply(
      totalInstances: Int,
      maxInstancesPerNode: Int,
      allowLocalRoutees: Boolean,
      useRoles: String*): ClusterRouterPoolSettings =
    new ClusterRouterPoolSettings(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles.toSet)

  // For backwards compatibility, useRoles is the combination of use-roles and use-role
  def fromConfig(config: Config): ClusterRouterPoolSettings =
    new ClusterRouterPoolSettings(
      totalInstances = ClusterRouterSettingsBase.getMaxTotalNrOfInstances(config),
      maxInstancesPerNode = config.getInt("cluster.max-nr-of-instances-per-node"),
      allowLocalRoutees = config.getBoolean("cluster.allow-local-routees"),
      useRoles = config.getStringList("cluster.use-roles").asScala.toSet ++ ClusterRouterSettingsBase.useRoleOption(
          config.getString("cluster.use-role")))

  def unapply(settings: ClusterRouterPoolSettings): Option[(Int, Int, Boolean, Set[String])] =
    Some((settings.totalInstances, settings.maxInstancesPerNode, settings.allowLocalRoutees, settings.useRoles))
}

/**
 * `totalInstances` of cluster router must be > 0
 * `maxInstancesPerNode` of cluster router must be > 0
 * `maxInstancesPerNode` of cluster router must be 1 when routeesPath is defined
 */
@SerialVersionUID(1L)
final class ClusterRouterPoolSettings(
    val totalInstances: Int,
    val maxInstancesPerNode: Int,
    val allowLocalRoutees: Boolean,
    val useRoles: Set[String])
    extends Product
    with Serializable
    with ClusterRouterSettingsBase {

  override def hashCode(): Int = {
    var seed = HashCode.SEED
    seed = HashCode.hash(seed, totalInstances)
    seed = HashCode.hash(seed, maxInstancesPerNode)
    seed = HashCode.hash(seed, allowLocalRoutees)
    seed = HashCode.hash(seed, useRoles)
    seed
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ClusterRouterPoolSettings]
  override def productArity: Int = 4
  override def productElement(n: Int): Any = n match {
    case 0 => totalInstances
    case 1 => maxInstancesPerNode
    case 2 => allowLocalRoutees
    case 3 => useRoles
  }

  override def equals(obj: Any): Boolean =
    obj match {
      case that: ClusterRouterPoolSettings =>
        this.totalInstances.equals(that.totalInstances) &&
        this.maxInstancesPerNode.equals(that.maxInstancesPerNode) &&
        this.allowLocalRoutees == that.allowLocalRoutees &&
        this.useRoles.equals(that.useRoles)
      case _ => false
    }

  override def toString: String =
    s"ClusterRouterPoolSettings($totalInstances,$maxInstancesPerNode,$allowLocalRoutees,$useRoles)"

  /**
   * Java API
   */
  def this(totalInstances: Int, maxInstancesPerNode: Int, allowLocalRoutees: Boolean, useRoles: java.util.Set[String]) =
    this(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles.asScala.toSet)

  if (maxInstancesPerNode <= 0)
    throw new IllegalArgumentException("maxInstancesPerNode of cluster pool router must be > 0")

  def withUseRoles(useRoles: Set[String]): ClusterRouterPoolSettings =
    new ClusterRouterPoolSettings(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles)

  @varargs
  def withUseRoles(useRoles: String*): ClusterRouterPoolSettings =
    new ClusterRouterPoolSettings(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles.toSet)

  /**
   * Java API
   */
  def withUseRoles(useRoles: java.util.Set[String]): ClusterRouterPoolSettings =
    new ClusterRouterPoolSettings(totalInstances, maxInstancesPerNode, allowLocalRoutees, useRoles.asScala.toSet)
}

/**
 * INTERNAL API
 */
private[akka] object ClusterRouterSettingsBase {
  def useRoleOption(role: String): Option[String] = role match {
    case null | "" => None
    case _         => Some(role)
  }

  /**
   * For backwards compatibility reasons, nr-of-instances
   * has the same purpose as max-total-nr-of-instances for cluster
   * aware routers and nr-of-instances (if defined by user) takes
   * precedence over max-total-nr-of-instances.
   */
  def getMaxTotalNrOfInstances(config: Config): Int =
    config.getInt("nr-of-instances") match {
      case 1 | 0 => config.getInt("cluster.max-nr-of-instances-per-node")
      case other => other
    }
}

/**
 * INTERNAL API
 */
private[akka] trait ClusterRouterSettingsBase {
  def totalInstances: Int
  def allowLocalRoutees: Boolean
  def useRoles: Set[String]

  require(totalInstances > 0, "totalInstances of cluster router must be > 0")
  require(useRoles != null, "useRoles must be non-null")
  require(!useRoles.exists(role => role == null || role.isEmpty), "All roles in useRoles must be non-empty")
}

/**
 * [[akka.routing.RouterConfig]] implementation for deployment on cluster nodes.
 * Delegates other duties to the local [[akka.routing.RouterConfig]],
 * which makes it possible to mix this with the built-in routers such as
 * [[akka.routing.RoundRobinGroup]] or custom routers.
 */
@SerialVersionUID(1L)
final case class ClusterRouterGroup(local: Group, settings: ClusterRouterGroupSettings)
    extends Group
    with ClusterRouterConfigBase {

  override def paths(system: ActorSystem): immutable.Iterable[String] =
    if (settings.allowLocalRoutees && settings.useRoles.nonEmpty) {
      if (settings.useRoles.subsetOf(Cluster(system).selfRoles)) {
        settings.routeesPaths
      } else Nil
    } else if (settings.allowLocalRoutees && settings.useRoles.isEmpty) {
      settings.routeesPaths
    } else Nil

  /**
   * INTERNAL API
   */
  override private[akka] def createRouterActor(): RouterActor = new ClusterRouterGroupActor(settings)

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case ClusterRouterGroup(_: ClusterRouterGroup, _) =>
      throw new IllegalStateException("ClusterRouterGroup is not allowed to wrap a ClusterRouterGroup")
    case ClusterRouterGroup(otherLocal, _) =>
      copy(local = this.local.withFallback(otherLocal).asInstanceOf[Group])
    case _ =>
      copy(local = this.local.withFallback(other).asInstanceOf[Group])
  }

}

/**
 * [[akka.routing.RouterConfig]] implementation for deployment on cluster nodes.
 * Delegates other duties to the local [[akka.routing.RouterConfig]],
 * which makes it possible to mix this with the built-in routers such as
 * [[akka.routing.RoundRobinGroup]] or custom routers.
 */
@SerialVersionUID(1L)
final case class ClusterRouterPool(local: Pool, settings: ClusterRouterPoolSettings)
    extends Pool
    with ClusterRouterConfigBase {

  require(local.resizer.isEmpty, "Resizer can't be used together with cluster router")

  @transient private val childNameCounter = new AtomicInteger

  /**
   * INTERNAL API
   */
  override private[akka] def newRoutee(routeeProps: Props, context: ActorContext): Routee = {
    val name = "c" + childNameCounter.incrementAndGet
    val ref = context
      .asInstanceOf[ActorCell]
      .attachChild(local.enrichWithPoolDispatcher(routeeProps, context), name, systemService = false)
    ActorRefRoutee(ref)
  }

  /**
   * Initial number of routee instances
   */
  override def nrOfInstances(sys: ActorSystem): Int =
    if (settings.allowLocalRoutees && settings.useRoles.nonEmpty) {
      if (settings.useRoles.subsetOf(Cluster(sys).selfRoles)) {
        settings.maxInstancesPerNode
      } else 0
    } else if (settings.allowLocalRoutees && settings.useRoles.isEmpty) {
      settings.maxInstancesPerNode
    } else 0

  override def resizer: Option[Resizer] = local.resizer

  /**
   * INTERNAL API
   */
  override private[akka] def createRouterActor(): RouterActor =
    new ClusterRouterPoolActor(local.supervisorStrategy, settings)

  override def supervisorStrategy: SupervisorStrategy = local.supervisorStrategy

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case ClusterRouterPool(_: ClusterRouterPool, _) =>
      throw new IllegalStateException("ClusterRouterPool is not allowed to wrap a ClusterRouterPool")
    case ClusterRouterPool(otherLocal, _) =>
      copy(local = this.local.withFallback(otherLocal).asInstanceOf[Pool])
    case _ =>
      copy(local = this.local.withFallback(other).asInstanceOf[Pool])
  }

}

/**
 * INTERNAL API
 */
private[akka] trait ClusterRouterConfigBase extends RouterConfig {
  def local: RouterConfig
  def settings: ClusterRouterSettingsBase
  override def createRouter(system: ActorSystem): Router = local.createRouter(system)
  override def routerDispatcher: String = local.routerDispatcher
  override def stopRouterWhenAllRouteesRemoved: Boolean = false
  override def routingLogicController(routingLogic: RoutingLogic): Option[Props] =
    local.routingLogicController(routingLogic)

  // Intercept ClusterDomainEvent and route them to the ClusterRouterActor
  override def isManagementMessage(msg: Any): Boolean =
    msg.isInstanceOf[ClusterDomainEvent] || msg.isInstanceOf[CurrentClusterState] || super.isManagementMessage(msg)
}

/**
 * INTERNAL API
 */
private[akka] class ClusterRouterPoolActor(
    supervisorStrategy: SupervisorStrategy,
    val settings: ClusterRouterPoolSettings)
    extends RouterPoolActor(supervisorStrategy)
    with ClusterRouterActor {

  override def receive = clusterReceive.orElse(super.receive)

  /**
   * Adds routees based on totalInstances and maxInstancesPerNode settings
   */
  override def addRoutees(): Unit = {
    @tailrec
    def doAddRoutees(): Unit = selectDeploymentTarget match {
      case None => // done
      case Some(target) =>
        val routeeProps = cell.routeeProps
        val deploy =
          Deploy(config = ConfigFactory.empty(), routerConfig = routeeProps.routerConfig, scope = RemoteScope(target))
        val routee = pool.newRoutee(routeeProps.withDeploy(deploy), context)
        // must register each one, since registered routees are used in selectDeploymentTarget
        cell.addRoutee(routee)

        // recursion until all created
        doAddRoutees()
    }

    doAddRoutees()
  }

  def selectDeploymentTarget: Option[Address] = {
    val currentRoutees = cell.router.routees
    val currentNodes = availableNodes
    if (currentNodes.isEmpty || currentRoutees.size >= settings.totalInstances) {
      None
    } else {
      // find the node with least routees
      val numberOfRouteesPerNode: Map[Address, Int] = {
        val nodeMap: Map[Address, Int] = currentNodes.map(_ -> 0).toMap.withDefaultValue(0)
        currentRoutees.foldLeft(nodeMap) { (acc, x) =>
          val address = fullAddress(x)
          acc + (address -> (acc(address) + 1))
        }
      }

      val (address, count) = numberOfRouteesPerNode.minBy(_._2)
      if (count < settings.maxInstancesPerNode) Some(address) else None
    }
  }

}

/**
 * INTERNAL API
 */
private[akka] class ClusterRouterGroupActor(val settings: ClusterRouterGroupSettings)
    extends RouterActor
    with ClusterRouterActor {

  val group = cell.routerConfig match {
    case x: Group => x
    case other =>
      throw ActorInitializationException("ClusterRouterGroupActor can only be used with group, not " + other.getClass)
  }

  override def receive = clusterReceive.orElse(super.receive)

  var usedRouteePaths: Map[Address, Set[String]] =
    if (settings.allowLocalRoutees)
      Map(cluster.selfAddress -> settings.routeesPaths.toSet)
    else
      Map.empty

  /**
   * Adds routees based on totalInstances and maxInstancesPerNode settings
   */
  override def addRoutees(): Unit = {
    @tailrec
    def doAddRoutees(): Unit = selectDeploymentTarget match {
      case None => // done
      case Some((address, path)) =>
        val routee = group.routeeFor(address.toString + path, context)
        usedRouteePaths = usedRouteePaths.updated(address, usedRouteePaths.getOrElse(address, Set.empty) + path)
        // must register each one, since registered routees are used in selectDeploymentTarget
        cell.addRoutee(routee)

        // recursion until all created
        doAddRoutees()
    }

    doAddRoutees()
  }

  def selectDeploymentTarget: Option[(Address, String)] = {
    val currentRoutees = cell.router.routees
    val currentNodes = availableNodes
    if (currentNodes.isEmpty || currentRoutees.size >= settings.totalInstances) {
      None
    } else {
      // find the node with least routees
      val unusedNodes = currentNodes.filterNot(usedRouteePaths.contains)

      if (unusedNodes.nonEmpty) {
        Some((unusedNodes.head, settings.routeesPaths.head))
      } else {
        val (address, used) = usedRouteePaths.minBy { case (_, used) => used.size }
        // pick next of the unused paths
        settings.routeesPaths.collectFirst { case p if !used.contains(p) => (address, p) }
      }
    }
  }

  override def removeMember(member: Member): Unit = {
    usedRouteePaths -= member.address
    super.removeMember(member)
  }
}

/**
 * INTERNAL API
 * The router actor, subscribes to cluster events and
 * adjusts the routees.
 */
private[akka] trait ClusterRouterActor { this: RouterActor =>

  def settings: ClusterRouterSettingsBase

  if (!cell.routerConfig.isInstanceOf[Pool] && !cell.routerConfig.isInstanceOf[Group])
    throw ActorInitializationException(
      "Cluster router actor can only be used with Pool or Group, not with " +
      cell.routerConfig.getClass)

  def cluster: Cluster = Cluster(context.system)

  // re-subscribe when restart
  override def preStart(): Unit =
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])

  override def postStop(): Unit = cluster.unsubscribe(self)

  var nodes: immutable.SortedSet[Address] = {
    import akka.cluster.Member.addressOrdering
    cluster.readView.members.collect {
      case m if isAvailable(m) => m.address
    }
  }

  def isAvailable(m: Member): Boolean =
    (m.status == MemberStatus.Up || m.status == MemberStatus.WeaklyUp) &&
    satisfiesRoles(m.roles) &&
    (settings.allowLocalRoutees || m.address != cluster.selfAddress)

  private def satisfiesRoles(memberRoles: Set[String]): Boolean = settings.useRoles.subsetOf(memberRoles)

  def availableNodes: immutable.SortedSet[Address] = {
    import akka.cluster.Member.addressOrdering
    if (nodes.isEmpty && settings.allowLocalRoutees && satisfiesRoles(cluster.selfRoles))
      // use my own node, cluster information not updated yet
      immutable.SortedSet(cluster.selfAddress)
    else
      nodes
  }

  /**
   * Fills in self address for local ActorRef
   */
  def fullAddress(routee: Routee): Address = {
    val address = routee match {
      case ActorRefRoutee(ref)       => ref.path.address
      case ActorSelectionRoutee(sel) => sel.anchor.path.address
      case unknown                   => throw new IllegalArgumentException(s"Unsupported routee type: ${unknown.getClass}")
    }
    address match {
      case Address(_, _, None, None) => cluster.selfAddress
      case a                         => a
    }
  }

  /**
   * Adds routees based on settings
   */
  def addRoutees(): Unit

  def addMember(member: Member): Unit = {
    nodes += member.address
    addRoutees()
  }

  def removeMember(member: Member): Unit = {
    val address = member.address
    nodes -= address

    // unregister routees that live on that node
    val affectedRoutees = cell.router.routees.filter(fullAddress(_) == address)
    cell.removeRoutees(affectedRoutees, stopChild = true)

    // addRoutees will not create more than createRoutees and maxInstancesPerNode
    // this is useful when totalInstances < upNodes.size
    addRoutees()
  }

  def clusterReceive: Receive = {
    case s: CurrentClusterState =>
      import akka.cluster.Member.addressOrdering
      nodes = s.members.collect { case m if isAvailable(m) => m.address }
      addRoutees()

    case m: MemberEvent if isAvailable(m.member) =>
      addMember(m.member)

    case other: MemberEvent =>
      // other events means that it is no longer interesting, such as
      // MemberExited, MemberRemoved
      removeMember(other.member)

    case UnreachableMember(m) =>
      removeMember(m)

    case ReachableMember(m) =>
      if (isAvailable(m)) addMember(m)
  }
}
