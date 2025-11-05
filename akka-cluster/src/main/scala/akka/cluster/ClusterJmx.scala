/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.lang.management.ManagementFactory
import javax.management.InstanceAlreadyExistsException
import javax.management.InstanceNotFoundException
import javax.management.ObjectName
import javax.management.StandardMBean

import akka.actor.AddressFromURIString
import akka.event.LoggingAdapter

/**
 * Interface for the cluster JMX MBean.
 */
trait ClusterNodeMBean {

  /**
   * Member status for this node.
   */
  def getMemberStatus: String

  /**
   * Comma separated addresses of member nodes, sorted in the cluster ring order.
   * The address format is `akka://actor-system-name@hostname:port`
   */
  def getMembers: String

  /**
   * Comma separated addresses of unreachable member nodes.
   * The address format is `akka://actor-system-name@hostname:port`
   */
  def getUnreachable: String

  /*
   * JSON format of the status of all nodes in the cluster as follows:
   * {{{
   * {
   *   "self-address": "akka://system@host1:2552",
   *   "members": [
   *     {
   *       "address": "akka://system@host1:2552",
   *       "status": "Up",
   *       "app-version": "1.0.0",
   *       "roles": [
   *         "frontend"
   *       ]
   *     },
   *     {
   *       "address": "akka://system@host2:2552",
   *       "status": "Up",
   *       "app-version": "1.0.0",
   *       "roles": [
   *         "frontend"
   *       ]
   *     },
   *     {
   *       "address": "akka://system@host3:2552",
   *       "status": "Down",
   *       "app-version": "1.0.0",
   *       "roles": [
   *         "backend"
   *       ]
   *     },
   *     {
   *       "address": "akka://system@host4:2552",
   *       "status": "Joining",
   *       "app-version": "1.1.0",
   *       "roles": [
   *         "backend"
   *       ]
   *     }
   *   ],
   *   "unreachable": [
   *     {
   *       "node": "akka://system@host2:2552",
   *       "observed-by": [
   *         "akka://system@host1:2552",
   *         "akka://system@host3:2552"
   *       ]
   *     },
   *     {
   *       "node": "akka://system@host3:2552",
   *       "observed-by": [
   *         "akka://system@host1:2552",
   *         "akka://system@host2:2552"
   *       ]
   *     }
   *   ]
   * }
   * }}}
   */
  def getClusterStatus: String

  /**
   * Get the address of the current leader.
   * The address format is `akka://actor-system-name@hostname:port`
   */
  def getLeader: String

  /**
   * Does the cluster consist of only one member?
   */
  def isSingleton: Boolean

  /**
   * Returns true if the node is not unreachable and not `Down`
   * and not `Removed`.
   */
  def isAvailable: Boolean

  /**
   * Try to join this cluster node with the node specified by 'address'.
   * The address format is `akka://actor-system-name@hostname:port`.
   * A 'Join(thisNodeAddress)' command is sent to the node to join.
   */
  def join(address: String): Unit

  /**
   * Send command to issue state transition to LEAVING for the node specified by 'address'.
   * The address format is `akka://actor-system-name@hostname:port`
   */
  def leave(address: String): Unit

  /**
   * Send command to DOWN the node specified by 'address'.
   * The address format is `akka://actor-system-name@hostname:port`
   */
  def down(address: String): Unit
}

/**
 * INTERNAL API
 */
private[akka] class ClusterJmx(cluster: Cluster, log: LoggingAdapter) {

  private val mBeanServer = ManagementFactory.getPlatformMBeanServer
  private val clusterMBeanName =
    if (cluster.settings.JmxMultiMbeansInSameEnabled)
      new ObjectName("akka:type=Cluster,port=" + cluster.selfUniqueAddress.address.port.getOrElse(""))
    else
      new ObjectName("akka:type=Cluster")

  private def clusterView = cluster.readView
  import cluster.ClusterLogger._

  /**
   * Creates the cluster JMX MBean and registers it in the MBean server.
   */
  def createMBean() = {
    val mbean = new StandardMBean(classOf[ClusterNodeMBean]) with ClusterNodeMBean {

      // JMX attributes (bean-style)

      def getClusterStatus: String = {
        val members = clusterView.members.toSeq
          .sorted(Member.ordering)
          .map { m =>
            s"""{
              |      "address": "${m.address}",
              |      "roles": [${if (m.roles.isEmpty) ""
               else m.roles.toList.sorted.map("\"" + _ + "\"").mkString("\n        ", ",\n        ", "\n      ")}],
              |      "status": "${m.status}",
              |      "app-version": "${m.appVersion}"
              |    }""".stripMargin
          }
          .mkString(",\n    ")

        val unreachable = clusterView.reachability.observersGroupedByUnreachable.toSeq
          .sortBy(_._1)
          .map {
            case (subject, observers) => {
              val observerAddresses = observers.toSeq.sorted.map("\"" + _.address + "\"")
              s"""{
              |      "node": "${subject.address}",
              |      "observed-by": [${if (observerAddresses.isEmpty) ""
                 else observerAddresses.mkString("\n        ", ",\n        ", "\n      ")}]
              |    }""".stripMargin
            }

          }
          .mkString(",\n    ")

        s"""{
        |  "members": [${if (members.isEmpty) "" else "\n    " + members + "\n  "}],
        |  "self-address": "${clusterView.selfAddress}",
        |  "unreachable": [${if (unreachable.isEmpty) "" else "\n    " + unreachable + "\n  "}]
        |}
        |""".stripMargin
      }

      def getMembers: String =
        clusterView.members.toSeq.map(_.address).mkString(",")

      def getUnreachable: String =
        clusterView.unreachableMembers.map(_.address).mkString(",")

      def getMemberStatus: String = clusterView.status.toString

      def getLeader: String = clusterView.leader.fold("")(_.toString)

      def isSingleton: Boolean = clusterView.isSingletonCluster

      def isAvailable: Boolean = clusterView.isAvailable

      // JMX commands

      def join(address: String) = cluster.join(AddressFromURIString(address))

      def leave(address: String) = cluster.leave(AddressFromURIString(address))

      def down(address: String) = cluster.down(AddressFromURIString(address))
    }
    try {
      mBeanServer.registerMBean(mbean, clusterMBeanName)
      logInfo("Registered cluster JMX MBean [{}]", clusterMBeanName)
    } catch {
      case e: InstanceAlreadyExistsException => {
        if (cluster.settings.JmxMultiMbeansInSameEnabled) {
          log.error(e, s"Failed to register Cluster JMX MBean with name=$clusterMBeanName")
        } else {
          log.warning(
            s"Could not register Cluster JMX MBean with name=$clusterMBeanName as it is already registered. " +
            "If you are running multiple clusters in the same JVM, set 'akka.cluster.jmx.multi-mbeans-in-same-jvm = on' in config")
        }
      }
    }
  }

  /**
   * Unregisters the cluster JMX MBean from MBean server.
   */
  def unregisterMBean(): Unit = {
    try {
      mBeanServer.unregisterMBean(clusterMBeanName)
    } catch {
      case e: InstanceNotFoundException => {
        if (cluster.settings.JmxMultiMbeansInSameEnabled) {
          log.error(e, s"Failed to unregister Cluster JMX MBean with name=$clusterMBeanName")
        } else {
          log.warning(
            s"Could not unregister Cluster JMX MBean with name=$clusterMBeanName as it was not found. " +
            "If you are running multiple clusters in the same JVM, set 'akka.cluster.jmx.multi-mbeans-in-same-jvm = on' in config")
        }
      }
    }
  }

}
