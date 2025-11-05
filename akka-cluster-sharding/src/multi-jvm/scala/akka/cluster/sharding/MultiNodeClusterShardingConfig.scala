/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.lang.reflect.Modifier

import com.typesafe.config.{ Config, ConfigFactory }

import akka.cluster.MultiNodeClusterSpec
import akka.persistence.journal.leveldb.SharedLeveldbJournal
import akka.remote.testkit.MultiNodeConfig

object MultiNodeClusterShardingConfig {

  private[sharding] def testNameFromCallStack(classToStartFrom: Class[_]): String = {

    def isAbstractClass(className: String): Boolean = {
      try {
        Modifier.isAbstract(Class.forName(className).getModifiers)
      } catch {
        case _: Throwable => false // yes catch everything, best effort check
      }
    }

    val startFrom = classToStartFrom.getName
    val filteredStack = Thread.currentThread.getStackTrace.iterator
      .map(_.getClassName)
      // drop until we find the first occurrence of classToStartFrom
      .dropWhile(!_.startsWith(startFrom))
      // then continue to the next entry after classToStartFrom that makes sense
      .dropWhile {
        case `startFrom`                            => true
        case str if str.startsWith(startFrom + "$") => true // lambdas inside startFrom etc
        case str if isAbstractClass(str)            => true
        case _                                      => false
      }

    if (filteredStack.isEmpty)
      throw new IllegalArgumentException(s"Couldn't find [${classToStartFrom.getName}] in call stack")

    // sanitize for actor system name
    scrubActorSystemName(filteredStack.next())
  }

  /**
   * Sanitize the `name` to be used as valid actor system name by
   * replacing invalid characters. `name` may for example be a fully qualified
   * class name and then the short class name will be used.
   */
  def scrubActorSystemName(name: String): String = {
    name
      .replaceFirst("""^.*\.""", "") // drop package name
      .replaceAll("""\$\$?\w+""", "") // drop scala anonymous functions/classes
      .replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def persistenceConfig(targetDir: String): Config =
    ConfigFactory.parseString(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
      akka.persistence.journal.leveldb-shared {
        timeout = 5s
        store {
          native = off
          dir = "$targetDir/journal"
        }
      }
      akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
      akka.persistence.snapshot-store.local.dir = "$targetDir/snapshots"
      """)

}

/**
 * A MultiNodeConfig for ClusterSharding. Implement the roles, etc. and create with the following:
 *
 * Note that this class is not used anywhere yet, but could be a good starting point
 * for new or refactored multi-node sharding specs
 *
 * @param mode the state store mode
 * @param rememberEntities defaults to off
 * @param additionalConfig additional config
 * @param loglevel defaults to INFO
 */
abstract class MultiNodeClusterShardingConfig(
    val mode: String = ClusterShardingSettings.StateStoreModeDData,
    val rememberEntities: Boolean = false,
    val rememberEntitiesStore: String = ClusterShardingSettings.RememberEntitiesStoreDData,
    additionalConfig: String = "",
    loglevel: String = "INFO")
    extends MultiNodeConfig {

  import MultiNodeClusterShardingConfig._

  val targetDir =
    s"target/ClusterSharding${testNameFromCallStack(classOf[MultiNodeClusterShardingConfig]).replace("Config", "").replace("_", "")}"

  val persistenceConfig: Config =
    if (mode == ClusterShardingSettings.StateStoreModeDData && rememberEntitiesStore != ClusterShardingSettings.RememberEntitiesStoreEventsourced)
      ConfigFactory.empty
    else MultiNodeClusterShardingConfig.persistenceConfig(targetDir)

  val common: Config =
    ConfigFactory
      .parseString(s"""
        akka.actor.provider = "cluster"
        akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
        akka.cluster.testkit.auto-down-unreachable-after = 0s
        akka.cluster.sharding.state-store-mode = "$mode"
        akka.cluster.sharding.remember-entities = $rememberEntities
        akka.cluster.sharding.remember-entities-store = "$rememberEntitiesStore"
        akka.cluster.sharding.distributed-data.durable.lmdb {
          dir = $targetDir/sharding-ddata
          map-size = 10 MiB
        }
        akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
        akka.loglevel = $loglevel
        """)
      .withFallback(SharedLeveldbJournal.configToEnableJavaSerializationForTest)
      .withFallback(MultiNodeClusterSpec.clusterConfig)

  commonConfig(ConfigFactory.parseString(additionalConfig).withFallback(persistenceConfig).withFallback(common))

}
