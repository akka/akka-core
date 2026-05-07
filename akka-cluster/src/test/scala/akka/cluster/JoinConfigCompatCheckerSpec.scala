/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.{ immutable => im }
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.testkit.{ AkkaSpec, LongRunningTest }

object JoinConfigCompatCheckerSpec {

  val baseConfig: Config =
    ConfigFactory.parseString("""
     akka.actor.provider = "cluster"
     akka.coordinated-shutdown.terminate-actor-system = on
     akka.remote.artery.canonical.port = 0
     akka.cluster.jmx.multi-mbeans-in-same-jvm = on
     akka.remote.artery.advanced.aeron.idle-cpu-level = 3
     """)

  val configWithChecker: Config =
    ConfigFactory.parseString("""
      akka.cluster {
        config-compat-test = "test"
        sensitive.properties {
          username = "abc"
          password = "def"
        }

        configuration-compatibility-check {
          enforce-on-join = on
          checkers {
           akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
          }
          sensitive-config-paths {
            akka = [ "akka.cluster.sensitive.properties" ]
          }
        }
      }
    """).withFallback(baseConfig)
}

class JoinConfigCompatCheckerSpec extends AkkaSpec with ClusterTestKit {
  import JoinConfigCompatCheckerSpec._

  "A Joining Node" must {

    "be allowed to join a cluster when its configuration is compatible" taggedAs LongRunningTest in {

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(configWithChecker)
      clusterTestUtil.formCluster()

      try {
        awaitCond(clusterTestUtil.isMemberUp(joiningNode), message = "awaiting joining node to be 'Up'")
      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "NOT be allowed to join a cluster when its configuration is incompatible" taggedAs LongRunningTest in {
      // this config is NOT compatible with the cluster config
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is incompatible
              config-compat-test = "test2"
              configuration-compatibility-check {
                enforce-on-join = on
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                }
              }
            }
            """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(configWithChecker))

      clusterTestUtil.formCluster()

      try {

        // node will shutdown after unsuccessful join attempt
        within(5.seconds) {
          awaitCond(clusterTestUtil.isTerminated(joiningNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "NOT be allowed to join a cluster when one of its required properties are not available on cluster side" taggedAs LongRunningTest in {

      // this config is NOT compatible with the cluster config
      // because there is one missing required configuration property.
      // This test verifies that cluster config are being sent back and checked on joining node as well
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is not available on cluster side
              akka.cluster.config-compat-test-extra = on

              configuration-compatibility-check {
                enforce-on-join = on
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                 akka-cluster-extra = "akka.cluster.JoinConfigCompatCheckerExtraTest"
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(configWithChecker))

      clusterTestUtil.formCluster()

      try {
        // node will shutdown after unsuccessful join attempt
        within(5.seconds) {
          awaitCond(clusterTestUtil.isTerminated(joiningNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "NOT be allowed to join a cluster when one of the cluster required properties are not available on the joining side" taggedAs LongRunningTest in {

      // this config is NOT compatible with the cluster config
      // because there is one missing required configuration property.
      // This test verifies that cluster config are being sent back and checked on joining node as well
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is required on cluster side
              # config-compat-test = "test"

              configuration-compatibility-check {
                enforce-on-join = on
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(baseConfig))

      clusterTestUtil.formCluster()

      try {
        // node will shutdown after unsuccessful join attempt
        within(5.seconds) {
          awaitCond(clusterTestUtil.isTerminated(joiningNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "be allowed to join a cluster when one of its required properties are not available on cluster side but it's configured to NOT enforce it" taggedAs LongRunningTest in {

      // this config is NOT compatible with the cluster config
      // because there is one missing required configuration property.
      // This test verifies that validation on joining side takes 'configuration-compatibility-check.enforce-on-join' in consideration
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is not available on cluster side
              akka.cluster.config-compat-test-extra = on

              configuration-compatibility-check {
                enforce-on-join = off
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                 akka-cluster-extra = "akka.cluster.JoinConfigCompatCheckerExtraTest"
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(configWithChecker))

      clusterTestUtil.formCluster()

      try {
        // join with compatible node
        awaitCond(clusterTestUtil.isMemberUp(joiningNode), message = "awaiting joining node to be 'Up'")
      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "be allowed to join a cluster when its configuration is incompatible but it's configured to NOT enforce it" taggedAs LongRunningTest in {
      // this config is NOT compatible with the cluster config,
      // but node will ignore the the config check and join anyway
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              configuration-compatibility-check {
                # not enforcing config compat check
                enforce-on-join = off
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                }
              }
             # this config is incompatible
             config-compat-test = "test2"
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(configWithChecker))

      clusterTestUtil.formCluster()

      try {
        // join with compatible node
        awaitCond(clusterTestUtil.isMemberUp(joiningNode), message = "awaiting joining node to be 'Up'")
      } finally {
        clusterTestUtil.shutdownAll()
      }
    }

    /** This test verifies the built-in JoinConfigCompatCheckerAkkaCluster */
    "NOT be allowed to join a cluster using a different value for akka.cluster.downing-provider-class" taggedAs LongRunningTest in {

      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # using explicit downing provider class
              downing-provider-class = "akka.cluster.testkit.AutoDowning"
              testkit.auto-down-unreachable-after = 0s

              configuration-compatibility-check {
                enforce-on-join = on
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(baseConfig)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(baseConfig))

      clusterTestUtil.formCluster()

      try {
        // node will shutdown after unsuccessful join attempt
        within(5.seconds) {
          awaitCond(clusterTestUtil.isTerminated(joiningNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

  }

  "A First Node" must {

    "be allowed to re-join a cluster when its configuration is compatible" taggedAs LongRunningTest in {

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      val firstNode = clusterTestUtil.newActorSystem(configWithChecker)
      // second node
      val secondNode = clusterTestUtil.newActorSystem(configWithChecker)

      clusterTestUtil.formCluster()

      try {
        within(25.seconds) {
          // we must wait second node to join the cluster before shutting down the first node
          awaitCond(clusterTestUtil.isMemberUp(secondNode), message = "awaiting second node to be 'Up'")

          val restartedNode = clusterTestUtil.quitAndRestart(firstNode, configWithChecker)
          clusterTestUtil.joinCluster(restartedNode)

          awaitCond(clusterTestUtil.isMemberUp(restartedNode), message = "awaiting restarted first node to be 'Up'")
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "NOT be allowed to re-join a cluster when its configuration is incompatible" taggedAs LongRunningTest in {
      // this config is NOT compatible with the cluster config
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is incompatible
              config-compat-test = "test2"
              configuration-compatibility-check {
                enforce-on-join = on
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      val firstNode = clusterTestUtil.newActorSystem(configWithChecker)
      // second node
      val secondNode = clusterTestUtil.newActorSystem(configWithChecker)

      clusterTestUtil.formCluster()

      try {
        within(25.seconds) {
          // we must wait second node to join the cluster before shutting down the first node
          awaitCond(clusterTestUtil.isMemberUp(secondNode), message = "awaiting second node to be 'Up'")

          val restartedNode = clusterTestUtil.quitAndRestart(firstNode, joinNodeConfig.withFallback(configWithChecker))
          clusterTestUtil.joinCluster(restartedNode)

          // node will shutdown after unsuccessful join attempt
          awaitCond(clusterTestUtil.isTerminated(restartedNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "NOT be allowed to re-join a cluster when one of its required properties are not available on cluster side" taggedAs LongRunningTest in {

      // this config is NOT compatible with the cluster config
      // because there is one missing required configuration property.
      // This test verifies that cluster config are being sent back and checked on joining node as well
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is not available on cluster side
              akka.cluster.config-compat-test-extra = on

              configuration-compatibility-check {
                enforce-on-join = on
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                 akka-cluster-extra = "akka.cluster.JoinConfigCompatCheckerExtraTest"
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      val firstNode = clusterTestUtil.newActorSystem(configWithChecker)
      // second node
      val secondNode = clusterTestUtil.newActorSystem(configWithChecker)

      clusterTestUtil.formCluster()

      try {
        within(25.seconds) {
          // we must wait second node to join the cluster before shutting down the first node
          awaitCond(clusterTestUtil.isMemberUp(secondNode), message = "awaiting second node to be 'Up'")

          val restartedNode = clusterTestUtil.quitAndRestart(firstNode, joinNodeConfig.withFallback(configWithChecker))
          clusterTestUtil.joinCluster(restartedNode)

          // node will shutdown after unsuccessful join attempt
          awaitCond(clusterTestUtil.isTerminated(restartedNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "NOT be allowed to re-join a cluster when one of the cluster required properties are not available on the joining side" taggedAs LongRunningTest in {

      // this config is NOT compatible with the cluster config
      // because there is one missing required configuration property.
      // This test verifies that cluster config are being sent back and checked on joining node as well
      val joinNodeConfig =
        ConfigFactory.parseString("""
              akka.cluster {

                # this config is required on cluster side
                # config-compat-test = "test"

                configuration-compatibility-check {
                  enforce-on-join = on
                }
              }
            """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      val firstNode = clusterTestUtil.newActorSystem(configWithChecker)
      // second node
      val secondNode = clusterTestUtil.newActorSystem(configWithChecker)

      clusterTestUtil.formCluster()

      try {
        within(25.seconds) {
          // we must wait second node to join the cluster before shutting down the first node
          awaitCond(clusterTestUtil.isMemberUp(secondNode), message = "awaiting second node to be 'Up'")

          val restartedNode = clusterTestUtil.quitAndRestart(firstNode, joinNodeConfig.withFallback(baseConfig))
          clusterTestUtil.joinCluster(restartedNode)

          // node will shutdown after unsuccessful join attempt
          awaitCond(clusterTestUtil.isTerminated(restartedNode))
        }

      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "be allowed to re-join a cluster when one of its required properties are not available on cluster side but it's configured to NOT enforce it" taggedAs LongRunningTest in {

      // this config is NOT compatible with the cluster config
      // because there is one missing required configuration property.
      // This test verifies that validation on joining side takes 'configuration-compatibility-check.enforce-on-join' in consideration
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # this config is not available on cluster side
              akka.cluster.config-compat-test-extra = on

              configuration-compatibility-check {
                enforce-on-join = off
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                 akka-cluster-extra = "akka.cluster.JoinConfigCompatCheckerExtraTest"
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      val firstNode = clusterTestUtil.newActorSystem(configWithChecker)
      // second node
      val secondNode = clusterTestUtil.newActorSystem(configWithChecker)

      clusterTestUtil.formCluster()

      try {
        within(25.seconds) {
          // join with compatible node
          // we must wait second node to join the cluster before shutting down the first node
          awaitCond(clusterTestUtil.isMemberUp(secondNode), message = "awaiting second node to be 'Up'")

          val restartedNode = clusterTestUtil.quitAndRestart(firstNode, joinNodeConfig.withFallback(configWithChecker))
          clusterTestUtil.joinCluster(restartedNode)

          // node will have joined the cluster
          awaitCond(clusterTestUtil.isMemberUp(restartedNode), message = "awaiting restarted node to be 'Up'")
        }
      } finally {
        clusterTestUtil.shutdownAll()
      }

    }

    "be allowed to re-join a cluster when its configuration is incompatible but it's configured to NOT enforce it" taggedAs LongRunningTest in {
      // this config is NOT compatible with the cluster config,
      // but node will ignore the the config check and join anyway
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              configuration-compatibility-check {
                # not enforcing config compat check
                enforce-on-join = off
                checkers {
                 akka-cluster-test = "akka.cluster.JoinConfigCompatCheckerTest"
                }
              }
             # this config is incompatible
             config-compat-test = "test2"
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      val firstNode = clusterTestUtil.newActorSystem(configWithChecker)
      // second node
      val secondNode = clusterTestUtil.newActorSystem(configWithChecker)

      clusterTestUtil.formCluster()

      try {
        within(25.seconds) {
          // join with compatible node
          // we must wait second node to join the cluster before shutting down the first node
          awaitCond(clusterTestUtil.isMemberUp(secondNode), message = "awaiting second node to be 'Up'")

          val restartedNode = clusterTestUtil.quitAndRestart(firstNode, joinNodeConfig.withFallback(configWithChecker))
          clusterTestUtil.joinCluster(restartedNode)

          // node will have joined the cluster
          awaitCond(clusterTestUtil.isMemberUp(restartedNode), message = "awaiting restarted node to be 'Up'")
        }
      } finally {
        clusterTestUtil.shutdownAll()
      }
    }

  }

  "A Cluster" must {
    "NOT exchange sensitive config paths with joining node" taggedAs LongRunningTest in {

      // this config has sensitive properties that are not compatible with the cluster
      // the cluster will ignore them, because they are on the sensitive-config-path
      // the cluster won't let it be leaked back to the joining node neither which will fail the join attempt.
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {

              # these config are compatible,
              # but won't be leaked back to joining node which will cause it to fail to join
              sensitive.properties {
                username = "abc"
                password = "def"
              }

              configuration-compatibility-check {
                enforce-on-join = on
                checkers {
                  # rogue checker to trick the cluster to leak sensitive data
                  rogue-checker = "akka.cluster.RogueJoinConfigCompatCheckerTest"
                }

                # unset sensitive config paths
                # this will allow the joining node to leak sensitive info and try
                # get back these same properties from the cluster
                sensitive-config-paths {
                  akka = []
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      // first node
      clusterTestUtil.newActorSystem(configWithChecker)
      val joiningNode = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(configWithChecker))

      clusterTestUtil.formCluster()

      try {
        // node will shutdown after unsuccessful join attempt
        within(5.seconds) {
          awaitCond(clusterTestUtil.isTerminated(joiningNode))
        }
      } finally {
        clusterTestUtil.shutdownAll()
      }
    }

    "be allowed to disable a check" taggedAs LongRunningTest in {

      // this config has sensitive properties that are not compatible with the cluster
      // the cluster will ignore them, because they are on the sensitive-config-path
      // the cluster won't let it be leaked back to the joining node neither which will fail the join attempt.
      val joinNodeConfig =
        ConfigFactory.parseString("""
            akka.cluster {
              configuration-compatibility-check {
                checkers {
                  # disable what is defined in reference.conf
                  akka-cluster = ""
                  akka-cluster-test = ""
                }
              }
            }
          """)

      val clusterTestUtil = new ClusterTestUtil(system.name)
      try {
        val sys = clusterTestUtil.newActorSystem(joinNodeConfig.withFallback(configWithChecker))
        Cluster(sys).settings.ConfigCompatCheckers should ===(Set.empty[String])
      } finally {
        clusterTestUtil.shutdownAll()
      }
    }
  }
}

class JoinConfigCompatCheckerTest extends JoinConfigCompatChecker {
  override def requiredKeys = im.Seq("akka.cluster.config-compat-test")

  override def check(toValidate: Config, actualConfig: Config): ConfigValidation =
    JoinConfigCompatChecker.fullMatch(requiredKeys, toValidate, actualConfig)
}

class JoinConfigCompatCheckerExtraTest extends JoinConfigCompatChecker {
  override def requiredKeys = im.Seq("akka.cluster.config-compat-test-extra")

  override def check(toValidate: Config, actualConfig: Config): ConfigValidation =
    JoinConfigCompatChecker.fullMatch(requiredKeys, toValidate, actualConfig)
}

/** Rogue checker that tries to leak sensitive information */
class RogueJoinConfigCompatCheckerTest extends JoinConfigCompatChecker {

  override def requiredKeys =
    im.Seq("akka.cluster.sensitive.properties.password", "akka.cluster.sensitive.properties.username")

  /** this check always returns Valid. The goal is to try to make the cluster leak those properties */
  override def check(toValidate: Config, actualConfig: Config): ConfigValidation =
    JoinConfigCompatChecker.fullMatch(requiredKeys, toValidate, actualConfig)
}
