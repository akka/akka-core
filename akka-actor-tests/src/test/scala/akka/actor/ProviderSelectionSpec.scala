/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem.Settings
import akka.actor.ActorSystem.findClassLoader
import akka.actor.setup.ActorSystemSetup
import akka.testkit.AbstractSpec

class ProviderSelectionSpec extends AbstractSpec {
  import ProviderSelection.{ ClusterActorRefProvider, RemoteActorRefProvider }

  "ProviderSelection" must {

    val setup = ActorSystemSetup()
    val localConfig = ConfigFactory.load()
    val classLoader = findClassLoader()

    def settingsWith(key: String): Settings = {
      val c = ConfigFactory.parseString(s"""akka.actor.provider = "$key"""").withFallback(localConfig)
      new Settings(classLoader, c, "test", setup)
    }

    "create a Local ProviderSelection and set local provider fqcn in Settings" in {
      val ps = ProviderSelection.Local
      ps.fqcn shouldEqual classOf[LocalActorRefProvider].getName
      ps.hasCluster shouldBe false
      settingsWith("local").ProviderClass shouldEqual ps.fqcn
    }

    "create a Remote ProviderSelection and set remote provider fqcn in Settings" in {
      val ps = ProviderSelection.Remote
      ps.fqcn shouldEqual RemoteActorRefProvider
      ps.hasCluster shouldBe false
      ProviderSelection("remote") shouldEqual ProviderSelection(RemoteActorRefProvider)
      settingsWith("remote").ProviderClass shouldEqual ps.fqcn
    }

    "create a Cluster ProviderSelection and set cluster provider fqcn in Settings" in {
      val ps = ProviderSelection.Cluster
      ps.fqcn shouldEqual ClusterActorRefProvider
      ps.hasCluster shouldBe true
      ProviderSelection("cluster") shouldEqual ProviderSelection(ClusterActorRefProvider)
      settingsWith("cluster").ProviderClass shouldEqual ps.fqcn
    }

    "create a Custom ProviderSelection and set custom provider fqcn in Settings" in {
      val other = "other.ActorRefProvider"
      val ps = ProviderSelection.Custom(other) //checked by dynamicAccess
      ps.fqcn shouldEqual "other.ActorRefProvider"
      ps.hasCluster shouldBe false
      settingsWith(other).ProviderClass shouldEqual ps.fqcn
    }
  }
}
