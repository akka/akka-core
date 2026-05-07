/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import com.typesafe.config.ConfigFactory

import akka.actor.typed.scaladsl.DispatcherSelectorSpec

class ClusterDispatcherSelectorSpec
    extends DispatcherSelectorSpec(ConfigFactory.parseString("""
    akka.actor.provider = cluster
    """).withFallback(DispatcherSelectorSpec.config)) {

  // same tests as in DispatcherSelectorSpec

}
