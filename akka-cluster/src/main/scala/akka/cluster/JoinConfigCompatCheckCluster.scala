/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.{ immutable => im }

import com.typesafe.config.Config

import akka.annotation.InternalApi
import akka.cluster.sbr.SplitBrainResolverProvider

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JoinConfigCompatCheckCluster {
  private val DowningProviderPath = "akka.cluster.downing-provider-class"
  private val SbrStrategyPath = "akka.cluster.split-brain-resolver.active-strategy"

  private val AkkaSbrProviderClass = classOf[SplitBrainResolverProvider].getName
  private val LightbendSbrProviderClass = "com.lightbend.akka.sbr.SplitBrainResolverProvider"
}

/**
 * INTERNAL API
 */
@InternalApi
final class JoinConfigCompatCheckCluster extends JoinConfigCompatChecker {
  import JoinConfigCompatCheckCluster._

  override def requiredKeys: im.Seq[String] = List(DowningProviderPath, SbrStrategyPath)

  override def check(toCheck: Config, actualConfig: Config): ConfigValidation = {
    val toCheckDowningProvider = toCheck.getString(DowningProviderPath)
    val actualDowningProvider = actualConfig.getString(DowningProviderPath)
    val downingProviderResult =
      if (toCheckDowningProvider == actualDowningProvider || Set(toCheckDowningProvider, actualDowningProvider) == Set(
            AkkaSbrProviderClass,
            LightbendSbrProviderClass))
        Valid
      else
        JoinConfigCompatChecker.checkEquality(List(DowningProviderPath), toCheck, actualConfig)

    val sbrStrategyResult =
      if (toCheck.hasPath(SbrStrategyPath) && actualConfig.hasPath(SbrStrategyPath))
        JoinConfigCompatChecker.checkEquality(List(SbrStrategyPath), toCheck, actualConfig)
      else Valid

    downingProviderResult ++ sbrStrategyResult
  }
}
