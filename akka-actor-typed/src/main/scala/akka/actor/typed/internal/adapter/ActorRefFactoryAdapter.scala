/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.adapter

import akka.ConfigurationException
import akka.actor.typed._
import akka.actor.typed.internal.CachedProps
import akka.annotation.InternalApi
import akka.util.ErrorMessages
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[typed] object ActorRefFactoryAdapter {

  private val remoteDeploymentNotAllowed = "Remote deployment not allowed for typed actors"

  private def classicPropsFor[T](behavior: Behavior[T], props: Props, rethrowTypedFailure: Boolean): akka.actor.Props =
    behavior._internalClassicPropsCache match {
      case OptionVal.Some(cachedProps)
          if (cachedProps.typedProps eq props) && cachedProps.rethrowTypedFailure == rethrowTypedFailure =>
        cachedProps.adaptedProps
      case _ =>
        val adapted =
          internal.adapter.PropsAdapter(() => Behavior.validateAsInitial(behavior), props, rethrowTypedFailure)
        // we only optimistically cache the last seen typed props instance, since for most scenarios
        // with large numbers of actors, they will be of the same type and the same props
        behavior._internalClassicPropsCache = OptionVal.Some(CachedProps(props, adapted, rethrowTypedFailure))
        adapted
    }

  def spawnAnonymous[T](
      context: akka.actor.ActorRefFactory,
      behavior: Behavior[T],
      props: Props,
      rethrowTypedFailure: Boolean): ActorRef[T] = {
    try {
      ActorRefAdapter(context.actorOf(classicPropsFor(behavior, props, rethrowTypedFailure)))
    } catch {
      case ex: ConfigurationException if ex.getMessage.startsWith(ErrorMessages.RemoteDeploymentConfigErrorPrefix) =>
        throw new ConfigurationException(remoteDeploymentNotAllowed, ex)
    }
  }

  def spawn[T](
      actorRefFactory: akka.actor.ActorRefFactory,
      behavior: Behavior[T],
      name: String,
      props: Props,
      rethrowTypedFailure: Boolean): ActorRef[T] = {
    try {
      ActorRefAdapter(actorRefFactory.actorOf(classicPropsFor(behavior, props, rethrowTypedFailure), name))
    } catch {
      case ex: ConfigurationException if ex.getMessage.startsWith(ErrorMessages.RemoteDeploymentConfigErrorPrefix) =>
        throw new ConfigurationException(remoteDeploymentNotAllowed, ex)
    }
  }
}
