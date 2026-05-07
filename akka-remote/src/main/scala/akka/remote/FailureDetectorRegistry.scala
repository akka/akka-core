/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import com.typesafe.config.Config

import akka.ConfigurationException
import akka.actor.{ ActorContext, ActorSystem, ExtendedActorSystem }
import akka.event.EventStream

/**
 * Interface for a registry of Akka failure detectors. New resources are implicitly registered when heartbeat is first
 * called with the resource given as parameter.
 *
 * type parameter A:
 *  - The type of the key that identifies a resource to be monitored by a failure detector
 */
trait FailureDetectorRegistry[A] {

  /**
   * Returns true if the resource is considered to be up and healthy and returns false otherwise.
   * For unregistered resources it returns true.
   */
  def isAvailable(resource: A): Boolean

  /**
   * Returns true if the failure detector has received any heartbeats and started monitoring
   * of the resource.
   */
  def isMonitoring(resource: A): Boolean

  /**
   * Records a heartbeat for a resource. If the resource is not yet registered (i.e. this is the first heartbeat) then
   * it is automatically registered.
   */
  def heartbeat(resource: A): Unit

  /**
   * Removes the heartbeat management for a resource.
   */
  def remove(resource: A): Unit

  /**
   * Removes all resources and any associated failure detector state.
   */
  def reset(): Unit
}

/**
 * INTERNAL API
 *
 * Utility class to create [[FailureDetector]] instances reflectively.
 */
private[akka] object FailureDetectorLoader {

  /**
   * Loads and instantiates a given [[FailureDetector]] implementation. The class to be loaded must have a constructor
   * that accepts a [[com.typesafe.config.Config]] and an [[akka.event.EventStream]] parameter. Will throw ConfigurationException
   * if the implementation cannot be loaded.
   *
   * @param fqcn Fully qualified class name of the implementation to be loaded.
   * @param config Configuration that will be passed to the implementation
   * @param system ActorSystem to be used for loading the implementation
   * @return A configured instance of the given [[FailureDetector]] implementation
   */
  def load(fqcn: String, config: Config, system: ActorSystem): FailureDetector = {
    system
      .asInstanceOf[ExtendedActorSystem]
      .dynamicAccess
      .createInstanceFor[FailureDetector](
        fqcn,
        List(classOf[Config] -> config, classOf[EventStream] -> system.eventStream))
      .recover({
        case e =>
          throw new ConfigurationException(s"Could not create custom failure detector [$fqcn] due to: ${e.toString}", e)
      })
      .get
  }

  /**
   * Loads and instantiates a given [[FailureDetector]] implementation. The class to be loaded must have a constructor
   * that accepts a [[com.typesafe.config.Config]] and an [[akka.event.EventStream]] parameter. Will throw ConfigurationException
   * if the implementation cannot be loaded. Use [[FailureDetectorLoader#load]] if no implicit [[akka.actor.ActorContext]] is
   * available.
   *
   * @param fqcn Fully qualified class name of the implementation to be loaded.
   * @param config Configuration that will be passed to the implementation
   * @return
   */
  def apply(fqcn: String, config: Config)(implicit ctx: ActorContext) = load(fqcn, config, ctx.system)

}
