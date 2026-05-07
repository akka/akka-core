/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.setup

import java.util.Optional

import scala.annotation.varargs
import scala.jdk.OptionConverters._
import scala.reflect.ClassTag

import akka.annotation.InternalApi

/**
 * Marker supertype for a setup part that can be put inside [[ActorSystemSetup]], if a specific concrete setup
 * is not specified in the actor system setup that means defaults are used (usually from the config file) - no concrete
 * setup instance should be mandatory in the [[ActorSystemSetup]] that an actor system is created with.
 */
abstract class Setup {

  /**
   * Construct an [[ActorSystemSetup]] with this setup combined with another one. Allows for
   * fluent creation of settings. If `other` is a setting of the same concrete [[Setup]] as this
   * it will replace this.
   */
  final def and(other: Setup): ActorSystemSetup = ActorSystemSetup(this, other)

}

object ActorSystemSetup {

  val empty = new ActorSystemSetup(Map.empty)

  /**
   * Scala API: Create an [[ActorSystemSetup]] containing all the provided settings
   */
  def apply(settings: Setup*): ActorSystemSetup =
    new ActorSystemSetup(settings.map(s => s.getClass -> s).toMap)

  /**
   * Java API: Create an [[ActorSystemSetup]] containing all the provided settings
   */
  @varargs
  def create(settings: Setup*): ActorSystemSetup = apply(settings: _*)
}

/**
 * A set of setup settings for programmatic configuration of the actor system.
 *
 * Constructor is *Internal API*. Use the factory methods [[ActorSystemSetup#create]] and [[akka.actor.Actor#apply]] to create
 * instances.
 */
final class ActorSystemSetup private[akka] (@InternalApi private[akka] val setups: Map[Class[_], AnyRef]) {

  /**
   * Java API: Extract a concrete [[Setup]] of type `T` if it is defined in the settings.
   */
  def get[T <: Setup](clazz: Class[T]): Optional[T] = {
    setups.get(clazz).map(_.asInstanceOf[T]).toJava
  }

  /**
   * Scala API: Extract a concrete [[Setup]] of type `T` if it is defined in the settings.
   */
  def get[T <: Setup: ClassTag]: Option[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    setups.get(clazz).map(_.asInstanceOf[T])
  }

  /**
   * Add a concrete [[Setup]]. If a setting of the same concrete [[Setup]] already is
   * present it will be replaced.
   */
  def withSetup[T <: Setup](t: T): ActorSystemSetup = {
    new ActorSystemSetup(setups + (t.getClass -> t))
  }

  /**
   * alias for `withSetup` allowing for fluent combination of settings: `a and b and c`, where `a`, `b` and `c` are
   * concrete [[Setup]] instances. If a setting of the same concrete [[Setup]] already is
   * present it will be replaced.
   */
  def and[T <: Setup](t: T): ActorSystemSetup = withSetup(t)

  override def toString: String = s"""ActorSystemSettings(${setups.keys.map(_.getName).mkString(",")})"""
}
