/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization

import scala.collection.immutable

import akka.actor.ExtendedActorSystem
import akka.actor.setup.Setup
import scala.jdk.CollectionConverters._

object SerializationSetup {

  /**
   * Scala API: Programmatic definition of serializers
   * @param createSerializers create pairs of serializer and the set of classes it should be used for
   */
  def apply(createSerializers: ExtendedActorSystem => immutable.Seq[SerializerDetails]): SerializationSetup = {
    new SerializationSetup(createSerializers)
  }

  /**
   * Java API: Programmatic definition of serializers
   * @param createSerializers create pairs of serializer and the set of classes it should be used for
   */
  def create(createSerializers: akka.japi.Function[ExtendedActorSystem, java.util.List[SerializerDetails]])
      : SerializationSetup =
    apply(sys => createSerializers(sys).asScala.toVector)

}

/**
 * Setup for the serialization subsystem, constructor is *Internal API*, use factories in [[SerializationSetup]]
 */
final class SerializationSetup private (val createSerializers: ExtendedActorSystem => immutable.Seq[SerializerDetails])
    extends Setup

object SerializerDetails {

  /**
   * Scala API: factory for details about one programmatically setup serializer
   *
   * @param alias Register the serializer under this alias (this allows it to be used by bindings in the config)
   * @param useFor A set of classes or superclasses to bind to the serializer, selection works just as if
   *               the classes, the alias and the serializer had been in the config.
   */
  def apply(alias: String, serializer: Serializer, useFor: immutable.Seq[Class[_]]): SerializerDetails =
    new SerializerDetails(alias, serializer, useFor)

  /**
   * Java API: factory for details about one programmatically setup serializer
   *
   * @param alias Register the serializer under this alias (this allows it to be used by bindings in the config)
   * @param useFor A set of classes or superclasses to bind to the serializer, selection works just as if
   *               the classes, the alias and the serializer had been in the config.
   */
  def create(alias: String, serializer: Serializer, useFor: java.util.List[Class[_]]): SerializerDetails =
    apply(alias, serializer, useFor.asScala.toVector)

}

/**
 * Constructor is internal API: Use the factories [[SerializerDetails#create]] or [[SerializerDetails#apply]]
 * to construct
 */
final class SerializerDetails private (
    val alias: String,
    val serializer: Serializer,
    val useFor: immutable.Seq[Class[_]])
