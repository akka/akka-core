/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import scala.collection.immutable

import com.typesafe.config.{ Config, ConfigFactory }
import util.{ Failure, Success }

import akka.actor.{ Deploy, ExtendedActorSystem, NoScopeGiven, Props, Scope }
import akka.protobufv3.internal.ByteString
import akka.remote.ByteStringUtils
import akka.remote.DaemonMsgCreate
import akka.remote.WireFormats.{ DaemonMsgCreateData, DeployData, PropsData }
import akka.routing.{ NoRouter, RouterConfig }
import akka.serialization.{ BaseSerializer, SerializationExtension, SerializerWithStringManifest }
import scala.jdk.CollectionConverters._

/**
 * Serializes Akka's internal DaemonMsgCreate using protobuf
 * for the core structure of DaemonMsgCreate, Props and Deploy.
 * Serialization of contained RouterConfig, Config, and Scope
 * is done with configured serializer for those classes.
 *
 * INTERNAL API
 */
private[akka] final class DaemonMsgCreateSerializer(val system: ExtendedActorSystem) extends BaseSerializer {
  import Deploy.NoDispatcherGiven
  import ProtobufSerializer.deserializeActorRef
  import ProtobufSerializer.serializeActorRef

  private lazy val serialization = SerializationExtension(system)

  override val includeManifest: Boolean = false

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case DaemonMsgCreate(props, deploy, path, supervisor) =>
      def deployProto(d: Deploy): DeployData = {
        val builder = DeployData.newBuilder.setPath(d.path)

        {
          val (serId, _, manifest, bytes) = serialize(d.config)
          builder.setConfigSerializerId(serId)
          builder.setConfigManifest(manifest)
          builder.setConfig(ByteStringUtils.toProtoByteStringUnsafe(bytes))
        }

        if (d.routerConfig != NoRouter) {
          val (serId, _, manifest, bytes) = serialize(d.routerConfig)
          builder.setRouterConfigSerializerId(serId)
          builder.setRouterConfigManifest(manifest)
          builder.setRouterConfig(ByteStringUtils.toProtoByteStringUnsafe(bytes))
        }

        if (d.scope != NoScopeGiven) {
          val (serId, _, manifest, bytes) = serialize(d.scope)
          builder.setScopeSerializerId(serId)
          builder.setScopeManifest(manifest)
          builder.setScope(ByteStringUtils.toProtoByteStringUnsafe(bytes))
        }

        if (d.dispatcher != NoDispatcherGiven) {
          builder.setDispatcher(d.dispatcher)
        }
        if (d.tags.nonEmpty) {
          builder.addAllTags(d.tags.asJava)
        }
        builder.build
      }

      def propsProto = {
        val builder = PropsData.newBuilder.setClazz(props.clazz.getName).setDeploy(deployProto(props.deploy))
        props.args.foreach { arg =>
          val (serializerId, hasManifest, manifest, bytes) = serialize(arg)
          builder.addArgs(ByteStringUtils.toProtoByteStringUnsafe(bytes))
          builder.addManifests(manifest)
          builder.addSerializerIds(serializerId)
          builder.addHasManifest(hasManifest)
        }
        builder.build
      }

      DaemonMsgCreateData.newBuilder
        .setProps(propsProto)
        .setDeploy(deployProto(deploy))
        .setPath(path)
        .setSupervisor(serializeActorRef(supervisor))
        .build
        .toByteArray

    case _ =>
      throw new IllegalArgumentException(
        "Can't serialize a non-DaemonMsgCreate message using DaemonMsgCreateSerializer [%s]".format(obj))
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val proto = DaemonMsgCreateData.parseFrom(bytes)

    def deploy(protoDeploy: DeployData): Deploy = {

      val config =
        if (protoDeploy.hasConfig) {
          if (protoDeploy.hasConfigSerializerId) {
            serialization
              .deserialize(
                protoDeploy.getConfig.toByteArray,
                protoDeploy.getConfigSerializerId,
                protoDeploy.getConfigManifest)
              .get
              .asInstanceOf[Config]
          } else {
            // old wire format
            oldDeserialize(protoDeploy.getConfig, classOf[Config])
          }
        } else ConfigFactory.empty

      val routerConfig =
        if (protoDeploy.hasRouterConfig) {
          if (protoDeploy.hasRouterConfigSerializerId) {
            serialization
              .deserialize(
                protoDeploy.getRouterConfig.toByteArray,
                protoDeploy.getRouterConfigSerializerId,
                protoDeploy.getRouterConfigManifest)
              .get
              .asInstanceOf[RouterConfig]
          } else {
            // old wire format
            oldDeserialize(protoDeploy.getRouterConfig, classOf[RouterConfig])
          }
        } else NoRouter

      val scope =
        if (protoDeploy.hasScope) {
          if (protoDeploy.hasScopeSerializerId) {
            serialization
              .deserialize(
                protoDeploy.getScope.toByteArray,
                protoDeploy.getScopeSerializerId,
                protoDeploy.getScopeManifest)
              .get
              .asInstanceOf[Scope]
          } else {
            // old wire format
            oldDeserialize(protoDeploy.getScope, classOf[Scope])
          }
        } else NoScopeGiven
      val dispatcher =
        if (protoDeploy.hasDispatcher) protoDeploy.getDispatcher
        else NoDispatcherGiven

      val tags: Set[String] =
        if (protoDeploy.getTagsCount == 0) Set.empty
        else protoDeploy.getTagsList.iterator().asScala.toSet
      val deploy = Deploy(protoDeploy.getPath, config, routerConfig, scope, dispatcher)
      if (tags.isEmpty) deploy
      else deploy.withTags(tags)
    }

    def props = {
      import scala.jdk.CollectionConverters._
      val protoProps = proto.getProps
      val actorClass = system.dynamicAccess.getClassFor[AnyRef](protoProps.getClazz).get
      val args: Vector[AnyRef] =
        // message from a newer node always contains serializer ids and possibly a string manifest for each position
        if (protoProps.getSerializerIdsCount > 0) {
          for {
            idx <- (0 until protoProps.getSerializerIdsCount).toVector
          } yield {
            val manifest =
              if (protoProps.getHasManifest(idx)) protoProps.getManifests(idx)
              else ""
            serialization
              .deserialize(protoProps.getArgs(idx).toByteArray(), protoProps.getSerializerIds(idx), manifest)
              .get
          }
        } else {
          // message from an older node, which only provides data and class name
          // and never any serializer ids
          proto.getProps.getArgsList.asScala
            .zip(proto.getProps.getManifestsList.asScala)
            .iterator
            .map(oldDeserialize)
            .to(immutable.Vector)
        }
      Props(deploy(proto.getProps.getDeploy), actorClass, args)
    }

    DaemonMsgCreate(
      props = props,
      deploy = deploy(proto.getDeploy),
      path = proto.getPath,
      supervisor = deserializeActorRef(system, proto.getSupervisor))
  }

  private def serialize(any: Any): (Int, Boolean, String, Array[Byte]) = {
    val m = any.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(m)

    // this trixery is to retain backwards wire compatibility while at the same time
    // allowing for usage of serializers with string manifests
    val hasManifest = serializer.includeManifest
    val manifest = serializer match {
      case ser: SerializerWithStringManifest =>
        ser.manifest(m)
      case _ =>
        // we do include class name regardless to retain wire compatibility
        // with older nodes who expect manifest to be the class name
        if (m eq null) {
          "null"
        } else {
          val className = m.getClass.getName
          if (m.isInstanceOf[java.io.Serializable] && m.getClass.isSynthetic && className.contains("$Lambda$")) {
            // When the additional-protobuf serializers are not enabled
            // the serialization of the parameters is based on passing class name instead of
            // serializerId and manifest as we usually do. With Scala 2.12 the functions are generated as
            // lambdas and we can't use that load class from that name when deserializing
            classOf[java.io.Serializable].getName
          } else {
            className
          }
        }
    }

    (serializer.identifier, hasManifest, manifest, serializer.toBinary(m))
  }

  private def oldDeserialize(p: (ByteString, String)): AnyRef =
    oldDeserialize(p._1, p._2)

  private def oldDeserialize(data: ByteString, className: String): AnyRef =
    if (data.isEmpty && className == "null") null
    else
      oldDeserialize[AnyRef](data, system.dynamicAccess.getClassFor[AnyRef](className).get.asInstanceOf[Class[AnyRef]])

  private def oldDeserialize[T](data: ByteString, clazz: Class[T]): T = {
    val bytes = data.toByteArray
    serialization.deserialize(bytes, clazz) match {
      case Success(x) =>
        if (clazz.isInstance(x)) x
        else throw new IllegalArgumentException("Can't deserialize to [%s], got [%s]".format(clazz.getName, x))
      case Failure(e) =>
        // Fallback to the java serializer, because some interfaces don't implement java.io.Serializable,
        // but the impl instance does. This could be optimized by adding java serializers in reference.conf:
        // com.typesafe.config.Config
        // akka.routing.RouterConfig
        // akka.actor.Scope
        serialization.deserialize(bytes, classOf[java.io.Serializable]) match {
          case Success(x) =>
            if (clazz.isInstance(x)) x.asInstanceOf[T]
            else throw e
          case _ => throw e // the first exception
        }
    }
  }

}
