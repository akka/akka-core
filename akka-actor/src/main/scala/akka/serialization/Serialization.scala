/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.NoSuchElementException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{ DynamicVariable, Failure, Try }
import scala.util.Success
import scala.util.control.NonFatal

import com.typesafe.config.Config

import akka.actor._
import akka.annotation.InternalApi
import akka.event.{ LogMarker, Logging, LoggingAdapter }

object Serialization {

  /**
   * Tuple that represents mapping from Class to Serializer
   */
  type ClassSerializer = (Class[_], Serializer)

  /**
   * INTERNAL API: This holds a reference to the current transport serialization information used for
   * serializing local actor refs, or if serializer library e.g. custom serializer/deserializer in
   * Jackson need access to the current `ActorSystem`.
   */
  @InternalApi private[akka] val currentTransportInformation = new DynamicVariable[Information](null)

  class Settings(val config: Config) {
    val Serializers: Map[String, String] = configToMap(config.getConfig("akka.actor.serializers"))
    val SerializationBindings: Map[String, String] = {
      val bindings = config.getConfig("akka.actor.serialization-bindings")
      configToMap(bindings)
    }

    private final def configToMap(cfg: Config): Map[String, String] = {
      import scala.jdk.CollectionConverters._
      cfg.root.unwrapped.asScala.toMap.map { case (k, v) => (k -> v.toString) }
    }
  }

  /**
   * The serialized path of an actorRef, based on the current transport serialization information.
   * If there is no external address available in the given `ActorRef` then the systems default
   * address will be used and that is retrieved from the ThreadLocal `Serialization.Information`
   * that was set with [[Serialization#withTransportInformation]].
   */
  def serializedActorPath(actorRef: ActorRef): String = {
    val path = actorRef.path
    val originalSystem: ExtendedActorSystem = actorRef match {
      case a: ActorRefWithCell => a.underlying.system.asInstanceOf[ExtendedActorSystem]
      case _                   => null
    }
    Serialization.currentTransportInformation.value match {
      case null =>
        originalSystem match {
          case null => path.toSerializationFormat
          case system =>
            try path.toSerializationFormatWithAddress(system.provider.getDefaultAddress)
            catch { case NonFatal(_) => path.toSerializationFormat }
        }
      case Information(address, system) =>
        if (originalSystem == null || originalSystem == system)
          path.toSerializationFormatWithAddress(address)
        else {
          val provider = originalSystem.provider
          path.toSerializationFormatWithAddress(
            provider.getExternalAddressFor(address).getOrElse(provider.getDefaultAddress))
        }
    }
  }

  /**
   * Serialization information needed for serializing local actor refs,
   * or if serializer library e.g. custom serializer/deserializer in Jackson need
   * access to the current `ActorSystem`.
   */
  final case class Information(address: Address, system: ActorSystem)

  /**
   * Sets serialization information in a `ThreadLocal` and runs `f`. The information is
   * needed for serializing local actor refs, or if serializer library e.g. custom serializer/deserializer
   * in Jackson need access to the current `ActorSystem`. The current [[Information]] can be accessed within
   * `f` via [[Serialization#getCurrentTransportInformation]].
   *
   * Akka Remoting sets this value when serializing and deserializing messages, and when using
   * the ordinary `serialize` and `deserialize` methods in [[Serialization]] the value is also
   * set automatically.
   *
   * @return value returned by `f`
   */
  def withTransportInformation[T](system: ExtendedActorSystem)(f: () => T): T = {
    val info = system.provider.serializationInformation
    if (Serialization.currentTransportInformation.value eq info)
      f() // already set
    else
      Serialization.currentTransportInformation.withValue(info) {
        f()
      }
  }

  /**
   * Gets the serialization information from a `ThreadLocal` that was assigned via
   * [[Serialization#withTransportInformation]]. The information is needed for serializing
   * local actor refs, or if serializer library e.g. custom serializer/deserializer
   * in Jackson need access to the current `ActorSystem`.
   *
   * @throws IllegalStateException if the information was not set
   */
  def getCurrentTransportInformation(): Information = {
    Serialization.currentTransportInformation.value match {
      case null =>
        throw new IllegalStateException(
          "currentTransportInformation is not set, use Serialization.withTransportInformation")
      case t => t
    }
  }

}

/**
 * Serialization module. Contains methods for serialization and deserialization as well as
 * locating a Serializer for a particular class as defined in the mapping in the configuration.
 */
class Serialization(val system: ExtendedActorSystem) extends Extension {
  import Serialization._

  val settings = new Settings(system.settings.config)
  val AllowJavaSerialization: Boolean = system.settings.AllowJavaSerialization

  private[this] val _log = Logging.withMarker(system, getClass.getName)
  val log: LoggingAdapter = _log
  private val manifestCache = new AtomicReference[Map[String, Option[Class[_]]]](Map.empty[String, Option[Class[_]]])

  /** INTERNAL API */
  @InternalApi private[akka] def serializationInformation: Serialization.Information =
    system.provider.serializationInformation

  private def withTransportInformation[T](f: () => T): T = {
    val oldInfo = Serialization.currentTransportInformation.value
    try {
      if (oldInfo eq null)
        Serialization.currentTransportInformation.value = serializationInformation
      f()
    } finally Serialization.currentTransportInformation.value = oldInfo
  }

  /**
   * Serializes the given AnyRef/java.lang.Object according to the Serialization configuration
   * to either an Array of Bytes or an Exception if one was thrown.
   */
  def serialize(o: AnyRef): Try[Array[Byte]] = {
    withTransportInformation { () =>
      Try(findSerializerFor(o).toBinary(o))
    }
  }

  /**
   * Deserializes the given array of bytes using the specified serializer id,
   * using the optional type hint to the Serializer.
   * Returns either the resulting object or an Exception if one was thrown.
   */
  @deprecated("Use deserialize that accepts the `manifest` as a class name.", since = "2.6.0")
  def deserialize[T](bytes: Array[Byte], serializerId: Int, clazz: Option[Class[_ <: T]]): Try[T] =
    Try {
      val serializer = try getSerializerById(serializerId)
      catch {
        case _: NoSuchElementException =>
          throw new NotSerializableException(
            s"Cannot find serializer with id [$serializerId]${clazz.map(c => " (class [" + c.getName + "])").getOrElse("")}. " +
            "The most probable reason is that the configuration entry " +
            "akka.actor.serializers is not in sync between the two systems.")
      }
      withTransportInformation { () =>
        serializer.fromBinary(bytes, clazz).asInstanceOf[T]
      }
    }

  /**
   * Deserializes the given array of bytes using the specified serializer id,
   * using the optional type hint to the Serializer.
   * Returns either the resulting object or an Exception if one was thrown.
   */
  def deserialize(bytes: Array[Byte], serializerId: Int, manifest: String): Try[AnyRef] =
    Try {
      val serializer = try getSerializerById(serializerId)
      catch {
        case _: NoSuchElementException =>
          throw new NotSerializableException(
            s"Cannot find serializer with id [$serializerId] (manifest [$manifest]). The most probable reason is that the configuration entry " +
            "akka.actor.serializers is not in sync between the two systems.")
      }
      deserializeByteArray(bytes, serializer, manifest)
    }

  private def deserializeByteArray(bytes: Array[Byte], serializer: Serializer, manifest: String): AnyRef = {

    @tailrec def updateCache(cache: Map[String, Option[Class[_]]], key: String, value: Option[Class[_]]): Boolean = {
      manifestCache.compareAndSet(cache, cache.updated(key, value)) ||
      updateCache(manifestCache.get, key, value) // recursive, try again
    }

    withTransportInformation { () =>
      serializer match {
        case s2: SerializerWithStringManifest => s2.fromBinary(bytes, manifest)
        case s1 =>
          if (manifest == "")
            s1.fromBinary(bytes, None)
          else {
            val cache = manifestCache.get
            cache.get(manifest) match {
              case Some(cachedClassManifest) => s1.fromBinary(bytes, cachedClassManifest)
              case None =>
                system.dynamicAccess.getClassFor[AnyRef](manifest) match {
                  case Success(classManifest) =>
                    val classManifestOption: Option[Class[_]] = Some(classManifest)
                    updateCache(cache, manifest, classManifestOption)
                    s1.fromBinary(bytes, classManifestOption)
                  case Failure(_) =>
                    throw new NotSerializableException(
                      s"Cannot find manifest class [$manifest] for serializer with id [${serializer.identifier}].")
                }
            }
          }
      }
    }
  }

  /**
   * Deserializes the given ByteBuffer of bytes using the specified serializer id,
   * using the optional type hint to the Serializer.
   * Returns either the resulting object or throws an exception if deserialization fails.
   */
  @throws(classOf[NotSerializableException])
  def deserializeByteBuffer(buf: ByteBuffer, serializerId: Int, manifest: String): AnyRef = {
    val serializer = try getSerializerById(serializerId)
    catch {
      case _: NoSuchElementException =>
        throw new NotSerializableException(
          s"Cannot find serializer with id [$serializerId] (manifest [$manifest]). The most probable reason is that the configuration entry " +
          "akka.actor.serializers is not in synch between the two systems.")
    }

    // not using `withTransportInformation { () =>` because deserializeByteBuffer is supposed to be the
    // possibility for allocation free serialization
    val oldInfo = Serialization.currentTransportInformation.value
    try {
      if (oldInfo eq null)
        Serialization.currentTransportInformation.value = serializationInformation

      serializer match {
        case ser: ByteBufferSerializer =>
          ser.fromBinary(buf, manifest)
        case _ =>
          val bytes = new Array[Byte](buf.remaining())
          buf.get(bytes)
          deserializeByteArray(bytes, serializer, manifest)
      }
    } finally Serialization.currentTransportInformation.value = oldInfo
  }

  /**
   * Deserializes the given array of bytes using the specified type to look up what Serializer should be used.
   * Returns either the resulting object or an Exception if one was thrown.
   */
  def deserialize[T](bytes: Array[Byte], clazz: Class[T]): Try[T] = {
    withTransportInformation { () =>
      Try(serializerFor(clazz).fromBinary(bytes, Some(clazz)).asInstanceOf[T])
    }
  }

  /**
   * Returns the Serializer configured for the given object, returns the NullSerializer if it's null.
   *
   * Throws akka.ConfigurationException if no `serialization-bindings` is configured for the
   *   class of the object.
   */
  def findSerializerFor(o: AnyRef): Serializer =
    if (o eq null) NullSerializer else serializerFor(o.getClass)

  /**
   * Returns the configured Serializer for the given Class. The configured Serializer
   * is used if the configured class `isAssignableFrom` from the `clazz`, i.e.
   * the configured class is a super class or implemented interface. In case of
   * ambiguity it is primarily using the most specific configured class,
   * and secondly the entry configured first.
   *
   * Throws java.io.NotSerializableException if no `serialization-bindings` is configured for the class.
   */
  @throws(classOf[NotSerializableException])
  def serializerFor(clazz: Class[_]): Serializer =
    serializerMap.get(clazz) match {
      case null => // bindings are ordered from most specific to least specific
        def unique(possibilities: immutable.Seq[(Class[_], Serializer)]): Boolean =
          possibilities.size == 1 ||
          (possibilities.forall(_._1.isAssignableFrom(possibilities(0)._1))) ||
          (possibilities.forall(_._2 == possibilities(0)._2))

        val ser = {
          bindings.filter {
            case (c, _) => c.isAssignableFrom(clazz)
          } match {
            case immutable.Seq() =>
              throw new NotSerializableException(s"No configured serialization-bindings for class [${clazz.getName}]")
            case possibilities =>
              if (unique(possibilities))
                possibilities.head._2
              else {
                // give JavaSerializer lower priority if multiple serializers found
                val possibilitiesWithoutJavaSerializer = possibilities.filter {
                  case (_, _: JavaSerializer)         => false
                  case (_, _: DisabledJavaSerializer) => false
                  case _                              => true
                }
                if (possibilitiesWithoutJavaSerializer.isEmpty) {
                  // shouldn't happen
                  throw new NotSerializableException(
                    s"More than one JavaSerializer configured for class [${clazz.getName}]")
                }

                if (!unique(possibilitiesWithoutJavaSerializer)) {
                  _log.warning(
                    LogMarker.Security,
                    "Multiple serializers found for [{}], choosing first of: [{}]",
                    clazz.getName,
                    possibilitiesWithoutJavaSerializer.map { case (_, s) => s.getClass.getName }.mkString(", "))
                }
                possibilitiesWithoutJavaSerializer.head._2

              }

          }
        }

        serializerMap.putIfAbsent(clazz, ser) match {
          case null =>
            if (shouldWarnAboutJavaSerializer(clazz, ser)) {
              _log.warning(
                LogMarker.Security,
                "Using the Java serializer for class [{}] which is not recommended because of " +
                "performance implications. Use another serializer or disable this warning using the setting " +
                "'akka.actor.warn-about-java-serializer-usage'",
                clazz.getName)
            }

            if (!warnUnexpectedNonAkkaSerializer(clazz, ser))
              log.debug("Using serializer [{}] for message [{}]", ser.getClass.getName, clazz.getName)

            ser
          case some => some
        }
      case ser => ser
    }

  /**
   * Tries to load the specified Serializer by the fully-qualified name; the actual
   * loading is performed by the system’s [[akka.actor.DynamicAccess]].
   */
  def serializerOf(serializerFQN: String): Try[Serializer] = {
    serializerOf(bindingName = "", serializerFQN) // for backwards compatibility since it's a public method
  }

  /**
   * Tries to load the specified Serializer by the fully-qualified name; the actual
   * loading is performed by the system’s [[akka.actor.DynamicAccess]].
   */
  private def serializerOf(bindingName: String, serializerFQN: String): Try[Serializer] = {
    // We override each instantiation of the JavaSerializer with the "disabled" serializer which will log warnings if used.
    val fqn =
      if (!system.settings.AllowJavaSerialization && serializerFQN == classOf[JavaSerializer].getName) {
        log.debug(
          "Replacing JavaSerializer with DisabledJavaSerializer, " +
          "due to `akka.actor.allow-java-serialization = off`.")
        classOf[DisabledJavaSerializer].getName
      } else serializerFQN

    system.dynamicAccess.createInstanceFor[Serializer](fqn, List(classOf[ExtendedActorSystem] -> system)).recoverWith {
      case _: NoSuchMethodException =>
        system.dynamicAccess.createInstanceFor[Serializer](fqn, List(classOf[ActorSystem] -> system)).recoverWith {
          case _: NoSuchMethodException =>
            system.dynamicAccess
              .createInstanceFor[Serializer](fqn, List(classOf[ClassicActorSystemProvider] -> system))
              .recoverWith {
                case _: NoSuchMethodException =>
                  system.dynamicAccess.createInstanceFor[Serializer](fqn, Nil).recoverWith {
                    case _: NoSuchMethodException =>
                      if (bindingName == "") {
                        // compatibility with (public) serializerOf method without bindingName
                        throw new NoSuchMethodException(s"The serializer [$fqn] doesn't have a matching constructor, " +
                        s"see API documentation of ${classOf[Serializer].getName}")
                      } else
                        system.dynamicAccess
                          .createInstanceFor[Serializer](
                            fqn,
                            List(classOf[ExtendedActorSystem] -> system, classOf[String] -> bindingName))
                          .recoverWith {
                            case _: NoSuchMethodException =>
                              system.dynamicAccess
                                .createInstanceFor[Serializer](
                                  fqn,
                                  List(classOf[ActorSystem] -> system, classOf[String] -> bindingName))
                                .recoverWith {
                                  case _: NoSuchMethodException =>
                                    system.dynamicAccess
                                      .createInstanceFor[Serializer](
                                        fqn,
                                        List(
                                          classOf[ClassicActorSystemProvider] -> system,
                                          classOf[String] -> bindingName))
                                      .recoverWith {
                                        case _: NoSuchMethodException =>
                                          Failure(new NoSuchMethodException(
                                            s"The serializer [$fqn] for binding [$bindingName] doesn't have a matching " +
                                            s"constructor, see API documentation of ${classOf[Serializer].getName}"))
                                      }
                                }
                          }
                  }
              }
        }
    }
  }

  /**
   * Programmatically defined serializers
   */
  private val serializerDetails: immutable.Seq[SerializerDetails] =
    (system.settings.setup.get[SerializationSetup] match {
      case None          => Vector.empty
      case Some(setting) => setting.createSerializers(system)
    }).collect {
      case det: SerializerDetails if isDisallowedJavaSerializer(det.serializer) =>
        log.debug(
          "Replacing JavaSerializer with DisabledJavaSerializer, " +
          "due to `akka.actor.allow-java-serialization = off`.")
        SerializerDetails(det.alias, new DisabledJavaSerializer(system), det.useFor)
      case det => det
    }

  /**
   * A Map of serializer from alias to implementation (class implementing akka.serialization.Serializer)
   * By default always contains the following mapping: "java" -> akka.serialization.JavaSerializer
   */
  private val serializers: Map[String, Serializer] = {
    val fromConfig = for ((k: String, v: String) <- settings.Serializers) yield k -> serializerOf(k, v).get
    val result = fromConfig ++ serializerDetails.map(d => d.alias -> d.serializer)
    ensureOnlyAllowedSerializers(result.iterator.map { case (_, ser) => ser })
    result
  }

  /**
   *  bindings is a Seq of tuple representing the mapping from Class to Serializer.
   *  It is primarily ordered by the most specific classes first, and secondly in the configured order.
   */
  private[akka] val bindings: immutable.Seq[ClassSerializer] = {
    val fromConfig = for {
      (className: String, alias: String) <- settings.SerializationBindings
      if alias != "none" && checkGoogleProtobuf(className) && checkAkkaProtobuf(className) && checkScalaPb(className)
    } yield (system.dynamicAccess.getClassFor[Any](className).get, serializers(alias))

    val fromSettings = serializerDetails.flatMap { detail =>
      detail.useFor.map(clazz => clazz -> detail.serializer)
    }

    val result = sort(fromConfig ++ fromSettings)
    ensureOnlyAllowedSerializers(result.iterator.map { case (_, ser) => ser })
    result.foreach { case (clazz, ser) => warnUnexpectedNonAkkaSerializer(clazz, ser) }
    result
  }

  private def ensureOnlyAllowedSerializers(iter: Iterator[Serializer]): Unit = {
    if (!system.settings.AllowJavaSerialization) {
      require(iter.forall(!isDisallowedJavaSerializer(_)), "Disallowed JavaSerializer binding.")
    }
  }

  private def warnUnexpectedNonAkkaSerializer(clazz: Class[_], ser: Serializer): Boolean = {
    val className = clazz.getName
    if (className.startsWith("akka.") && !ser.getClass.getName.startsWith("akka.") &&
        // no serializers for these in Akka
        !(className.startsWith("akka.grpc") || className.startsWith("akka.http") || className.startsWith(
          "akka.runtime"))) {
      log.warning(
        "Using serializer [{}] for message [{}]. Note that this serializer " +
        "is not implemented by Akka. It's not recommended to replace serializers for messages " +
        "provided by Akka.",
        ser.getClass.getName,
        clazz.getName)
      true
    } else false
  }

  // com.google.protobuf serialization binding is only used if the class can be loaded,
  // i.e. com.google.protobuf dependency has been added in the application project.
  // The reason for this special case is for backwards compatibility so that we still can
  // include "com.google.protobuf.GeneratedMessage" = proto in configured serialization-bindings.
  private def checkGoogleProtobuf(className: String): Boolean = checkClass("com.google.protobuf", className)

  // akka-protobuf is now not a dependency of remote so only load if user has explicitly added it
  // remove in 2.7
  private def checkAkkaProtobuf(className: String): Boolean = checkClass("akka.protobuf", className)

  // for convenience scalapb.GeneratedMessage is defined in reference.conf, but might not be included in classpath
  private def checkScalaPb(className: String): Boolean = checkClass("scalapb.GeneratedMessage", className)

  private def checkClass(prefix: String, className: String): Boolean =
    !className.startsWith(prefix) || system.dynamicAccess.getClassFor[Any](className).isSuccess

  /**
   * Sort so that subtypes always precede their supertypes, but without
   * obeying any order between unrelated subtypes (insert sort).
   */
  private def sort(in: Iterable[ClassSerializer]): immutable.Seq[ClassSerializer] =
    (in
      .foldLeft(new ArrayBuffer[ClassSerializer](in.size)) { (buf, ca) =>
        buf.indexWhere(_._1.isAssignableFrom(ca._1)) match {
          case -1 => buf.append(ca)
          case x  => buf.insert(x, ca)
        }
        buf
      })
      .to(immutable.Seq)

  /**
   * serializerMap is a Map whose keys is the class that is serializable and values is the serializer
   * to be used for that class.
   */
  private val serializerMap: ConcurrentHashMap[Class[_], Serializer] =
    bindings.foldLeft(new ConcurrentHashMap[Class[_], Serializer]) { case (map, (c, s)) => map.put(c, s); map }

  /**
   * Maps from a Serializer Identity (Int) to a Serializer instance (optimization)
   */
  val serializerByIdentity: Map[Int, Serializer] = {
    val zero: Map[Int, Serializer] = Map(NullSerializer.identifier -> NullSerializer)
    serializers.foldLeft(zero) {
      case (acc, (_, ser)) =>
        val id = ser.identifier
        acc.get(id) match {
          case Some(existing) if existing != ser =>
            throw new IllegalArgumentException(
              s"Serializer identifier [$id] of [${ser.getClass.getName}] " +
              s"is not unique. It is also used by [${acc(id).getClass.getName}].")
          case _ =>
            acc.updated(id, ser)
        }
    }
  }

  /**
   * Serializers with id 0 - 1023 are stored in an array for quick allocation free access
   */
  private val quickSerializerByIdentity: Array[Serializer] = {
    val size = 1024
    val table = new Array[Serializer](size)
    serializerByIdentity.foreach {
      case (id, ser) => if (0 <= id && id < size) table(id) = ser
    }
    table
  }

  /**
   * @throws `NoSuchElementException` if no serializer with given `id`
   */
  private def getSerializerById(id: Int): Serializer = {
    if (0 <= id && id < quickSerializerByIdentity.length) {
      quickSerializerByIdentity(id) match {
        case null => throw new NoSuchElementException(s"key not found: $id")
        case ser  => ser
      }
    } else
      serializerByIdentity(id)
  }

  private val isJavaSerializationWarningEnabled =
    settings.config.getBoolean("akka.actor.warn-about-java-serializer-usage")
  private val isWarningOnNoVerificationEnabled =
    settings.config.getBoolean("akka.actor.warn-on-no-serialization-verification")

  private def isDisallowedJavaSerializer(serializer: Serializer): Boolean = {
    serializer.isInstanceOf[JavaSerializer] && !system.settings.AllowJavaSerialization
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def shouldWarnAboutJavaSerializer(serializedClass: Class[_], serializer: Serializer) = {

    def suppressWarningOnNonSerializationVerification(serializedClass: Class[_]) = {
      //suppressed, only when warn-on-no-serialization-verification = off, and extending NoSerializationVerificationNeeded
      !isWarningOnNoVerificationEnabled && classOf[NoSerializationVerificationNeeded].isAssignableFrom(serializedClass)
    }

    isJavaSerializationWarningEnabled &&
    (serializer.isInstanceOf[JavaSerializer] || serializer.isInstanceOf[DisabledJavaSerializer]) &&
    !serializedClass.getName.startsWith("akka.") &&
    !serializedClass.getName.startsWith("java.lang.") &&
    !suppressWarningOnNonSerializationVerification(serializedClass)
  }

}
