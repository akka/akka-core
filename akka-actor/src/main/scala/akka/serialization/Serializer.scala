/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.NotSerializableException
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.Callable

import scala.util.DynamicVariable
import scala.util.control.NoStackTrace

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.LogMarker
import akka.event.Logging
import akka.util.ClassLoaderObjectInputStream

/**
 * A Serializer represents a bimap between an object and an array of bytes representing that object.
 *
 * Serializers are loaded using reflection during [[akka.actor.ActorSystem]]
 * start-up, where constructors are tried in order:
 *
 * <ul>
 * <li>taking exactly one argument of type [[akka.actor.ExtendedActorSystem]];
 * this should be the preferred one because all reflective loading of classes
 * during deserialization should use ExtendedActorSystem.dynamicAccess (see
 * [[akka.actor.DynamicAccess]])</li>
 * <li>taking exactly one argument of type [[akka.actor.ActorSystem]]
 * <li>taking exactly one argument of type [[akka.actor.ClassicActorSystemProvider]]
 * <li>without arguments</li>
 * <li>taking two arguments of type [[akka.actor.ExtendedActorSystem]] and `String` where the second `String` argument is the binding name
 * <li>taking two arguments of type [[akka.actor.ActorSystem]] and `String` where the second `String` argument is the binding name
 * <li>taking two arguments of type [[akka.actor.ClassicActorSystemProvider]] and `String` where the second `String` argument is the binding name
 * </ul>
 *
 * <b>Be sure to always use the </b>[[akka.actor.DynamicAccess]]<b> for loading classes!</b> This is necessary to
 * avoid strange match errors and inequalities which arise from different class loaders loading
 * the same class.
 */
trait Serializer {

  /**
   * Completely unique value to identify this implementation of Serializer, used to optimize network traffic.
   * Values from 0 to 40 are reserved for Akka internal usage.
   */
  def identifier: Int

  /**
   * Serializes the given object into an Array of Byte.
   *
   * Note that the array must not be mutated by the serializer after it has been returned.
   */
  def toBinary(o: AnyRef): Array[Byte]

  /**
   * Returns whether this serializer needs a manifest in the fromBinary method
   */
  def includeManifest: Boolean

  /**
   * Produces an object from an array of bytes, with an optional type-hint;
   * the class should be loaded using ActorSystem.dynamicAccess.
   */
  @throws(classOf[NotSerializableException])
  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef

  /**
   * Java API: deserialize without type hint
   */
  final def fromBinary(bytes: Array[Byte]): AnyRef = fromBinary(bytes, None)

  /**
   * Java API: deserialize with type hint
   */
  @throws(classOf[NotSerializableException])
  final def fromBinary(bytes: Array[Byte], clazz: Class[_]): AnyRef = fromBinary(bytes, Option(clazz))
}

object Serializers {

  // NOTE!!! If you change this method it is likely that DaemonMsgCreateSerializer.serialize needs the changes too.
  def manifestFor(s: Serializer, message: AnyRef): String = s match {
    case s2: SerializerWithStringManifest => s2.manifest(message)
    case _                                => if (s.includeManifest) message.getClass.getName else ""
  }

}

/**
 * A Serializer represents a bimap between an object and an array of bytes representing that object.
 *
 * For serialization of data that need to evolve over time the `SerializerWithStringManifest` is recommended instead
 * of [[Serializer]] because the manifest (type hint) is a `String` instead of a `Class`. That means
 * that the class can be moved/removed and the serializer can still deserialize old data by matching
 * on the `String`. This is especially useful for Akka Persistence.
 *
 * The manifest string can also encode a version number that can be used in `fromBinary` to
 * deserialize in different ways to migrate old data to new domain objects.
 *
 * If the data was originally serialized with [[Serializer]] and in a later version of the
 * system you change to `SerializerWithStringManifest` the manifest string will be the full class name if
 * you used `includeManifest=true`, otherwise it will be the empty string.
 *
 * Serializers are loaded using reflection during [[akka.actor.ActorSystem]]
 * start-up, where constructors are tried in order:
 *
 * <ul>
 * <li>taking exactly one argument of type [[akka.actor.ExtendedActorSystem]];
 * this should be the preferred one because all reflective loading of classes
 * during deserialization should use ExtendedActorSystem.dynamicAccess (see
 * [[akka.actor.DynamicAccess]])</li>
 * <li>taking exactly one argument of type [[akka.actor.ActorSystem]]
 * <li>taking exactly one argument of type [[akka.actor.ClassicActorSystemProvider]]
 * <li>without arguments</li>
 * <li>taking two arguments of type [[akka.actor.ExtendedActorSystem]] and `String` where the second `String` argument is the binding name
 * <li>taking two arguments of type [[akka.actor.ActorSystem]] and `String` where the second `String` argument is the binding name
 * <li>taking two arguments of type [[akka.actor.ClassicActorSystemProvider]] and `String` where the second `String` argument is the binding name
 * </ul>
 *
 * <b>Be sure to always use the </b>[[akka.actor.DynamicAccess]]<b> for loading classes!</b> This is necessary to
 * avoid strange match errors and inequalities which arise from different class loaders loading
 * the same class.
 */
abstract class SerializerWithStringManifest extends Serializer {

  /**
   * Completely unique value to identify this implementation of Serializer, used to optimize network traffic.
   * Values from 0 to 40 are reserved for Akka internal usage.
   */
  def identifier: Int

  final override def includeManifest: Boolean = true

  /**
   * Return the manifest (type hint) that will be provided in the fromBinary method.
   * Use `""` if manifest is not needed.
   */
  def manifest(o: AnyRef): String

  /**
   * Serializes the given object into an Array of Byte.
   *
   * Note that the array must not be mutated by the serializer after it has been returned.
   */
  def toBinary(o: AnyRef): Array[Byte]

  /**
   * Produces an object from an array of bytes, with an optional type-hint.
   *
   * It's recommended to throw `java.io.NotSerializableException` in `fromBinary`
   * if the manifest is unknown. This makes it possible to introduce new message
   * types and send them to nodes that don't know about them. This is typically
   * needed when performing rolling upgrades, i.e. running a cluster with mixed
   * versions for while. `NotSerializableException` is treated as a transient
   * problem in the TCP based remoting layer. The problem will be logged
   * and message is dropped. Other exceptions will tear down the TCP connection
   * because it can be an indication of corrupt bytes from the underlying transport.
   */
  @throws(classOf[NotSerializableException])
  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef

  final def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val manifestString = manifest match {
      case Some(c) => c.getName
      case None    => ""
    }
    fromBinary(bytes, manifestString)
  }

}

/**
 * Serializer between an object and a `ByteBuffer` representing that object.
 *
 * Implementations should typically extend [[SerializerWithStringManifest]] and
 * in addition to the `ByteBuffer` based `toBinary` and `fromBinary` methods also
 * implement the array based `toBinary` and `fromBinary` methods. The array based
 * methods will be used when `ByteBuffer` is not used, e.g. in Akka Persistence.
 *
 * Note that the array based methods can for example be implemented by delegation
 * like this:
 * {{{
 *   // you need to know the maximum size in bytes of the serialized messages
 *   val pool = new akka.io.DirectByteBufferPool(defaultBufferSize = 1024 * 1024, maxPoolEntries = 10)
 *
 *
 *  // Implement this method for compatibility with `SerializerWithStringManifest`.
 *  override def toBinary(o: AnyRef): Array[Byte] = {
 *    val buf = pool.acquire()
 *    try {
 *      toBinary(o, buf)
 *      buf.flip()
 *      val bytes = new Array[Byte](buf.remaining)
 *      buf.get(bytes)
 *      bytes
 *    } finally {
 *      pool.release(buf)
 *    }
 *  }
 *
 *  // Implement this method for compatibility with `SerializerWithStringManifest`.
 *  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
 *    fromBinary(ByteBuffer.wrap(bytes), manifest)
 *
 * }}}
 */
//#ByteBufferSerializer
trait ByteBufferSerializer {

  /**
   * Serializes the given object into the `ByteBuffer`.
   */
  def toBinary(o: AnyRef, buf: ByteBuffer): Unit

  /**
   * Produces an object from a `ByteBuffer`, with an optional type-hint;
   * the class should be loaded using ActorSystem.dynamicAccess.
   */
  @throws(classOf[NotSerializableException])
  def fromBinary(buf: ByteBuffer, manifest: String): AnyRef

}
//#ByteBufferSerializer

/**
 *  Base serializer trait with serialization identifiers configuration contract,
 *  when globally unique serialization identifier is configured in the `reference.conf`.
 */
trait BaseSerializer extends Serializer {

  /**
   *  Actor system which is required by most serializer implementations.
   */
  def system: ExtendedActorSystem

  /**
   * Configuration namespace of serialization identifiers in the `reference.conf`.
   *
   * Each serializer implementation must have an entry in the following format:
   * `akka.actor.serialization-identifiers."FQCN" = ID`
   * where `FQCN` is fully qualified class name of the serializer implementation
   * and `ID` is globally unique serializer identifier number.
   */
  final val SerializationIdentifiers = BaseSerializer.SerializationIdentifiers

  /**
   * Globally unique serialization identifier configured in the `reference.conf`.
   *
   * See [[Serializer.identifier]].
   */
  override val identifier: Int = identifierFromConfig

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def identifierFromConfig: Int =
    BaseSerializer.identifierFromConfig(getClass, system)
}
object BaseSerializer {

  /**
   * Configuration namespace of serialization identifiers in the `reference.conf`.
   *
   * Each serializer implementation must have an entry in the following format:
   * `akka.actor.serialization-identifiers."FQCN" = ID`
   * where `FQCN` is fully qualified class name of the serializer implementation
   * and `ID` is globally unique serializer identifier number.
   */
  final val SerializationIdentifiers = "akka.actor.serialization-identifiers"

  /** INTERNAL API */
  @InternalApi
  private[akka] def identifierFromConfig(clazz: Class[_], system: ExtendedActorSystem): Int =
    system.settings.config.getInt(s"""$SerializationIdentifiers."${clazz.getName}"""")

  /** INTERNAL API */
  @InternalApi
  private[akka] def identifierFromConfig(bindingName: String, system: ExtendedActorSystem): Int =
    system.settings.config.getInt(s"""$SerializationIdentifiers."$bindingName"""")
}

/**
 * Java API for creating a Serializer: make sure to include a constructor which
 * takes exactly one argument of type [[akka.actor.ExtendedActorSystem]], because
 * that is the preferred constructor which will be invoked when reflectively instantiating
 * the JSerializer (also possible with empty constructor).
 */
abstract class JSerializer extends Serializer {

  @throws(classOf[NotSerializableException])
  final def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
    fromBinaryJava(bytes, manifest.orNull)

  /**
   * This method must be implemented, manifest may be null.
   */
  protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef
}

object NullSerializer extends NullSerializer

object JavaSerializer {

  /**
   * This holds a reference to the current ActorSystem (the surrounding context)
   * during serialization and deserialization.
   *
   * If you are using Serializers yourself, outside of SerializationExtension,
   * you'll need to surround the serialization/deserialization with:
   *
   * JavaSerializer.currentSystem.withValue(system) {
   *   ...code...
   * }
   *
   * or
   *
   * JavaSerializer.currentSystem.withValue(system, callable)
   */
  val currentSystem = new CurrentSystem
  final class CurrentSystem extends DynamicVariable[ExtendedActorSystem](null) {

    /**
     * Java API: invoke the callable with the current system being set to the given value for this thread.
     *
     * @param value - the current value under the call to callable.call()
     * @param callable - the operation to be performed
     * @return the result of callable.call()
     */
    def withValue[S](value: ExtendedActorSystem, callable: Callable[S]): S = super.withValue[S](value)(callable.call)
  }
}

/**
 * This Serializer uses standard Java Serialization
 */
class JavaSerializer(val system: ExtendedActorSystem) extends BaseSerializer {
  if (!system.settings.AllowJavaSerialization)
    throw new DisabledJavaSerializer.JavaSerializationException(
      "Attempted creation of `JavaSerializer` while `akka.actor.allow-java-serialization = off` was set!")

  def includeManifest: Boolean = false

  def toBinary(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    JavaSerializer.currentSystem.withValue(system) { out.writeObject(o) }
    out.close()
    bos.toByteArray
  }

  @throws(classOf[NotSerializableException])
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val in = new ClassLoaderObjectInputStream(system.dynamicAccess.classLoader, new ByteArrayInputStream(bytes))
    val obj = JavaSerializer.currentSystem.withValue(system) { in.readObject }
    in.close()
    obj
  }
}

/**
 * This Serializer is used when `akka.actor.java-serialization = off`
 */
final case class DisabledJavaSerializer(system: ExtendedActorSystem) extends Serializer with ByteBufferSerializer {
  import DisabledJavaSerializer._

  // use same identifier as JavaSerializer, since it's a replacement
  override val identifier: Int = BaseSerializer.identifierFromConfig(classOf[JavaSerializer], system)

  private[this] val empty = Array.empty[Byte]

  private[this] val log = Logging.withMarker(system, classOf[DisabledJavaSerializer])

  def includeManifest: Boolean = false

  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = {
    log.warning(
      LogMarker.Security,
      "Outgoing message attempted to use Java Serialization even though `akka.actor.allow-java-serialization = off` was set! " +
      "Message type was: [{}]",
      o.getClass)
    throw IllegalSerialization
  }

  @throws(classOf[NotSerializableException])
  override def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    log.warning(
      LogMarker.Security,
      "Incoming message attempted to use Java Serialization even though `akka.actor.allow-java-serialization = off` was set!")
    throw IllegalDeserialization
  }

  @throws(classOf[NotSerializableException])
  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
    // we don't capture the manifest or mention it in the log as the default setting for includeManifest is set to false.
    log.warning(
      LogMarker.Security,
      "Incoming message attempted to use Java Serialization even though `akka.actor.allow-java-serialization = off` was set!")
    throw IllegalDeserialization
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    toBinary(o, null)
    empty // won't return, toBinary throws
  }

}

object DisabledJavaSerializer {
  final class JavaSerializationException(msg: String) extends RuntimeException(msg) with NoStackTrace
  final val IllegalSerialization = new JavaSerializationException(
    "Attempted to serialize message using Java serialization while `akka.actor.allow-java-serialization` was disabled. Check WARNING logs for more details.")
  final val IllegalDeserialization = new JavaSerializationException(
    "Attempted to deserialize message using Java serialization while `akka.actor.allow-java-serialization` was disabled. Check WARNING logs for more details.")
}

/**
 * This is a special Serializer that Serializes and deserializes nulls only
 */
class NullSerializer extends Serializer {
  val nullAsBytes = Array[Byte]()
  def includeManifest: Boolean = false
  def identifier = 0
  def toBinary(o: AnyRef): Array[Byte] = nullAsBytes
  @throws(classOf[NotSerializableException])
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = null
}

/**
 * This is a special Serializer that Serializes and deserializes byte arrays only,
 * (just returns the byte array unchanged/uncopied)
 */
class ByteArraySerializer(val system: ExtendedActorSystem) extends BaseSerializer with ByteBufferSerializer {

  def includeManifest: Boolean = false
  def toBinary(o: AnyRef): Array[Byte] = o match {
    case null           => null
    case o: Array[Byte] => o
    case other =>
      throw new IllegalArgumentException(
        s"${getClass.getName} only serializes byte arrays, not [${other.getClass.getName}]")
  }

  @throws(classOf[NotSerializableException])
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = bytes

  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit =
    o match {
      case null               =>
      case bytes: Array[Byte] => buf.put(bytes)
      case other =>
        throw new IllegalArgumentException(
          s"${getClass.getName} only serializes byte arrays, not [${other.getClass.getName}]")
    }

  @throws(classOf[NotSerializableException])
  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
    val bytes = new Array[Byte](buf.remaining())
    buf.get(bytes)
    bytes
  }
}
