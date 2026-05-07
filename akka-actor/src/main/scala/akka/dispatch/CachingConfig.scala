/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.lang.Enum
import java.util
import java.util.concurrent.{ ConcurrentHashMap, TimeUnit }

import scala.util.{ Failure, Success, Try }

import com.typesafe.config._

/**
 * INTERNAL API
 */
private[akka] object CachingConfig {
  val emptyConfig = ConfigFactory.empty()

  sealed abstract trait PathEntry {
    val valid: Boolean
    val exists: Boolean
    val config: Config
  }
  final case class ValuePathEntry(valid: Boolean, exists: Boolean, config: Config = emptyConfig) extends PathEntry
  final case class StringPathEntry(valid: Boolean, exists: Boolean, config: Config, value: String) extends PathEntry

  val invalidPathEntry = ValuePathEntry(valid = false, exists = true)
  val nonExistingPathEntry = ValuePathEntry(valid = true, exists = false)
  val emptyPathEntry = ValuePathEntry(valid = true, exists = true)
}

/**
 * INTERNAL API
 *
 * A CachingConfig is a Config that wraps another Config and is used to cache path lookup and string
 * retrieval, which we happen to do a lot in some critical paths of the actor creation and mailbox
 * selection code.
 *
 * All other Config operations are delegated to the wrapped Config.
 */
private[akka] class CachingConfig(_config: Config) extends Config {

  import CachingConfig._

  private val (config: Config, entryMap: ConcurrentHashMap[String, PathEntry]) = _config match {
    case cc: CachingConfig => (cc.config, cc.entryMap)
    case _                 => (_config, new ConcurrentHashMap[String, PathEntry])
  }

  private def getPathEntry(path: String): PathEntry = entryMap.get(path) match {
    case null =>
      val ne = Try { config.hasPath(path) } match {
        case Failure(_)     => invalidPathEntry
        case Success(false) => nonExistingPathEntry
        case _ =>
          Try { config.getValue(path) } match {
            case Failure(_) =>
              emptyPathEntry
            case Success(v) =>
              if (v.valueType() == ConfigValueType.STRING)
                StringPathEntry(valid = true, exists = true, v.atKey("cached"), v.unwrapped().asInstanceOf[String])
              else
                ValuePathEntry(valid = true, exists = true, v.atKey("cached"))
          }
      }

      entryMap.putIfAbsent(path, ne) match {
        case null => ne
        case e    => e
      }

    case e => e
  }

  def checkValid(reference: Config, restrictToPaths: String*): Unit = {
    config.checkValid(reference, restrictToPaths: _*)
  }

  def root() = config.root()

  def origin() = config.origin()

  def withFallback(other: ConfigMergeable) = new CachingConfig(config.withFallback(other))

  def resolve() = resolve(ConfigResolveOptions.defaults())

  def resolve(options: ConfigResolveOptions) = {
    val resolved = config.resolve(options)
    if (resolved eq config) this
    else new CachingConfig(resolved)
  }

  def hasPath(path: String) = {
    val entry = getPathEntry(path)
    if (entry.valid)
      entry.exists
    else // run the real code to get proper exceptions
      config.hasPath(path)
  }

  def hasPathOrNull(path: String): Boolean = config.hasPathOrNull(path)

  def isEmpty = config.isEmpty

  def entrySet() = config.entrySet()

  def getBoolean(path: String) = config.getBoolean(path)

  def getNumber(path: String) = config.getNumber(path)

  def getInt(path: String) = config.getInt(path)

  def getLong(path: String) = config.getLong(path)

  def getDouble(path: String) = config.getDouble(path)

  def getString(path: String) = {
    getPathEntry(path) match {
      case StringPathEntry(_, _, _, string) =>
        string
      case e => e.config.getString("cached")
    }
  }

  def getObject(path: String) = config.getObject(path)

  def getConfig(path: String) = config.getConfig(path)

  def getAnyRef(path: String) = config.getAnyRef(path)

  def getValue(path: String) = config.getValue(path)

  def getBytes(path: String) = config.getBytes(path)

  def getMilliseconds(path: String) = config.getDuration(path, TimeUnit.MILLISECONDS)

  def getNanoseconds(path: String) = config.getDuration(path, TimeUnit.NANOSECONDS)

  def getList(path: String) = config.getList(path)

  def getBooleanList(path: String) = config.getBooleanList(path)

  def getNumberList(path: String) = config.getNumberList(path)

  def getIntList(path: String) = config.getIntList(path)

  def getLongList(path: String) = config.getLongList(path)

  def getDoubleList(path: String) = config.getDoubleList(path)

  def getStringList(path: String) = config.getStringList(path)

  def getObjectList(path: String) = config.getObjectList(path)

  def getConfigList(path: String) = config.getConfigList(path)

  def getAnyRefList(path: String) = config.getAnyRefList(path)

  def getBytesList(path: String) = config.getBytesList(path)

  def getMillisecondsList(path: String) = config.getDurationList(path, TimeUnit.MILLISECONDS)

  def getNanosecondsList(path: String) = config.getDurationList(path, TimeUnit.NANOSECONDS)

  def withOnlyPath(path: String) = new CachingConfig(config.withOnlyPath(path))

  def withoutPath(path: String) = new CachingConfig(config.withoutPath(path))

  def atPath(path: String) = new CachingConfig(config.atPath(path))

  def atKey(key: String) = new CachingConfig(config.atKey(key))

  def withValue(path: String, value: ConfigValue) = new CachingConfig(config.withValue(path, value))

  def getDuration(path: String, unit: TimeUnit) = config.getDuration(path, unit)

  def getDurationList(path: String, unit: TimeUnit) = config.getDurationList(path, unit)

  def getDuration(path: String): java.time.Duration = config.getDuration(path)

  def getDurationList(path: String) = config.getDurationList(path)

  def getPeriod(path: String) = config.getPeriod(path)

  def getTemporal(path: String) = config.getTemporal(path)

  def getIsNull(path: String): Boolean = config.getIsNull(path)

  def getMemorySize(path: String) = config.getMemorySize(path)

  def getMemorySizeList(path: String) = config.getMemorySizeList(path)

  def isResolved() = config.isResolved()

  def resolveWith(source: Config, options: ConfigResolveOptions) = config.resolveWith(source, options)

  def resolveWith(source: Config) = config.resolveWith(source)

  override def getEnumList[T <: Enum[T]](enumClass: Class[T], path: String): util.List[T] =
    config.getEnumList(enumClass, path)

  override def getEnum[T <: Enum[T]](enumClass: Class[T], path: String): T = config.getEnum(enumClass, path)

}
