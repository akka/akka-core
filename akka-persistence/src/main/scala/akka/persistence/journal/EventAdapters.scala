/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Try

import com.typesafe.config.Config

import akka.actor.ExtendedActorSystem
import akka.event.{ Logging, LoggingAdapter }

/**
 * `EventAdapters` serves as a per-journal collection of bound event adapters.
 */
class EventAdapters(
    map: ConcurrentHashMap[Class[_], EventAdapter],
    bindings: immutable.Seq[(Class[_], EventAdapter)],
    log: LoggingAdapter) {

  /**
   * Finds the "most specific" matching adapter for the given class (i.e. it may return an adapter that can work on a
   * interface implemented by the given class if no direct match is found).
   *
   * Falls back to [[IdentityEventAdapter]] if no adapter was defined for the given class.
   */
  def get(clazz: Class[_]): EventAdapter = {
    map.get(clazz) match {
      case null => // bindings are ordered from most specific to least specific
        val value = bindings.filter {
          _._1.isAssignableFrom(clazz)
        } match {
          case (_, bestMatch) +: _ => bestMatch
          case _                   => IdentityEventAdapter
        }
        map.putIfAbsent(clazz, value) match {
          case null =>
            log.debug(s"Using EventAdapter: {} for event [{}]", value.getClass.getName, clazz.getName)
            value
          case some => some
        }
      case value => value
    }
  }

  override def toString =
    s"${getClass.getName}($map, $bindings)"

}

/** INTERNAL API */
private[akka] object EventAdapters {
  type Name = String
  type BoundAdapters = immutable.Seq[String]
  type FQN = String
  type ClassHandler = (Class[_], EventAdapter)

  def apply(system: ExtendedActorSystem, config: Config): EventAdapters = {
    val adapters = configToMap(config, "event-adapters")
    val adapterBindings = configToListMap(config, "event-adapter-bindings")
    if (adapters.isEmpty && adapterBindings.isEmpty)
      IdentityEventAdapters
    else
      apply(system, adapters, adapterBindings)
  }

  private def apply(
      system: ExtendedActorSystem,
      adapters: Map[Name, FQN],
      adapterBindings: Map[FQN, BoundAdapters]): EventAdapters = {

    val adapterNames = adapters.keys.toSet
    for {
      (fqn, boundToAdapters) <- adapterBindings
      boundAdapter <- boundToAdapters
    } require(
      adapterNames(boundAdapter.toString),
      s"$fqn was bound to undefined event-adapter: $boundAdapter (bindings: ${boundToAdapters
        .mkString("[", ", ", "]")}, known adapters: ${adapters.keys.mkString})")

    // A Map of handler from alias to implementation (i.e. class implementing akka.serialization.Serializer)
    // For example this defines a handler named 'country': `"country" -> com.example.comain.CountryTagsAdapter`
    val handlers = for ((k: String, v: String) <- adapters) yield k -> instantiateAdapter(v, system).get

    // bindings is a Seq of tuple representing the mapping from Class to handler.
    // It is primarily ordered by the most specific classes first, and secondly in the configured order.
    val bindings: immutable.Seq[ClassHandler] = {
      val bs =
        for ((k: FQN, as: BoundAdapters) <- adapterBindings)
          yield
            if (as.size == 1) (system.dynamicAccess.getClassFor[Any](k).get, handlers(as.head))
            else
              (
                system.dynamicAccess.getClassFor[Any](k).get,
                NoopWriteEventAdapter(CombinedReadEventAdapter(as.map(handlers))))

      sort(bs)
    }

    val backing = bindings.foldLeft(new ConcurrentHashMap[Class[_], EventAdapter]) {
      case (map, (c, s)) => map.put(c, s); map
    }

    new EventAdapters(backing, bindings, Logging(system, classOf[EventAdapters]))
  }

  def instantiateAdapter(adapterFQN: String, system: ExtendedActorSystem): Try[EventAdapter] = {
    val clazz = system.dynamicAccess.getClassFor[Any](adapterFQN).get
    if (classOf[EventAdapter].isAssignableFrom(clazz))
      instantiate[EventAdapter](adapterFQN, system)
    else if (classOf[WriteEventAdapter].isAssignableFrom(clazz))
      instantiate[WriteEventAdapter](adapterFQN, system).map(NoopReadEventAdapter.apply)
    else if (classOf[ReadEventAdapter].isAssignableFrom(clazz))
      instantiate[ReadEventAdapter](adapterFQN, system).map(NoopWriteEventAdapter.apply)
    else
      throw new IllegalArgumentException(s"Configured $adapterFQN does not implement any EventAdapter interface!")
  }

  /** INTERNAL API */
  private[akka] case class CombinedReadEventAdapter(adapters: immutable.Seq[EventAdapter]) extends EventAdapter {
    private def onlyReadSideException =
      new IllegalStateException("CombinedReadEventAdapter must not be used when writing (creating manifests) events!")
    override def manifest(event: Any): String = throw onlyReadSideException
    override def toJournal(event: Any): Any = throw onlyReadSideException

    override def fromJournal(event: Any, manifest: String): EventSeq =
      EventSeq(adapters.flatMap(_.fromJournal(event, manifest).events): _*) // TODO could we could make EventSeq flatMappable

    override def toString =
      s"CombinedReadEventAdapter(${adapters.map(_.getClass.getCanonicalName).mkString(",")})"
  }

  /**
   * Tries to load the specified Serializer by the fully-qualified name; the actual
   * loading is performed by the system’s [[akka.actor.DynamicAccess]].
   */
  private def instantiate[T: ClassTag](fqn: FQN, system: ExtendedActorSystem): Try[T] =
    system.dynamicAccess.createInstanceFor[T](fqn, List(classOf[ExtendedActorSystem] -> system)).recoverWith {
      case _: NoSuchMethodException => system.dynamicAccess.createInstanceFor[T](fqn, Nil)
    }

  /**
   * Sort so that subtypes always precede their supertypes, but without
   * obeying any order between unrelated subtypes (insert sort).
   */
  private def sort[T](in: Iterable[(Class[_], T)]): immutable.Seq[(Class[_], T)] =
    in.foldLeft(new ArrayBuffer[(Class[_], T)](in.size)) { (buf, ca) =>
        buf.indexWhere(_._1.isAssignableFrom(ca._1)) match {
          case -1 => buf.append(ca)
          case x  => buf.insert(x, ca)
        }
        buf
      }
      .to(immutable.Seq)

  private final def configToMap(config: Config, path: String): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    if (config.hasPath(path)) {
      config.getConfig(path).root.unwrapped.asScala.toMap.map { case (k, v) => k -> v.toString }
    } else Map.empty
  }

  private final def configToListMap(config: Config, path: String): Map[String, immutable.Seq[String]] = {
    import scala.jdk.CollectionConverters._
    if (config.hasPath(path)) {
      config.getConfig(path).root.unwrapped.asScala.toMap.map {
        case (k, v: util.ArrayList[_]) if v.isInstanceOf[util.ArrayList[_]] => k -> v.asScala.map(_.toString).toList
        case (k, v)                                                         => k -> List(v.toString)
      }
    } else Map.empty
  }

}

private[akka] case object IdentityEventAdapters extends EventAdapters(null, null, null) {
  override def get(clazz: Class[_]): EventAdapter = IdentityEventAdapter
  override def toString = Logging.simpleName(IdentityEventAdapters)
}
