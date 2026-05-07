/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.pubsub

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.scaladsl.Behaviors
import akka.util.PrettyDuration.PrettyPrintableDuration
import org.slf4j.LoggerFactory

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.JavaDurationOps
import scala.reflect.ClassTag

/**
 * Registry for [[Topic]]s. Keeps one topic actor instance of each requested topic name so that they can be shared
 * between all subscribers in the same actor system.
 *
 * Note that manually spawned [[Topic]] actors will not appear in the registry.
 */
object PubSub extends ExtensionId[PubSub] {
  override def createExtension(system: ActorSystem[_]): PubSub = new PubSub(system)

  def get(system: ActorSystem[_]): PubSub = apply(system)

  private object TopicJanitor {
    sealed trait Command
    final case class TopicStarted(topic: ActorRef[_], name: String) extends Command
    final case class TopicStopped(name: String) extends Command
    def apply(extension: PubSub): Behavior[Command] = Behaviors.receive {
      case (context, TopicStarted(topic, name)) =>
        context.watchWith(topic, TopicStopped(name))
        Behaviors.same
      case (_, TopicStopped(name)) =>
        extension.remove(name)
        Behaviors.same
    }
  }
}

final class PubSub(system: ActorSystem[_]) extends Extension {
  import PubSub.TopicJanitor

  private val log = LoggerFactory.getLogger(getClass)

  private val registry = new ConcurrentHashMap[String, (Class[Any], Option[FiniteDuration], ActorRef[Any])]()
  private val topicJanitor = system.systemActorOf(PubSub.TopicJanitor(this), "PubSubTopicJanitor")

  /**
   * Scala API: Spawn an actor with the given topic name or share an existing one if it is already running.
   *
   * Note that [[Topic]] actors manually started will not be part of this registry.
   *
   * @tparam T the type of messages the topic accepts for publishing and subscribing
   */
  def topic[T](name: String)(implicit classTag: ClassTag[T]): ActorRef[Topic.Command[T]] =
    topicFor[T](name, None)(classTag)

  /**
   * Scala API: Spawn an actor with the given topic name or share an existing one if it is already running.
   *
   * Note that [[Topic]] actors manually started will not be part of this registry.
   *
   * @tparam T the type of messages the topic accepts for publishing and subscribing
   * @param ttl If the topic is idle this long, shut the actor down. All calls to `topic` for the same topic name should
   *            use the same ttl.
   */
  def topic[T](name: String, ttl: FiniteDuration)(implicit classTag: ClassTag[T]): ActorRef[Topic.Command[T]] =
    topicFor[T](name, Some(ttl))(classTag)

  /**
   * Java API: Spawn an actor with the given topic name or share an existing one if it is already running.
   *
   * Note that [[Topic]] actors manually started will not be part of this registry.
   *
   * @param messageClass the type of messages the topic accepts for publishing and subscribing
   */
  def topic[T](messageClass: Class[T], name: String): ActorRef[Topic.Command[T]] =
    topicFor(name, None)(ClassTag(messageClass))

  /**
   * Scala API: Spawn an actor with the given topic name or share an existing one if it is already running.
   *
   * Note that [[Topic]] actors manually started will not be part of this registry.
   *
   * @tparam T the type of messages the topic accepts for publishing and subscribing
   * @param ttl If the topic is idle this long, shut the actor down. All calls to `topic` for the same topic name should
   *            use the same ttl.
   */
  def topic[T](messageClass: Class[T], name: String, ttl: java.time.Duration): ActorRef[Topic.Command[T]] =
    topicFor(name, Some(ttl.toScala))(ClassTag(messageClass))

  /**
   * Scala API: return the current set of running topics
   */
  def currentTopics: Set[String] = registry.keySet().asScala.toSet

  /**
   * Java API: return the current set of running topics
   */
  def getCurrentTopics(): java.util.Set[String] = registry.keySet().stream().collect(Collectors.toSet[String])

  private def topicFor[T](name: String, ttl: Option[FiniteDuration])(
      classTag: ClassTag[T]): ActorRef[Topic.Command[T]] = {
    val messageClass = classTag.runtimeClass
    val (classInRegistry, ttlInRegistry, actorRefInRegistry) =
      registry.computeIfAbsent(
        name,
        name => (messageClass.asInstanceOf[Class[Any]], ttl, spawnTopic(name, classTag, ttl)))
    if (classInRegistry != messageClass)
      throw new IllegalArgumentException(
        s"Trying to start topic [$name] with command class [$messageClass], but it was already started with a " +
        s"different command class [$classInRegistry]. Use unique topic names for all your topics.")
    if (ttlInRegistry != ttl)
      log.warn(
        "Asked for topic [{}] with TTL [{}] but already existing topic has a different TTL [{}]",
        name,
        ttl.map(_.pretty).getOrElse("none"),
        ttlInRegistry.map(_.pretty).getOrElse("none"))
    actorRefInRegistry.narrow
  }

  private def spawnTopic[T](name: String, classTag: ClassTag[T], ttl: Option[FiniteDuration]): ActorRef[Any] = {
    log.debug("Starting topic [{}] for message type [{}]", name, classTag.runtimeClass)
    val actorNameSafe = URLEncoder.encode(name, StandardCharsets.UTF_8)
    val topic = ttl match {
      case Some(ttl) => Topic(name, ttl)(classTag)
      case None      => Topic(name)(classTag)
    }
    val startedTopic = system.systemActorOf(topic, s"Topic-$actorNameSafe").unsafeUpcast[Any]
    topicJanitor ! TopicJanitor.TopicStarted(startedTopic, name)
    startedTopic
  }

  private def remove(name: String): Unit =
    registry.remove(name)

}
