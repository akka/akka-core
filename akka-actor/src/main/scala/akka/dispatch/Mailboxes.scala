/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.lang.reflect.ParameterizedType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import com.typesafe.config.{ Config, ConfigFactory }

import akka.ConfigurationException
import akka.actor.{ Actor, ActorRef, ActorSystem, DeadLetter, Deploy, DynamicAccess, Props }
import akka.annotation.InternalStableApi
import akka.dispatch.sysmsg.{
  EarliestFirstSystemMessageList,
  LatestFirstSystemMessageList,
  SystemMessage,
  SystemMessageList
}
import akka.event.EventStream
import akka.event.Logging.Warning
import akka.util.Reflect

object Mailboxes {
  final val DefaultMailboxId = "akka.actor.default-mailbox"
  final val NoMailboxRequirement = ""
  final val BoundedCapacityPrefix = "bounded-capacity:"
}

private[akka] class Mailboxes(
    val settings: ActorSystem.Settings,
    val eventStream: EventStream,
    dynamicAccess: DynamicAccess,
    deadLetters: ActorRef) {

  import Mailboxes._

  val deadLetterMailbox: Mailbox = new Mailbox(new MessageQueue {
    def enqueue(receiver: ActorRef, envelope: Envelope): Unit = onDeadLetterMailboxEnqueue(receiver, envelope)
    def dequeue() = null
    def hasMessages = false
    def numberOfMessages = 0
    def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = ()
  }) {
    becomeClosed()
    def systemEnqueue(receiver: ActorRef, handle: SystemMessage): Unit =
      deadLetters ! DeadLetter(handle, receiver, receiver)
    def systemDrain(newContents: LatestFirstSystemMessageList): EarliestFirstSystemMessageList = SystemMessageList.ENil
    def hasSystemMessages = false
  }

  @InternalStableApi
  private[akka] def onDeadLetterMailboxEnqueue(receiver: ActorRef, envelope: Envelope): Unit = envelope.message match {
    case _: DeadLetter => // actor subscribing to DeadLetter, drop it
    case msg           => deadLetters.tell(DeadLetter(msg, envelope.sender, receiver), envelope.sender)
  }

  private val mailboxTypeConfigurators = new ConcurrentHashMap[String, MailboxType]

  private val mailboxBindings: Map[Class[_ <: Any], String] = {
    import scala.jdk.CollectionConverters._
    settings.config
      .getConfig("akka.actor.mailbox.requirements")
      .root
      .unwrapped
      .asScala
      .toMap
      .foldLeft(Map.empty[Class[_ <: Any], String]) {
        case (m, (k, v)) =>
          dynamicAccess
            .getClassFor[Any](k)
            .map { x =>
              m.updated(x, v.toString)
            }
            .recover {
              case e =>
                throw new ConfigurationException(
                  s"Type [$k] specified as akka.actor.mailbox.requirement " +
                  s"[$v] in config can't be loaded due to [${e.getMessage}]",
                  e)
            }
            .get
      }
  }

  /**
   * Returns a mailbox type as specified in configuration, based on the id, or if not defined None.
   */
  def lookup(id: String): MailboxType = lookupConfigurator(id)

  /**
   * Returns a mailbox type as specified in configuration, based on the type, or if not defined None.
   */
  def lookupByQueueType(queueType: Class[_ <: Any]): MailboxType = lookup(lookupId(queueType))

  private final val rmqClass = classOf[RequiresMessageQueue[_]]

  /**
   * Return the required message queue type for this class if any.
   */
  def getRequiredType(actorClass: Class[_ <: Actor]): Class[_] =
    Reflect.findMarker(actorClass, rmqClass) match {
      case t: ParameterizedType =>
        t.getActualTypeArguments.head match {
          case c: Class[_] => c
          case x =>
            throw new IllegalArgumentException(s"no wildcard type allowed in RequireMessageQueue argument (was [$x])")
        }
      case unexpected =>
        throw new IllegalArgumentException(s"Unexpected actor class marker: $unexpected") // will not happen, for exhaustiveness check
    }

  // don’t care if this happens twice
  private var mailboxNonZeroPushTimeoutWarningIssued = false

  def getMailboxRequirement(config: Config) = config.getString("mailbox-requirement") match {
    case NoMailboxRequirement => classOf[MessageQueue]
    case x                    => dynamicAccess.getClassFor[AnyRef](x).get
  }

  def getProducedMessageQueueType(mailboxType: MailboxType): Class[_] = {
    val pmqClass = classOf[ProducesMessageQueue[_]]
    if (!pmqClass.isAssignableFrom(mailboxType.getClass)) classOf[MessageQueue]
    else
      Reflect.findMarker(mailboxType.getClass, pmqClass) match {
        case t: ParameterizedType =>
          t.getActualTypeArguments.head match {
            case c: Class[_] => c
            case x =>
              throw new IllegalArgumentException(
                s"no wildcard type allowed in ProducesMessageQueue argument (was [$x])")
          }
        case unexpected =>
          throw new IllegalArgumentException(s"Unexpected message queue type marker: $unexpected") // will not happen, for exhaustiveness check
      }
  }

  /**
   * Finds out the mailbox type for an actor based on configuration, props and requirements.
   */
  protected[akka] def getMailboxType(props: Props, dispatcherConfig: Config): MailboxType = {
    val id = dispatcherConfig.getString("id")
    val deploy = props.deploy
    val actorClass = props.actorClass()
    lazy val actorRequirement = getRequiredType(actorClass)

    val mailboxRequirement: Class[_] = getMailboxRequirement(dispatcherConfig)

    val hasMailboxRequirement: Boolean = mailboxRequirement != classOf[MessageQueue]

    val hasMailboxType =
      dispatcherConfig.hasPath("mailbox-type") &&
      dispatcherConfig.getString("mailbox-type") != Deploy.NoMailboxGiven

    def verifyRequirements(mailboxType: MailboxType): MailboxType = {
      lazy val mqType: Class[_] = getProducedMessageQueueType(mailboxType)
      if (hasMailboxRequirement && !mailboxRequirement.isAssignableFrom(mqType))
        throw new IllegalArgumentException(
          s"produced message queue type [$mqType] does not fulfill requirement for dispatcher [$id]. " +
          s"Must be a subclass of [$mailboxRequirement].")
      if (hasRequiredType(actorClass) && !actorRequirement.isAssignableFrom(mqType))
        throw new IllegalArgumentException(
          s"produced message queue type [$mqType] does not fulfill requirement for actor class [$actorClass]. " +
          s"Must be a subclass of [$actorRequirement].")
      mailboxType
    }

    if (deploy.mailbox != Deploy.NoMailboxGiven) {
      verifyRequirements(lookup(deploy.mailbox))
    } else if (deploy.dispatcher != Deploy.NoDispatcherGiven && deploy.dispatcher != Deploy.DispatcherSameAsParent && hasMailboxType) {
      verifyRequirements(lookup(dispatcherConfig.getString("id")))
    } else if (hasRequiredType(actorClass)) {
      try verifyRequirements(lookupByQueueType(getRequiredType(actorClass)))
      catch {
        case NonFatal(_) if hasMailboxRequirement => verifyRequirements(lookupByQueueType(mailboxRequirement))
      }
    } else if (hasMailboxRequirement) {
      verifyRequirements(lookupByQueueType(mailboxRequirement))
    } else {
      verifyRequirements(lookup(DefaultMailboxId))
    }
  }

  /**
   * Check if this class can have a required message queue type.
   */
  def hasRequiredType(actorClass: Class[_ <: Actor]): Boolean = rmqClass.isAssignableFrom(actorClass)

  private def lookupId(queueType: Class[_]): String =
    mailboxBindings.get(queueType) match {
      case None    => throw new ConfigurationException(s"Mailbox Mapping for [$queueType] not configured")
      case Some(s) => s
    }

  private def lookupConfigurator(id: String): MailboxType = {
    mailboxTypeConfigurators.get(id) match {
      case null =>
        // It doesn't matter if we create a mailbox type configurator that isn't used due to concurrent lookup.
        val newConfigurator = id match {
          // TODO RK remove these two for Akka 2.3
          case "unbounded"                               => UnboundedMailbox()
          case "bounded"                                 => new BoundedMailbox(settings, config(id))
          case _ if id.startsWith(BoundedCapacityPrefix) =>
            // hack to allow programmatic set of capacity through props in akka-typed but still share
            // mailbox configurators for the same size
            val capacity = id.split(':')(1).toInt
            new BoundedMailbox(capacity, Duration.Zero)

          case _ =>
            if (!settings.config.hasPath(id)) throw new ConfigurationException(s"Mailbox Type [$id] not configured")
            val conf = config(id)

            val mailboxType = conf.getString("mailbox-type") match {
              case "" => throw new ConfigurationException(s"The setting mailbox-type, defined in [$id] is empty")
              case fqcn =>
                val args = List(classOf[ActorSystem.Settings] -> settings, classOf[Config] -> conf)
                dynamicAccess
                  .createInstanceFor[MailboxType](fqcn, args)
                  .recover {
                    case exception =>
                      throw new IllegalArgumentException(
                        s"Cannot instantiate MailboxType [$fqcn], defined in [$id], make sure it has a public" +
                        " constructor with [akka.actor.ActorSystem.Settings, com.typesafe.config.Config] parameters",
                        exception)
                  }
                  .get
            }

            if (!mailboxNonZeroPushTimeoutWarningIssued) {
              mailboxType match {
                case m: ProducesPushTimeoutSemanticsMailbox if m.pushTimeOut.toNanos > 0L =>
                  warn(
                    s"Configured potentially-blocking mailbox [$id] configured with non-zero pushTimeOut (${m.pushTimeOut}), " +
                    s"which can lead to blocking behavior when sending messages to this mailbox. " +
                    s"Avoid this by setting `$id.mailbox-push-timeout-time` to `0`.")
                  mailboxNonZeroPushTimeoutWarningIssued = true
                case _ => // good; nothing to see here, move along, sir.
              }
            }

            mailboxType
        }

        mailboxTypeConfigurators.putIfAbsent(id, newConfigurator) match {
          case null     => newConfigurator
          case existing => existing
        }

      case existing => existing
    }
  }

  private val defaultMailboxConfig = settings.config.getConfig(DefaultMailboxId)

  private final def warn(msg: String): Unit =
    eventStream.publish(Warning("mailboxes", getClass, msg))

  //INTERNAL API
  private def config(id: String): Config = {
    import scala.jdk.CollectionConverters._
    ConfigFactory
      .parseMap(Map("id" -> id).asJava)
      .withFallback(settings.config.getConfig(id))
      .withFallback(defaultMailboxConfig)
  }

  private val stashCapacityCache = new AtomicReference[Map[String, Int]](Map.empty[String, Int])
  private val defaultStashCapacity: Int =
    stashCapacityFromConfig(Dispatchers.DefaultDispatcherId, Mailboxes.DefaultMailboxId)

  /**
   * INTERNAL API: The capacity of the stash. Configured in the actor's mailbox or dispatcher config.
   */
  private[akka] final def stashCapacity(dispatcher: String, mailbox: String): Int = {

    @tailrec def updateCache(cache: Map[String, Int], key: String, value: Int): Boolean = {
      stashCapacityCache.compareAndSet(cache, cache.updated(key, value)) ||
      updateCache(stashCapacityCache.get, key, value) // recursive, try again
    }

    if (dispatcher == Dispatchers.DefaultDispatcherId && mailbox == Mailboxes.DefaultMailboxId)
      defaultStashCapacity
    else {
      val cache = stashCapacityCache.get
      val key = dispatcher + "-" + mailbox
      cache.get(key) match {
        case Some(value) => value
        case None =>
          val value = stashCapacityFromConfig(dispatcher, mailbox)
          updateCache(cache, key, value)
          value
      }
    }
  }

  private def stashCapacityFromConfig(dispatcher: String, mailbox: String): Int = {
    val disp = Dispatchers.getConfig(settings.config, dispatcher)
    val fallback = disp.withFallback(settings.config.getConfig(Mailboxes.DefaultMailboxId))
    val config =
      if (mailbox == Mailboxes.DefaultMailboxId) fallback
      else settings.config.getConfig(mailbox).withFallback(fallback)
    config.getInt("stash-capacity")
  }
}
