/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.annotation.varargs
import scala.collection.immutable
import scala.reflect.ClassTag
import akka.actor.Deploy.{ NoDispatcherGiven, NoMailboxGiven }
import akka.annotation.InternalApi
import akka.dispatch._
import akka.routing._

/**
 * Factory for Props instances.
 *
 * Props is a ActorRef configuration object, that is immutable, so it is thread safe and fully sharable.
 *
 * Used when creating new actors through <code>ActorSystem.actorOf</code> and <code>ActorContext.actorOf</code>.
 */
object Props extends AbstractProps {

  /**
   * The defaultCreator, simply throws an UnsupportedOperationException when applied, which is used when creating a Props
   */
  final val defaultCreator: () => Actor = () => throw new UnsupportedOperationException("No actor creator specified!")

  /**
   * The defaultRoutedProps is NoRouter which is used when creating a Props
   */
  final val defaultRoutedProps: RouterConfig = NoRouter

  /**
   * The default Deploy instance which is used when creating a Props
   */
  final val defaultDeploy = Deploy()

  /**
   * A Props instance whose creator will create an actor that doesn't respond to any message
   */
  final val empty = Props[EmptyActor]()

  /**
   * The default Props instance, uses the settings from the Props object starting with default*.
   */
  final val default = Props(defaultDeploy, classOf[CreatorFunctionConsumer], List(defaultCreator))

  /**
   * INTERNAL API
   *
   * (Not because it is so immensely complicated, only because we might remove it if no longer needed internally)
   */
  @InternalApi
  private[akka] class EmptyActor extends Actor {
    def receive = Actor.emptyBehavior
  }

  /**
   * Scala API: Returns a Props that has default values except for "creator" which will be a function that creates an instance
   * of the supplied type using the default constructor.
   */
  def apply[T <: Actor: ClassTag](): Props = apply(defaultDeploy, implicitly[ClassTag[T]].runtimeClass, List.empty)

  /**
   * Scala API: Returns a Props that has default values except for "creator" which will be a function that creates an instance
   * using the supplied thunk.
   *
   * CAVEAT: Required mailbox type cannot be detected when using anonymous mixin composition
   * when creating the instance. For example, the following will not detect the need for
   * `DequeBasedMessageQueueSemantics` as defined in `Stash`:
   * {{{
   * 'Props(new Actor with Stash { ... })
   * }}}
   * Instead you must create a named class that mixin the trait,
   * e.g. `class MyActor extends Actor with Stash`.
   */
  def apply[T <: Actor: ClassTag](creator: => T): Props =
    mkProps(implicitly[ClassTag[T]].runtimeClass, () => creator)

  private def mkProps(classOfActor: Class[_], ctor: () => Actor): Props =
    Props(classOf[TypedCreatorFunctionConsumer], classOfActor, ctor)

  /**
   * Scala API: create a Props given a class and its constructor arguments.
   */
  def apply(clazz: Class[_], args: Any*): Props = apply(defaultDeploy, clazz, args.toList)

}

/**
 * Props is a configuration object using in creating an [[Actor]]; it is
 * immutable, so it is thread-safe and fully shareable.
 *
 * Examples on Scala API:
 * {{{
 *  val props = Props.empty
 *  val props = Props[MyActor]
 *  val props = Props(classOf[MyActor], arg1, arg2)
 *
 *  val otherProps = props.withDispatcher("dispatcher-id")
 *  val otherProps = props.withDeploy(<deployment info>)
 * }}}
 *
 * Examples on Java API:
 * {{{
 *  final Props props = Props.empty();
 *  final Props props = Props.create(MyActor.class, arg1, arg2);
 *
 *  final Props otherProps = props.withDispatcher("dispatcher-id");
 *  final Props otherProps = props.withDeploy(<deployment info>);
 * }}}
 */
@SerialVersionUID(2L)
final case class Props(deploy: Deploy, clazz: Class[_], args: immutable.Seq[Any]) {

  Props.validate(clazz)

  // derived property, does not need to be serialized
  @transient
  private[this] var _producer: IndirectActorProducer = _

  // derived property, does not need to be serialized
  @transient
  private[this] var _cachedActorClass: Class[_ <: Actor] = _

  /**
   * INTERNAL API
   */
  private[akka] def producer: IndirectActorProducer = {
    if (_producer eq null)
      _producer = IndirectActorProducer(clazz, args)

    _producer
  }

  private[this] def cachedActorClass: Class[_ <: Actor] = {
    if (_cachedActorClass eq null)
      _cachedActorClass = producer.actorClass

    _cachedActorClass
  }

  // validate producer constructor signature; throws IllegalArgumentException if invalid
  producer

  /**
   * Convenience method for extracting the dispatcher information from the
   * contained [[Deploy]] instance.
   */
  def dispatcher: String = deploy.dispatcher match {
    case NoDispatcherGiven => Dispatchers.DefaultDispatcherId
    case x                 => x
  }

  /**
   * Convenience method for extracting the mailbox information from the
   * contained [[Deploy]] instance.
   */
  def mailbox: String = deploy.mailbox match {
    case NoMailboxGiven => Mailboxes.DefaultMailboxId
    case x              => x
  }

  /**
   * Convenience method for extracting the router configuration from the
   * contained [[Deploy]] instance.
   */
  def routerConfig: RouterConfig = deploy.routerConfig

  /**
   * Returns a new Props with the specified dispatcher set.
   */
  def withDispatcher(d: String): Props = deploy.dispatcher match {
    case NoDispatcherGiven => copy(deploy = deploy.copy(dispatcher = d))
    case x                 => if (x == d) this else copy(deploy = deploy.copy(dispatcher = d))
  }

  /**
   * Returns a new Props with the specified mailbox set.
   */
  def withMailbox(m: String): Props = deploy.mailbox match {
    case NoMailboxGiven => copy(deploy = deploy.copy(mailbox = m))
    case x              => if (x == m) this else copy(deploy = deploy.copy(mailbox = m))
  }

  /**
   * Returns a new Props with the specified router config set.
   */
  def withRouter(r: RouterConfig): Props = copy(deploy = deploy.copy(routerConfig = r))

  /**
   * Returns a new Props with the specified deployment configuration.
   */
  def withDeploy(d: Deploy): Props = copy(deploy = d.withFallback(deploy))

  /**
   * Returns a new Props with the specified set of tags.
   */
  @varargs
  def withActorTags(tags: String*): Props =
    withActorTags(tags.toSet)

  /**
   * Scala API: Returns a new Props with the specified set of tags.
   */
  def withActorTags(tags: Set[String]): Props =
    copy(deploy = deploy.withTags(tags))

  /**
   * Obtain an upper-bound approximation of the actor class which is going to
   * be created by these Props. In other words, the actor factory method will
   * produce an instance of this class or a subclass thereof. This is used by
   * the actor system to select special dispatchers or mailboxes in case
   * dependencies are encoded in the actor type.
   */
  def actorClass(): Class[_ <: Actor] = cachedActorClass

  /**
   * INTERNAL API
   *
   * Create a new actor instance. This method is only useful when called during
   * actor creation by the ActorSystem, i.e. for user-level code it can only be
   * used within the implementation of [[IndirectActorProducer#produce]].
   */
  private[akka] def newActor(): Actor = {
    producer.produce()
  }
}
