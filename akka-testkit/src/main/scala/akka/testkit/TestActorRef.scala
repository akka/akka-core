/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.reflect.ClassTag

import akka.actor._
import akka.dispatch._
import akka.pattern.ask

/**
 * This special ActorRef is exclusively for use during unit testing in a single-threaded environment. Therefore, it
 * overrides the dispatcher to CallingThreadDispatcher and sets the receiveTimeout to None. Otherwise,
 * it acts just like a normal ActorRef. You may retrieve a reference to the underlying actor to test internal logic.
 *
 * @since 1.1
 */
@nowarn // 'early initializers' are deprecated on 2.13 and will be replaced with trait parameters on 2.14. https://github.com/akka/akka/issues/26753
class TestActorRef[T <: Actor](_system: ActorSystem, _props: Props, _supervisor: ActorRef, name: String)
    extends LocalActorRef({
      val disregard = _supervisor match {
        case l: LocalActorRef => l.underlying.reserveChild(name)
        case r: RepointableActorRef =>
          r.underlying match {
            case _: UnstartedCell =>
              throw new IllegalStateException(
                "cannot attach a TestActor to an unstarted top-level actor, ensure that it is started by sending a message and observing the reply")
            case c: ActorCell => c.reserveChild(name)
            case o =>
              _system.log.error(
                "trying to attach child {} to unknown type of supervisor cell {}, this is not going to end well",
                name,
                o.getClass)
          }
        case s =>
          _system.log.error(
            "trying to attach child {} to unknown type of supervisor {}, this is not going to end well",
            name,
            s.getClass)
      }

      _system.asInstanceOf[ActorSystemImpl]
    }, {
      _props.withDispatcher(
        if (_props.deploy.dispatcher == Deploy.NoDispatcherGiven) CallingThreadDispatcher.Id
        else _props.dispatcher)
    }, {
      val props = _props.withDispatcher(
        if (_props.deploy.dispatcher == Deploy.NoDispatcherGiven) CallingThreadDispatcher.Id
        else _props.dispatcher)
      _system.dispatchers.lookup(props.dispatcher)
    }, {
      val props = _props.withDispatcher(
        if (_props.deploy.dispatcher == Deploy.NoDispatcherGiven) CallingThreadDispatcher.Id
        else _props.dispatcher)
      val dispatcher = _system.dispatchers.lookup(props.dispatcher)
      _system.mailboxes.getMailboxType(props, dispatcher.configurator.config)
    }, _supervisor.asInstanceOf[InternalActorRef], _supervisor.path / name) {

  val props = _props.withDispatcher(
    if (_props.deploy.dispatcher == Deploy.NoDispatcherGiven) CallingThreadDispatcher.Id
    else _props.dispatcher)

  val dispatcher = _system.dispatchers.lookup(props.dispatcher)

  // we need to start ourselves since the creation of an actor has been split into initialization and starting
  underlying.start()

  import TestActorRef.InternalGetActor

  protected override def newActorCell(
      system: ActorSystemImpl,
      ref: InternalActorRef,
      props: Props,
      dispatcher: MessageDispatcher,
      supervisor: InternalActorRef): ActorCell =
    new ActorCell(system, ref, props, dispatcher, supervisor) {
      override def autoReceiveMessage(msg: Envelope): Unit = {
        msg.message match {
          case InternalGetActor => sender() ! actor
          case _                => super.autoReceiveMessage(msg)
        }
      }
    }

  /**
   * Directly inject messages into actor receive behavior. Any exceptions
   * thrown will be available to you, while still being able to use
   * become/unbecome.
   */
  def receive(o: Any): Unit = receive(o, underlying.system.deadLetters)

  /**
   * Directly inject messages into actor receive behavior. Any exceptions
   * thrown will be available to you, while still being able to use
   * become/unbecome.
   */
  def receive(o: Any, sender: ActorRef): Unit =
    try {
      underlying.currentMessage =
        Envelope(o, if (sender eq null) underlying.system.deadLetters else sender, underlying.system)
      underlying.receiveMessage(o)
    } finally underlying.currentMessage = null

  /**
   * Retrieve reference to the underlying actor, where the static type matches the factory used inside the
   * constructor. Beware that this reference is discarded by the ActorRef upon restarting the actor (should this
   * reference be linked to a supervisor). The old Actor may of course still be used in post-mortem assertions.
   */
  def underlyingActor: T = {
    // volatile mailbox read to bring in actor field
    if (isTerminated) throw IllegalActorStateException("underlying actor is terminated")
    underlying.actor.asInstanceOf[T] match {
      case null =>
        val t = TestKitExtension(_system).DefaultTimeout
        Await.result(this.?(InternalGetActor)(t), t.duration).asInstanceOf[T]
      case ref => ref
    }
  }

  /**
   * Registers this actor to be a death monitor of the provided ActorRef
   * This means that this actor will get a Terminated()-message when the provided actor
   * is permanently terminated.
   *
   * @return the same ActorRef that is provided to it, to allow for cleaner invocations
   */
  def watch(subject: ActorRef): ActorRef = underlying.watch(subject)

  /**
   * Unregisters this actor from being a death monitor of the provided ActorRef
   * This means that this actor will not get a Terminated()-message when the provided actor
   * is permanently terminated.
   *
   * @return the same ActorRef that is provided to it, to allow for cleaner invocations
   */
  def unwatch(subject: ActorRef): ActorRef = underlying.unwatch(subject)

  override def toString = "TestActor[" + path + "]"

}

object TestActorRef {

  private case object InternalGetActor extends AutoReceivedMessage with PossiblyHarmful

  private val number = new AtomicLong
  private[testkit] def randomName: String = {
    val l = number.getAndIncrement()
    "$" + akka.util.Helpers.base64(l)
  }

  def apply[T <: Actor: ClassTag](factory: => T)(implicit system: ActorSystem): TestActorRef[T] =
    apply[T](Props(factory), randomName)

  def apply[T <: Actor: ClassTag](factory: => T, name: String)(implicit system: ActorSystem): TestActorRef[T] =
    apply[T](Props(factory), name)

  def apply[T <: Actor](props: Props)(implicit system: ActorSystem): TestActorRef[T] = apply[T](props, randomName)

  def apply[T <: Actor](props: Props, name: String)(implicit system: ActorSystem): TestActorRef[T] =
    apply[T](props, system.asInstanceOf[ActorSystemImpl].guardian, name)

  def apply[T <: Actor](props: Props, supervisor: ActorRef)(implicit system: ActorSystem): TestActorRef[T] = {
    val sysImpl = system.asInstanceOf[ActorSystemImpl]
    new TestActorRef(sysImpl, props, supervisor.asInstanceOf[InternalActorRef], randomName)
  }

  def apply[T <: Actor](props: Props, supervisor: ActorRef, name: String)(
      implicit system: ActorSystem): TestActorRef[T] = {
    val sysImpl = system.asInstanceOf[ActorSystemImpl]
    new TestActorRef(sysImpl, props, supervisor.asInstanceOf[InternalActorRef], name)
  }

  def apply[T <: Actor](implicit t: ClassTag[T], system: ActorSystem): TestActorRef[T] = apply[T](randomName)

  private def dynamicCreateRecover[U]: PartialFunction[Throwable, U] = {
    case exception =>
      throw ActorInitializationException(
        null,
        "Could not instantiate Actor" +
        "\nMake sure Actor is NOT defined inside a class/trait," +
        "\nif so put it outside the class/trait, f.e. in a companion object," +
        "\nOR try to change: 'actorOf(Props[MyActor]' to 'actorOf(Props(new MyActor)'.",
        exception)
  }

  def apply[T <: Actor](name: String)(implicit t: ClassTag[T], system: ActorSystem): TestActorRef[T] =
    apply[T](Props({
      system
        .asInstanceOf[ExtendedActorSystem]
        .dynamicAccess
        .createInstanceFor[T](t.runtimeClass, Nil)
        .recover(dynamicCreateRecover)
        .get
    }), name)

  def apply[T <: Actor](supervisor: ActorRef)(implicit t: ClassTag[T], system: ActorSystem): TestActorRef[T] =
    apply[T](Props({
      system
        .asInstanceOf[ExtendedActorSystem]
        .dynamicAccess
        .createInstanceFor[T](t.runtimeClass, Nil)
        .recover(dynamicCreateRecover)
        .get
    }), supervisor)

  def apply[T <: Actor](supervisor: ActorRef, name: String)(
      implicit t: ClassTag[T],
      system: ActorSystem): TestActorRef[T] =
    apply[T](
      Props({
        system
          .asInstanceOf[ExtendedActorSystem]
          .dynamicAccess
          .createInstanceFor[T](t.runtimeClass, Nil)
          .recover(dynamicCreateRecover)
          .get
      }),
      supervisor,
      name)

  /**
   * Java API: create a TestActorRef in the given system for the given props,
   * with the given supervisor and name.
   */
  def create[T <: Actor](system: ActorSystem, props: Props, supervisor: ActorRef, name: String): TestActorRef[T] =
    apply(props, supervisor, name)(system)

  /**
   * Java API: create a TestActorRef in the given system for the given props,
   * with the given supervisor and a random name.
   */
  def create[T <: Actor](system: ActorSystem, props: Props, supervisor: ActorRef): TestActorRef[T] =
    apply(props, supervisor)(system)

  /**
   * Java API: create a TestActorRef in the given system for the given props,
   * with the given name.
   */
  def create[T <: Actor](system: ActorSystem, props: Props, name: String): TestActorRef[T] = apply(props, name)(system)

  /**
   * Java API: create a TestActorRef in the given system for the given props,
   * with a random name.
   */
  def create[T <: Actor](system: ActorSystem, props: Props): TestActorRef[T] = apply(props)(system)
}
