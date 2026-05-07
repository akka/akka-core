/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.time.{ Duration => JDuration }
import java.util.Optional

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration
import scala.runtime.BoxedUnit

import akka.annotation.DoNotInherit
import akka.japi.pf.ReceiveBuilder

/**
 * Java API: compatible with lambda expressions
 */
object AbstractActor {

  /**
   * Defines which messages the Actor can handle, along with the implementation of
   * how the messages should be processed. You can build such behavior with the
   * [[akka.japi.pf.ReceiveBuilder]] but it can be implemented in other ways than
   * using the `ReceiveBuilder` since it in the end is just a wrapper around a
   * Scala `PartialFunction`. In Java, you can implement `PartialFunction` by
   * extending `AbstractPartialFunction`.
   */
  final class Receive(val onMessage: PartialFunction[Any, BoxedUnit]) {

    /**
     * Composes this `Receive` with a fallback which gets applied
     * where this partial function is not defined.
     */
    def orElse(other: Receive): Receive =
      new Receive(onMessage.orElse(other.onMessage))
  }

  /**
   * emptyBehavior is a Receive-expression that matches no messages at all, ever.
   */
  final val emptyBehavior: Receive = new Receive(PartialFunction.empty)

  /**
   * The actor context - the view of the actor cell from the actor.
   * Exposes contextual information for the actor and the current message.
   *
   * Not intended for public inheritance/implementation
   */
  @DoNotInherit
  trait ActorContext extends akka.actor.ActorContext {

    /**
     * The ActorRef representing this actor
     *
     * This method is thread-safe and can be called from other threads than the ordinary
     * actor message processing thread, such as [[java.util.concurrent.CompletionStage]] and [[scala.concurrent.Future]] callbacks.
     */
    def getSelf(): ActorRef

    /**
     * Retrieve the Props which were used to create this actor.
     *
     * This method is thread-safe and can be called from other threads than the ordinary
     * actor message processing thread, such as [[java.util.concurrent.CompletionStage]] and [[scala.concurrent.Future]] callbacks.
     */
    def getProps(): Props

    /**
     * Returns the sender 'ActorRef' of the current message.
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] and [[scala.concurrent.Future]] callbacks.
     */
    def getSender(): ActorRef = sender()

    /**
     * Returns an unmodifiable Java Collection containing the linked actors
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] callbacks.
     */
    def getChildren(): java.lang.Iterable[ActorRef]

    /**
     * Returns a reference to the named child if it exists.
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] callbacks.
     */
    def findChild(name: String): Optional[ActorRef]

    /**
     * Returns the supervisor of this actor.
     *
     * Same as `parent()`.
     *
     * This method is thread-safe and can be called from other threads than the ordinary
     * actor message processing thread, such as [[java.util.concurrent.CompletionStage]] callbacks.
     */
    def getParent(): ActorRef

    /**
     * Returns the system this actor is running in.
     *
     * Same as `system()`
     *
     * This method is thread-safe and can be called from other threads than the ordinary
     * actor message processing thread, such as [[java.util.concurrent.CompletionStage]] callbacks.
     */
    def getSystem(): ActorSystem

    /**
     * Returns the dispatcher (MessageDispatcher) that is used for this Actor.
     *
     * This method is thread-safe and can be called from other threads than the ordinary
     * actor message processing thread, such as [[java.util.concurrent.CompletionStage]] and [[scala.concurrent.Future]] callbacks.
     */
    def getDispatcher(): ExecutionContextExecutor

    /**
     * Changes the Actor's behavior to become the new 'Receive' handler.
     * Replaces the current behavior on the top of the behavior stack.
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] callbacks.
     */
    def become(behavior: Receive): Unit =
      become(behavior, discardOld = true)

    /**
     * Changes the Actor's behavior to become the new 'Receive' handler.
     * This method acts upon the behavior stack as follows:
     *
     *  - if `discardOld = true` it will replace the top element (i.e. the current behavior)
     *  - if `discardOld = false` it will keep the current behavior and push the given one atop
     *
     * The default of replacing the current behavior on the stack has been chosen to avoid memory
     * leaks in case client code is written without consulting this documentation first (i.e.
     * always pushing new behaviors and never issuing an `unbecome()`)
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] callbacks.
     */
    def become(behavior: Receive, discardOld: Boolean): Unit =
      become(behavior.onMessage.asInstanceOf[PartialFunction[Any, Unit]], discardOld)

    /**
     * Gets the current receive timeout.
     * When specified, the receive method should be able to handle a [[akka.actor.ReceiveTimeout]] message.
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] and [[scala.concurrent.Future]] callbacks.
     */
    def getReceiveTimeout(): java.time.Duration = {
      if (receiveTimeout.isFinite) {
        JDuration.ofNanos(receiveTimeout.toNanos)
      } else {
        JDuration.ZERO
      }
    }

    /**
     * Defines the inactivity timeout after which the sending of a [[akka.actor.ReceiveTimeout]] message is triggered.
     * When specified, the receive function should be able to handle a [[akka.actor.ReceiveTimeout]] message.
     * 1 millisecond is the minimum supported timeout.
     *
     * Please note that the receive timeout might fire and enqueue the `ReceiveTimeout` message right after
     * another message was enqueued; hence it is '''not guaranteed''' that upon reception of the receive
     * timeout there must have been an idle period beforehand as configured via this method.
     *
     * Once set, the receive timeout stays in effect (i.e. continues firing repeatedly after inactivity
     * periods). Pass in `Duration.Undefined` to switch off this feature.
     *
     * Messages marked with [[NotInfluenceReceiveTimeout]] will not reset the timer. This can be useful when
     * `ReceiveTimeout` should be fired by external inactivity but not influenced by internal activity,
     * e.g. scheduled tick messages.
     *
     * *Warning*: This method is not thread-safe and must not be accessed from threads other
     * than the ordinary actor message processing thread, such as [[java.util.concurrent.CompletionStage]] and [[scala.concurrent.Future]] callbacks.
     */
    def setReceiveTimeout(timeout: java.time.Duration): Unit = {
      import scala.jdk.DurationConverters._
      setReceiveTimeout(timeout.toScala)
    }

    /**
     * Cancel the sending of receive timeout notifications.
     */
    def cancelReceiveTimeout(): Unit = setReceiveTimeout(Duration.Undefined)
  }
}

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class that should be extended to create Java actors that use lambdas.
 * <p/>
 * Example:
 * <pre>
 * public class MyActorForJavaDoc extends AbstractActor{
 *    @Override
 *    public Receive createReceive() {
 *        return receiveBuilder()
 *                .match(Double.class, d -> {
 *                    sender().tell(d.isNaN() ? 0 : d, self());
 *                })
 *                .match(Integer.class, i -> {
 *                    sender().tell(i * 10, self());
 *                })
 *                .match(String.class, s -> s.startsWith("foo"), s -> {
 *                    sender().tell(s.toUpperCase(), self());
 *                })
 *                .build();
 *    }
 * }
 * </pre>
 *
 */
abstract class AbstractActor extends Actor {

  /**
   * Returns this AbstractActor's ActorContext
   * The ActorContext is not thread safe so do not expose it outside of the
   * AbstractActor.
   */
  def getContext(): AbstractActor.ActorContext = context.asInstanceOf[AbstractActor.ActorContext]

  /**
   * Returns the ActorRef for this actor.
   *
   * Same as `self()`.
   */
  def getSelf(): ActorRef = self

  /**
   * The reference sender Actor of the currently processed message. This is
   * always a legal destination to send to, even if there is no logical recipient
   * for the reply, in which case it will be sent to the dead letter mailbox.
   *
   * Same as `sender()`.
   *
   * WARNING: Only valid within the Actor itself, so do not close over it and
   * publish it to other threads!
   */
  def getSender(): ActorRef = sender()

  /**
   * User overridable definition the strategy to use for supervising
   * child actors.
   */
  override def supervisorStrategy: SupervisorStrategy = super.supervisorStrategy

  /**
   * User overridable callback.
   * <p/>
   * Is called when an Actor is started.
   * Actor are automatically started asynchronously when created.
   * Empty default implementation.
   */
  @throws(classOf[Exception])
  override def preStart(): Unit = super.preStart()

  /**
   * User overridable callback.
   * <p/>
   * Is called asynchronously after `getContext().stop()` is invoked.
   * Empty default implementation.
   */
  @throws(classOf[Exception])
  override def postStop(): Unit = super.postStop()

  // TODO In 2.6.0 we can remove deprecation and make the method final
  @deprecated("Override preRestart with message parameter with Optional type instead", "2.5.0")
  @throws(classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    import scala.jdk.OptionConverters._
    preRestart(reason, message.toJava)
  }

  /**
   * User overridable callback: '''By default it disposes of all children and then calls `postStop()`.'''
   * <p/>
   * Is called on a crashed Actor right BEFORE it is restarted to allow clean
   * up of resources before Actor is terminated.
   */
  @throws(classOf[Exception])
  def preRestart(reason: Throwable, message: Optional[Any]): Unit = {
    import scala.jdk.OptionConverters._
    super.preRestart(reason, message.toScala)
  }

  /**
   * User overridable callback: By default it calls `preStart()`.
   * <p/>
   * Is called right AFTER restart on the newly created Actor to allow reinitialization after an Actor crash.
   */
  @throws(classOf[Exception])
  override def postRestart(reason: Throwable): Unit = super.postRestart(reason)

  /**
   * An actor has to define its initial receive behavior by implementing
   * the `createReceive` method.
   */
  def createReceive(): AbstractActor.Receive

  override def receive: PartialFunction[Any, Unit] =
    createReceive().onMessage.asInstanceOf[PartialFunction[Any, Unit]]

  /**
   * Convenience factory of the `ReceiveBuilder`.
   * Creates a new empty `ReceiveBuilder`.
   */
  final def receiveBuilder(): ReceiveBuilder = ReceiveBuilder.create()
}

/**
 * If the validation of the `ReceiveBuilder` match logic turns out to be a bottleneck for some of your
 * actors you can consider to implement it at lower level by extending `UntypedAbstractActor` instead
 * of `AbstractActor`. The partial functions created by the `ReceiveBuilder` consist of multiple lambda
 * expressions for every match statement, where each lambda is referencing the code to be run. This is something
 * that the JVM can have problems optimizing and the resulting code might not be as performant as the
 * untyped version. When extending `UntypedAbstractActor` each message is received as an untyped
 * `Object` and you have to inspect and cast it to the actual message type in other ways (instanceof checks).
 */
abstract class UntypedAbstractActor extends AbstractActor {

  final override def createReceive(): AbstractActor.Receive =
    throw new UnsupportedOperationException("createReceive should not be used by UntypedAbstractActor")

  override def receive: PartialFunction[Any, Unit] = { case msg => onReceive(msg) }

  /**
   * To be implemented by concrete UntypedAbstractActor, this defines the behavior of the
   * actor.
   */
  @throws(classOf[Throwable])
  def onReceive(message: Any): Unit

  /**
   * Recommended convention is to call this method if the message
   * isn't handled in [[#onReceive]] (e.g. unknown message type).
   * By default it fails with either a [[akka.actor.DeathPactException]] (in
   * case of an unhandled [[akka.actor.Terminated]] message) or publishes an [[akka.actor.UnhandledMessage]]
   * to the actor's system's [[akka.event.EventStream]].
   */
  override def unhandled(message: Any): Unit = super.unhandled(message)
}

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class that mixes in logging into the Actor.
 *
 */
abstract class AbstractLoggingActor extends AbstractActor with ActorLogging

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class that should be extended to create an actor with a stash.
 *
 * The stash enables an actor to temporarily stash away messages that can not or
 * should not be handled using the actor's current behavior.
 * <p/>
 * Example:
 * <pre>
 * public class MyActorWithStash extends AbstractActorWithStash {
 *   int count = 0;
 *
 *   public MyActorWithStash() {
 *     receive(ReceiveBuilder. match(String.class, s -> {
 *       if (count < 0) {
 *         sender().tell(new Integer(s.length()), self());
 *       } else if (count == 2) {
 *         count = -1;
 *         unstashAll();
 *       } else {
 *         count += 1;
 *         stash();
 *       }}).build()
 *     );
 *   }
 * }
 * </pre>
 * Note that the subclasses of `AbstractActorWithStash` by default request a Deque based mailbox since this class
 * implements the `RequiresMessageQueue&lt;DequeBasedMessageQueueSemantics&gt;` marker interface.
 * You can override the default mailbox provided when `DequeBasedMessageQueueSemantics` are requested via config:
 * <pre>
 *   akka.actor.mailbox.requirements {
 *     "akka.dispatch.BoundedDequeBasedMessageQueueSemantics" = your-custom-mailbox
 *   }
 * </pre>
 * Alternatively, you can add your own requirement marker to the actor and configure a mailbox type to be used
 * for your marker.
 *
 * For a `Stash` based actor that enforces unbounded deques see [[akka.actor.AbstractActorWithUnboundedStash]].
 * There is also an unrestricted version [[akka.actor.AbstractActorWithUnrestrictedStash]] that does not
 * enforce the mailbox type.
 *
 */
abstract class AbstractActorWithStash extends AbstractActor with Stash

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class with `Stash` that enforces an unbounded deque for the actor. The proper mailbox has to be configured
 * manually, and the mailbox should extend the [[akka.dispatch.DequeBasedMessageQueueSemantics]] marker trait.
 * See [[akka.actor.AbstractActorWithStash]] for details on how `Stash` works.
 *
 */
abstract class AbstractActorWithUnboundedStash extends AbstractActor with UnboundedStash

/**
 * Java API: compatible with lambda expressions
 *
 * Actor base class with `Stash` that does not enforce any mailbox type. The mailbox of the actor has to be configured
 * manually. See [[akka.actor.AbstractActorWithStash]] for details on how `Stash` works.
 *
 */
abstract class AbstractActorWithUnrestrictedStash extends AbstractActor with UnrestrictedStash
