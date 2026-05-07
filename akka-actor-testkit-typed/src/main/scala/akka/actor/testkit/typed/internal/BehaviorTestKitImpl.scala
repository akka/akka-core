/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import java.util

import scala.annotation.tailrec
import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.control.Exception.Catcher
import scala.util.control.NonFatal

import akka.actor.ActorPath
import akka.actor.testkit.typed.{ CapturedLogEvent, Effect }
import akka.actor.testkit.typed.Effect._
import akka.actor.typed.{ ActorRef, Behavior, BehaviorInterceptor, PostStop, Signal, TypedActorContext }
import akka.actor.typed.internal.{ AdaptMessage, AdaptWithRegisteredMessageAdapter }
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.japi.function.{ Function => JFunction }
import akka.pattern.StatusReply
import akka.util.OptionVal
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class BehaviorTestKitImpl[T](
    system: ActorSystemStub,
    _path: ActorPath,
    _initialBehavior: Behavior[T])
    extends akka.actor.testkit.typed.javadsl.BehaviorTestKit[T]
    with akka.actor.testkit.typed.scaladsl.BehaviorTestKit[T] {

  // really this should be private, make so when we port out tests that need it
  private[akka] val context: EffectfulActorContext[T] =
    new EffectfulActorContext[T](system, _path, () => currentBehavior, this)

  private[akka] def as[U]: BehaviorTestKitImpl[U] = this.asInstanceOf[BehaviorTestKitImpl[U]]

  private var currentUncanonical = _initialBehavior
  private var current = {
    try {
      context.setCurrentActorThread()
      Behavior.validateAsInitial(Behavior.start(_initialBehavior, context))
    } finally {
      context.clearCurrentActorThread()
    }
  }

  // execute any future tasks scheduled in Actor's constructor
  runAllTasks()

  override def runAsk[Res](f: ActorRef[Res] => T): ReplyInboxImpl[Res] = {
    val replyToInbox = TestInboxImpl[Res]("replyTo")

    run(f(replyToInbox.ref))
    new ReplyInboxImpl(OptionVal(replyToInbox))
  }

  override def runAsk[Res](messageFactory: JFunction[ActorRef[Res], T]): ReplyInboxImpl[Res] =
    runAsk(messageFactory.apply _)

  override def runAskWithStatus[Res](f: ActorRef[StatusReply[Res]] => T): StatusReplyInboxImpl[Res] = {
    val replyToInbox = TestInboxImpl[StatusReply[Res]]("replyTo")

    run(f(replyToInbox.ref))
    new StatusReplyInboxImpl(OptionVal(replyToInbox))
  }

  override def runAskWithStatus[Res](
      messageFactory: JFunction[ActorRef[StatusReply[Res]], T]): StatusReplyInboxImpl[Res] =
    runAskWithStatus(messageFactory.apply _)

  override def retrieveEffect(): Effect = context.effectQueue.poll() match {
    case null => NoEffects
    case x    => x
  }

  override def childInbox[U](name: String): TestInboxImpl[U] = {
    val inbox = context.childInbox[U](name)
    assert(inbox.isDefined, s"Child not created: $name. Children created: [${context.childrenNames.mkString(",")}]")
    inbox.get
  }

  override def childInbox[U](ref: ActorRef[U]): TestInboxImpl[U] =
    childInbox(ref.path.name)

  override def childTestKit[U](child: ActorRef[U]): BehaviorTestKitImpl[U] = context.childTestKit(child)

  override def selfInbox(): TestInboxImpl[T] = context.selfInbox

  override def retrieveAllEffects(): immutable.Seq[Effect] = {
    @tailrec def rec(acc: List[Effect]): List[Effect] = context.effectQueue.poll() match {
      case null => acc.reverse
      case x    => rec(x :: acc)
    }

    rec(Nil)
  }

  def getEffect(): Effect = retrieveEffect()

  def getAllEffects(): util.List[Effect] = retrieveAllEffects().asJava

  override def expectEffect(expectedEffect: Effect): Unit = {
    context.effectQueue.poll() match {
      case null   => assert(expectedEffect == NoEffects, s"expected: $expectedEffect but no effects were recorded")
      case effect => assert(expectedEffect == effect, s"expected: $expectedEffect but found $effect")
    }
  }

  def expectEffectClass[E <: Effect](effectClass: Class[E]): E = {
    context.effectQueue.poll() match {
      case null if effectClass.isAssignableFrom(NoEffects.getClass) => effectClass.cast(NoEffects)
      case null =>
        throw new AssertionError(s"expected: effect type ${effectClass.getName} but no effects were recorded")
      case effect if effectClass.isAssignableFrom(effect.getClass) => effect.asInstanceOf[E]
      case other                                                   => throw new AssertionError(s"expected: effect class ${effectClass.getName} but found $other")
    }
  }

  def expectEffectPF[R](f: PartialFunction[Effect, R]): R = {
    context.effectQueue.poll() match {
      case null if f.isDefinedAt(NoEffects) =>
        f.apply(NoEffects)
      case eff if f.isDefinedAt(eff) =>
        f.apply(eff)
      case other =>
        throw new AssertionError(s"expected matching effect but got: $other")
    }
  }

  def expectEffectType[E <: Effect](implicit classTag: ClassTag[E]): E =
    expectEffectClass(classTag.runtimeClass.asInstanceOf[Class[E]])

  def returnedBehavior: Behavior[T] = currentUncanonical
  def currentBehavior: Behavior[T] = current
  def isAlive: Boolean = Behavior.isAlive(current)

  private def handleException: Catcher[Unit] = {
    case NonFatal(e) =>
      try Behavior.canonicalize(Behavior.interpretSignal(current, context, PostStop), current, context) // TODO why canonicalize here?
      catch {
        case NonFatal(_) => /* ignore, real is logging */
      }
      throw e
  }

  private def runAllTasks(): Unit = {
    context.executionContext match {
      case controlled: ControlledExecutor => controlled.runAll()
      case _                              =>
    }
  }

  override def run(message: T): Unit = {
    try {
      context.setCurrentActorThread()
      try {
        //we need this to handle message adapters related messages
        val intercepted = BehaviorTestKitImpl.Interceptor.inteceptBehaviour(current, context)
        currentUncanonical = Behavior.interpretMessage(intercepted, context, message)
        //notice we pass current and not intercepted, this way Behaviors.same will be resolved to current which will be intercepted again on the next message
        //otherwise we would have risked intercepting an already intercepted behavior (or would have had to explicitly check if the current behavior is already intercepted by us)
        current = Behavior.canonicalize(currentUncanonical, current, context)
      } finally {
        context.clearCurrentActorThread()
      }
      runAllTasks()
    } catch handleException
  }

  override def runOne(): Unit = run(selfInbox().receiveMessage())

  override def signal(signal: Signal): Unit = {
    try {
      context.setCurrentActorThread()
      currentUncanonical = Behavior.interpretSignal(current, context, signal)
      current = Behavior.canonicalize(currentUncanonical, current, context)
    } catch handleException
    finally {
      context.clearCurrentActorThread()
    }
  }

  override def hasEffects(): Boolean = !context.effectQueue.isEmpty

  override def getAllLogEntries(): util.List[CapturedLogEvent] = logEntries().asJava

  override def logEntries(): immutable.Seq[CapturedLogEvent] = context.logEntries

  override def clearLog(): Unit = context.clearLog()

  override def receptionistInbox(): TestInboxImpl[Receptionist.Command] = context.system.receptionistInbox
}

private[akka] object BehaviorTestKitImpl {
  object Interceptor extends BehaviorInterceptor[Any, Any]() {

    // Intercept a internal message adaptors related messages, forward the rest
    override def aroundReceive(
        ctx: TypedActorContext[Any],
        msg: Any,
        target: BehaviorInterceptor.ReceiveTarget[Any]): Behavior[Any] = {
      msg match {
        case AdaptWithRegisteredMessageAdapter(msgToAdapt) =>
          val fn = ctx
            .asInstanceOf[StubbedActorContext[Any]]
            .messageAdapters
            .collectFirst {
              case (clazz, func) if clazz.isInstance(msgToAdapt) => func
            }
            .getOrElse(sys.error(s"can't find a message adaptor for $msgToAdapt"))

          val adaptedMsg = fn(msgToAdapt)
          target.apply(ctx, adaptedMsg)

        case AdaptMessage(msgToAdapt, messageAdapter) =>
          val adaptedMsg = messageAdapter(msgToAdapt)
          target.apply(ctx, adaptedMsg)

        case t => target.apply(ctx, t)
      }
    }

    def inteceptBehaviour[T](behavior: Behavior[T], ctx: TypedActorContext[T]): Behavior[T] =
      Behavior
        .start(Behaviors.intercept { () =>
          this.asInstanceOf[BehaviorInterceptor[Any, T]]
        }(behavior), ctx.asInstanceOf[TypedActorContext[Any]])
        .unsafeCast[T]
  }
}
