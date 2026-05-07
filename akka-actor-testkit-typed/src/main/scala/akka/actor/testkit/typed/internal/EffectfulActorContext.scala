/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

import akka.actor.{ ActorPath, Cancellable }
import akka.actor.testkit.typed.Effect
import akka.actor.testkit.typed.Effect._
import akka.actor.typed.{ ActorRef, Behavior, Props }
import akka.actor.typed.RecipientRef
import akka.actor.typed.internal.TimerSchedulerCrossDslSupport
import akka.annotation.InternalApi
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class EffectfulActorContext[T](
    system: ActorSystemStub,
    path: ActorPath,
    currentBehaviorProvider: () => Behavior[T],
    behaviorTestKit: BehaviorTestKitImpl[T])
    extends StubbedActorContext[T](system, path, currentBehaviorProvider) {

  private[akka] val effectQueue = new ConcurrentLinkedQueue[Effect]

  override def ask[Req, Res](target: RecipientRef[Req], createRequest: ActorRef[Res] => Req)(
      mapResponse: Try[Res] => T)(implicit responseTimeout: Timeout, classTag: ClassTag[Res]): Unit = {
    // In the real implementation, this propagates as an immediately-failed future,
    // but since an illegal timeout is the sort of thing that ideally would have been
    // a type error, blowing up the test is the next-best thing
    require(responseTimeout.duration.length > 0, s"Timeout length must be positive, question not sent to [$target]")

    val responseClass = classTag.runtimeClass.asInstanceOf[Class[Res]]

    commonAsk(responseClass, createRequest, target, responseTimeout.duration, mapResponse)
  }

  override def ask[Req, Res](
      resClass: Class[Res],
      target: RecipientRef[Req],
      responseTimeout: Duration,
      createRequest: akka.japi.function.Function[ActorRef[Res], Req],
      applyToResponse: akka.japi.function.Function2[Res, Throwable, T]): Unit = {
    require(
      responseTimeout.getSeconds > 0 || responseTimeout.getNano > 0,
      s"Timeout length must be positive, question not sent to [$target]")

    val scalaCreateRequest = createRequest(_)
    val scalaMapResponse = { (result: Try[Res]) =>
      result
        .map(applyToResponse(_, null))
        .recover {
          case NonFatal(ex) => applyToResponse(null.asInstanceOf[Res], ex)
        }
        .get
    }

    commonAsk(resClass, scalaCreateRequest, target, responseTimeout.toScala, scalaMapResponse)
  }

  private def commonAsk[Req, Res](
      responseClass: Class[Res],
      createRequest: ActorRef[Res] => Req,
      target: RecipientRef[Req],
      responseTimeout: FiniteDuration,
      mapResponse: Try[Res] => T): Unit = {
    val replyTo = system.ignoreRef[Res]
    val askMessage = createRequest(replyTo)
    target ! askMessage

    val responseForwarder = { (t: Try[Res]) =>
      import akka.actor.typed.internal.AdaptMessage

      // Yay erasure
      val adaptedTestKit = behaviorTestKit.asInstanceOf[BehaviorTestKitImpl[AdaptMessage[Try[Res], T]]]

      adaptedTestKit.run(AdaptMessage(t, mapResponse))
    }

    effectQueue.offer(AskInitiated(target, responseTimeout, responseClass)(askMessage, responseForwarder, mapResponse))
  }

  override def spawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty): ActorRef[U] = {
    val ref = super.spawnAnonymous(behavior, props)
    effectQueue.offer(new SpawnedAnonymous(behavior, props, ref))
    ref
  }
  override def spawnMessageAdapter[U](f: U => T): ActorRef[U] = {
    val ref = super.spawnMessageAdapter(f)
    effectQueue.offer(new SpawnedAnonymousAdapter(ref))
    ref
  }
  override def spawnMessageAdapter[U](f: U => T, name: String): ActorRef[U] = {
    val ref = super.spawnMessageAdapter(f, name)
    effectQueue.offer(new SpawnedAdapter(name, ref))
    ref
  }
  override def messageAdapter[U: ClassTag](f: U => T): ActorRef[U] = {
    val ref = super.messageAdapter(f)
    effectQueue.offer(MessageAdapter(implicitly[ClassTag[U]].runtimeClass.asInstanceOf[Class[U]], f))
    ref
  }
  override def messageAdapter[U](messageClass: Class[U], f: akka.japi.function.Function[U, T]): ActorRef[U] = {
    val ref = super.messageAdapter(messageClass, f)
    effectQueue.offer(MessageAdapter[U, T](messageClass, f.apply))
    ref
  }
  override def spawn[U](behavior: Behavior[U], name: String, props: Props = Props.empty): ActorRef[U] = {
    val ref = super.spawn(behavior, name, props)
    effectQueue.offer(new Spawned(behavior, name, props, ref))
    ref
  }
  override def stop[U](child: ActorRef[U]): Unit = {
    effectQueue.offer(Stopped(child.path.name))
    super.stop(child)
  }
  override def watch[U](other: ActorRef[U]): Unit = {
    effectQueue.offer(Watched(other))
    super.watch(other)
  }
  override def watchWith[U](other: ActorRef[U], message: T): Unit = {
    effectQueue.offer(WatchedWith(other, message))
    super.watchWith(other, message)
  }
  override def unwatch[U](other: ActorRef[U]): Unit = {
    effectQueue.offer(Unwatched(other))
    super.unwatch(other)
  }
  override def setReceiveTimeout(d: FiniteDuration, message: T): Unit = {
    effectQueue.offer(ReceiveTimeoutSet(d, message))
    super.setReceiveTimeout(d, message)
  }
  override def cancelReceiveTimeout(): Unit = {
    effectQueue.offer(ReceiveTimeoutCancelled)
    super.cancelReceiveTimeout()
  }
  override def scheduleOnce[U](delay: FiniteDuration, target: ActorRef[U], message: U): Cancellable = {
    effectQueue.offer(Scheduled(delay, target, message))
    super.scheduleOnce(delay, target, message)
  }

  override def mkTimer(): TimerSchedulerCrossDslSupport[T] = new TimerSchedulerCrossDslSupport[T] {
    var activeTimers: Map[Any, Effect.TimerScheduled[T]] = Map.empty

    override def startTimerWithFixedDelay(key: Any, msg: T, delay: FiniteDuration): Unit =
      startTimer(key, msg, delay, Effect.TimerScheduled.FixedDelayMode)

    override def startTimerWithFixedDelay(key: Any, msg: T, initialDelay: FiniteDuration, delay: FiniteDuration): Unit =
      startTimer(key, msg, delay, Effect.TimerScheduled.FixedDelayModeWithInitialDelay(initialDelay))

    override def startTimerAtFixedRate(key: Any, msg: T, interval: FiniteDuration): Unit =
      startTimer(key, msg, interval, Effect.TimerScheduled.FixedRateMode)

    override def startTimerAtFixedRate(key: Any, msg: T, initialDelay: FiniteDuration, interval: FiniteDuration): Unit =
      startTimer(key, msg, interval, Effect.TimerScheduled.FixedRateModeWithInitialDelay(initialDelay))

    override def startPeriodicTimer(key: Any, msg: T, interval: FiniteDuration): Unit =
      startTimer(key, msg, interval, Effect.TimerScheduled.FixedRateMode)

    override def startSingleTimer(key: Any, msg: T, delay: FiniteDuration): Unit =
      startTimer(key, msg, delay, Effect.TimerScheduled.SingleMode)

    override def isTimerActive(key: Any): Boolean = activeTimers.isDefinedAt(key)

    override def cancel(key: Any): Unit = if (activeTimers.keySet(key)) {
      val effect = Effect.TimerCancelled(key)
      effectQueue.offer(effect)
      activeTimers -= key
    }

    override def cancelAll(): Unit = activeTimers.foreach(cancel)

    private def sendAction(key: Any): () => Unit = () => {
      activeTimers.get(key).foreach {
        case Effect.TimerScheduled(_, msg, _, mode, _) =>
          mode match {
            case Effect.TimerScheduled.SingleMode =>
              activeTimers -= key
            case _ =>
          }
          self ! msg
      }

    }

    def startTimer(key: Any, msg: T, delay: FiniteDuration, mode: Effect.TimerScheduled.TimerMode) = {
      val effect = Effect.TimerScheduled(key, msg, delay, mode, activeTimers.keySet(key))(sendAction(key))
      activeTimers += (key -> effect)
      effectQueue.offer(effect)
    }
  }
}
