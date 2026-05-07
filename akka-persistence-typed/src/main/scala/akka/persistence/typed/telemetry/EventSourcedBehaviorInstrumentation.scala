/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.telemetry

import scala.collection.immutable
import scala.jdk.CollectionConverters._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.annotation.InternalStableApi
import akka.event.Logging
import akka.util.TopologicalSort.topologicalSort

/**
 * INTERNAL API
 */
@InternalStableApi
object EventSourcedBehaviorInstrumentation {
  type Context = AnyRef
  val EmptyContext: Context = null
}

/**
 * INTERNAL API: Instrumentation SPI for EventSourcedBehavior.
 */
@InternalStableApi
trait EventSourcedBehaviorInstrumentation {
  import EventSourcedBehaviorInstrumentation.Context

  /**
   * Initialize state for an EventSourcedBehavior actor.
   */
  def actorInitialized(actorRef: ActorRef[_]): Unit

  /**
   * Record before a recovery permit is requested.
   *
   * @param actorRef the `ActorRef` for which the recovery permit is about to be requested
   * @return context that will be passed to `afterRequestRecoveryPermit`
   */
  def beforeRequestRecoveryPermit(actorRef: ActorRef[_]): Context

  /**
   * Record after a recovery permit is requested.
   *
   * @param actorRef the `ActorRef` for which the recovery permit is requested
   * @param context  returned by `beforeRequestRecoveryPermit`
   */
  def afterRequestRecoveryPermit(actorRef: ActorRef[_], context: Context): Unit

  /**
   * Record persistence recovery started.
   *
   * @param actorRef the `ActorRef` for which the recovery is started.
   */
  def recoveryStarted(actorRef: ActorRef[_]): Unit

  /**
   * Record persistence recovery done.
   *
   * @param actorRef the `ActorRef` for which the recovery is finished.
   */
  def recoveryDone(actorRef: ActorRef[_]): Unit

  /**
   * Record persistence recovery failure.
   *
   * @param actorRef  the `ActorRef` for which the recovery has failed.
   * @param throwable the cause of the failure.
   * @param event     the event that was replayed, if any (otherwise null)
   */
  def recoveryFailed(actorRef: ActorRef[_], throwable: Throwable, event: Any): Unit

  /**
   * Record persist event.
   *
   * @param actorRef        the `ActorRef` for which the event will be sent to the journal.
   * @param event           the event that was submitted for persistence. For persist of several events it will be
   *                        called for each event in the batch in the same order.
   * @param command         actor message (command), if any (otherwise null), for which the event was emitted.
   * @return context that will be passed to `persistEventWritten`
   */
  def persistEventCalled(actorRef: ActorRef[_], event: Any, command: Any): Context

  /**
   * Record event is written but the registered callback has not been called yet
   *
   * @param actorRef     the `ActorRef` for which the event has been successfully persisted.
   * @param event        the event that was stored in the journal.
   * @param context context returned by `persistEventCalled`
   * @return context that will be passed to `persistEventDone`
   */
  def persistEventWritten(actorRef: ActorRef[_], event: Any, context: Context): Context

  /**
   * Record event is written and the registered callback is called.
   *
   * @param actorRef     the `ActorRef` for which the event has been successfully persisted.
   * @param context context returned by `persistEventWritten`
   */
  def persistEventDone(actorRef: ActorRef[_], context: Context): Unit

  /**
   * Record persistence persist failure.
   *
   * @param actorRef     the `ActorRef` for which the recovery has failed.
   * @param throwable    the cause of the failure.
   * @param event        the event that was to be persisted.
   * @param seqNr        the sequence number associated with the failure
   * @param context context returned by `persistEventCalled`
   */
  def persistFailed(actorRef: ActorRef[_], throwable: Throwable, event: Any, seqNr: Long, context: Context): Unit

  /**
   * Record persistence persist failure.
   *
   * @param actorRef     the `ActorRef` for which the recovery has failed.
   * @param throwable    the cause of the failure.
   * @param event        the event that was to be persisted.
   * @param seqNr        the sequence number associated with the failure
   * @param context context returned by `persistEventCalled`
   */
  def persistRejected(actorRef: ActorRef[_], throwable: Throwable, event: Any, seqNr: Long, context: Context): Unit

  /**
   * Optional dependencies for this instrumentation.
   *
   * Dependency instrumentations will always be ordered before this instrumentation.
   *
   * @return list of class names for optional instrumentation dependencies
   */
  def dependencies: immutable.Seq[String]
}

/**
 * INTERNAL API
 */
@InternalStableApi
object EmptyEventSourcedBehaviorInstrumentation extends EmptyEventSourcedBehaviorInstrumentation

/**
 * INTERNAL API
 */
@InternalStableApi
class EmptyEventSourcedBehaviorInstrumentation extends EventSourcedBehaviorInstrumentation {
  import EventSourcedBehaviorInstrumentation.{ Context, EmptyContext }

  def this(system: ActorSystem[_]) = this()

  override def actorInitialized(actorRef: ActorRef[_]): Unit = ()

  override def beforeRequestRecoveryPermit(actorRef: ActorRef[_]): Context = EmptyContext

  override def afterRequestRecoveryPermit(actorRef: ActorRef[_], context: Context): Unit = ()

  override def recoveryStarted(actorRef: ActorRef[_]): Unit = ()

  override def recoveryDone(actorRef: ActorRef[_]): Unit = ()

  override def recoveryFailed(actorRef: ActorRef[_], throwable: Throwable, event: Any): Unit = ()

  override def persistEventCalled(actorRef: ActorRef[_], event: Any, command: Any): Context = EmptyContext

  override def persistEventWritten(actorRef: ActorRef[_], event: Any, context: Context): Context = EmptyContext

  override def persistEventDone(actorRef: ActorRef[_], context: Context): Unit = ()

  override def persistFailed(
      actorRef: ActorRef[_],
      throwable: Throwable,
      event: Any,
      seqNr: Long,
      context: Context): Unit = ()

  override def persistRejected(
      actorRef: ActorRef[_],
      throwable: Throwable,
      event: Any,
      seqNr: Long,
      context: Context): Unit = ()

  override def dependencies: immutable.Seq[String] = Nil
}

/**
 * INTERNAL API
 */
@InternalStableApi
class EventSourcedBehaviorEnsemble(val instrumentations: Seq[EventSourcedBehaviorInstrumentation])
    extends EventSourcedBehaviorInstrumentation {
  import EventSourcedBehaviorInstrumentation.Context

  override def actorInitialized(actorRef: ActorRef[_]): Unit =
    instrumentations.foreach(_.actorInitialized(actorRef))

  override def beforeRequestRecoveryPermit(actorRef: ActorRef[_]): Context =
    instrumentations.map(_.beforeRequestRecoveryPermit(actorRef))

  override def afterRequestRecoveryPermit(actorRef: ActorRef[_], context: Context): Unit = {
    val contexts = context.asInstanceOf[Seq[Context]]
    contexts.zip(instrumentations).foreach {
      case (ctx, instrumentation) => instrumentation.afterRequestRecoveryPermit(actorRef, ctx)
    }
  }

  override def recoveryStarted(actorRef: ActorRef[_]): Unit =
    instrumentations.foreach(_.recoveryStarted(actorRef))

  override def recoveryDone(actorRef: ActorRef[_]): Unit =
    instrumentations.foreach(_.recoveryDone(actorRef))

  override def recoveryFailed(actorRef: ActorRef[_], throwable: Throwable, event: Any): Unit =
    instrumentations.foreach(_.recoveryFailed(actorRef, throwable, event))

  override def persistEventCalled(actorRef: ActorRef[_], event: Any, command: Any): Context =
    instrumentations.map(_.persistEventCalled(actorRef, event, command))

  override def persistEventWritten(actorRef: ActorRef[_], event: Any, context: Context): Context = {
    val contexts = context.asInstanceOf[Seq[Context]]
    contexts.zip(instrumentations).map {
      case (ctx, instrumentation) => instrumentation.persistEventWritten(actorRef, event, ctx)
    }
  }

  override def persistEventDone(actorRef: ActorRef[_], context: Context): Unit = {
    val contexts = context.asInstanceOf[Seq[Context]]
    contexts.zip(instrumentations).foreach {
      case (ctx, instrumentation) => instrumentation.persistEventDone(actorRef, ctx)
    }
  }

  override def persistFailed(
      actorRef: ActorRef[_],
      throwable: Throwable,
      event: Any,
      seqNr: Long,
      context: Context): Unit = {
    val contexts = context.asInstanceOf[Seq[Context]]
    contexts.zip(instrumentations).foreach {
      case (ctx, instrumentation) => instrumentation.persistFailed(actorRef, throwable, event, seqNr, ctx)
    }
  }

  override def persistRejected(
      actorRef: ActorRef[_],
      throwable: Throwable,
      event: Any,
      seqNr: Long,
      context: Context): Unit = {
    val contexts = context.asInstanceOf[Seq[Context]]
    contexts.zip(instrumentations).foreach {
      case (ctx, instrumentation) => instrumentation.persistRejected(actorRef, throwable, event, seqNr, ctx)
    }
  }

  override def dependencies: immutable.Seq[String] =
    instrumentations.flatMap(_.dependencies)
}

/**
 * INTERNAL API
 */
@InternalStableApi
object EventSourcedBehaviorInstrumentationProvider extends ExtensionId[EventSourcedBehaviorInstrumentationProvider] {
  def createExtension(system: ActorSystem[_]): EventSourcedBehaviorInstrumentationProvider =
    new EventSourcedBehaviorInstrumentationProvider(system)
  def get(system: ActorSystem[_]): EventSourcedBehaviorInstrumentationProvider = apply(system)
}

/**
 * INTERNAL API
 */
@InternalStableApi
class EventSourcedBehaviorInstrumentationProvider(system: ActorSystem[_]) extends Extension {
  private val fqcnConfigPath = "akka.persistence.typed.telemetry.event-sourced-behavior.instrumentations"

  val instrumentation: EventSourcedBehaviorInstrumentation = {
    if (!system.settings.config.hasPath(fqcnConfigPath)) {
      EmptyEventSourcedBehaviorInstrumentation
    } else {
      val fqcns = system.settings.config.getStringList(fqcnConfigPath).asScala.toVector
      fqcns.size match {
        case 0 => EmptyEventSourcedBehaviorInstrumentation
        case 1 => create(fqcns.head)
        case _ =>
          val instrumentationsByFqcn = fqcns.iterator.map(fqcn => fqcn -> create(fqcn)).toMap
          val sortedNames = topologicalSort[String](fqcns, fqcn => instrumentationsByFqcn(fqcn).dependencies.toSet)
          val instrumentations = sortedNames.map(instrumentationsByFqcn).toVector
          new EventSourcedBehaviorEnsemble(instrumentations)
      }
    }
  }

  private def create(fqcn: String): EventSourcedBehaviorInstrumentation = {
    try {
      system.dynamicAccess
        .createInstanceFor[EventSourcedBehaviorInstrumentation](fqcn, immutable.Seq((classOf[ActorSystem[_]], system)))
        .get
    } catch {
      case t: Throwable => // Throwable, because instrumentation failure should not cause fatal shutdown
        Logging(system.classicSystem, classOf[EventSourcedBehaviorInstrumentationProvider])
          .warning(t, "Cannot create instrumentation [{}]", fqcn)
        EmptyEventSourcedBehaviorInstrumentation
    }
  }
}
