/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import java.util.concurrent.ConcurrentLinkedQueue

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import akka.actor.typed.Behavior
import akka.actor.typed.internal.BehaviorImpl.DeferredBehavior
import akka.actor.typed.scaladsl.{ AbstractBehavior, ActorContext, Behaviors }
import akka.annotation.InternalApi
import akka.persistence.testkit.{ javadsl, scaladsl }
import akka.persistence.typed.internal.EventSourcedBehaviorImpl
import akka.persistence.typed.internal.Running.WithSeqNrAccessible
import akka.persistence.typed.state.internal.DurableStateBehaviorImpl
import akka.persistence.typed.state.internal.Running.WithRevisionAccessible
import akka.util.ConstantFun.{ scalaAnyToUnit => doNothing }
import akka.util.ccompat.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object Unpersistent {

  def eventSourced[Command, Event, State](behavior: Behavior[Command], fromStateAndSequenceNr: Option[(State, Long)])(
      onEvent: (Event, Long, Set[String]) => Unit)(onSnapshot: (State, Long) => Unit): Behavior[Command] = {
    @tailrec
    def findEventSourcedBehavior(
        b: Behavior[Command],
        context: ActorContext[Command]): Option[EventSourcedBehaviorImpl[Command, Event, State]] = {
      b match {
        case es: EventSourcedBehaviorImpl[Command, _, _] =>
          Some(es.asInstanceOf[EventSourcedBehaviorImpl[Command, Event, State]])

        case deferred: DeferredBehavior[Command] =>
          findEventSourcedBehavior(deferred(context), context)

        case _ => None
      }
    }

    Behaviors.setup[Command] { context =>
      findEventSourcedBehavior(behavior, context).fold {
        throw new AssertionError("Did not find the expected EventSourcedBehavior")
      } { esBehavior =>
        val (initialState, initialSequenceNr) = fromStateAndSequenceNr.getOrElse(esBehavior.emptyState -> 0L)
        new WrappedEventSourcedBehavior(context, esBehavior, initialState, initialSequenceNr, onEvent, onSnapshot)
      }
    }
  }

  def durableState[Command, State](behavior: Behavior[Command], fromState: Option[State])(
      onPersist: (State, Long, String) => Unit): Behavior[Command] = {

    @tailrec
    def findDurableStateBehavior(
        b: Behavior[Command],
        context: ActorContext[Command]): Option[DurableStateBehaviorImpl[Command, State]] =
      b match {
        case ds: DurableStateBehaviorImpl[Command, _] =>
          Some(ds.asInstanceOf[DurableStateBehaviorImpl[Command, State]])

        case deferred: DeferredBehavior[Command] => findDurableStateBehavior(deferred(context), context)
        case _                                   => None
      }

    Behaviors.setup[Command] { context =>
      findDurableStateBehavior(behavior, context).fold {
        throw new AssertionError("Did not find the expected DurableStateBehavior")
      } { dsBehavior =>
        val initialState = fromState.getOrElse(dsBehavior.emptyState)
        new WrappedDurableStateBehavior(context, dsBehavior, initialState, onPersist)
      }
    }
  }

  private class WrappedEventSourcedBehavior[Command, Event, State](
      context: ActorContext[Command],
      esBehavior: EventSourcedBehaviorImpl[Command, Event, State],
      initialState: State,
      initialSequenceNr: Long,
      onEvent: (Event, Long, Set[String]) => Unit,
      onSnapshot: (State, Long) => Unit)
      extends AbstractBehavior[Command](context)
      with WithSeqNrAccessible {
    import akka.persistence.typed.{ EventSourcedSignal, RecoveryCompleted, SnapshotCompleted, SnapshotMetadata }
    import akka.persistence.typed.internal._

    override def currentSequenceNumber: Long = sequenceNr

    private def commandHandler = esBehavior.commandHandler
    private def eventHandler = esBehavior.eventHandler
    private def tagger = esBehavior.tagger
    private def snapshotWhen = esBehavior.snapshotWhen
    private def retention = esBehavior.retention
    private def signalHandler = esBehavior.signalHandler

    private var sequenceNr: Long = initialSequenceNr
    private var state: State = initialState
    private val stashedCommands = ListBuffer.empty[Command]

    private def snapshotMetadata() =
      SnapshotMetadata(esBehavior.persistenceId.toString, sequenceNr, System.currentTimeMillis())
    private def sendSignal(signal: EventSourcedSignal): Unit =
      signalHandler.applyOrElse(state -> signal, doNothing)

    sendSignal(RecoveryCompleted)

    override def onMessage(cmd: Command): Behavior[Command] = {
      var shouldSnapshot = false
      var shouldUnstash = false
      var shouldStop = false

      def snapshotRequested(evt: Event): Boolean = {
        val snapshotFromRetention = retention match {
          case DisabledRetentionCriteria             => false
          case s: SnapshotCountRetentionCriteriaImpl => s.snapshotWhen(sequenceNr)
          case unexpected                            => throw new IllegalStateException(s"Unexpected retention criteria: $unexpected")
        }

        snapshotFromRetention || snapshotWhen(state, evt, sequenceNr)
      }

      @tailrec
      def applyEffects(curEffect: EffectImpl[Event, State], sideEffects: immutable.Seq[SideEffect[State]]): Unit =
        curEffect match {
          case CompositeEffect(eff: EffectImpl[Event, State], se) =>
            applyEffects(eff, se ++ sideEffects)

          case Persist(event) =>
            sequenceNr += 1
            state = eventHandler(state, event)
            onEvent(event, sequenceNr, tagger(state, event))
            shouldSnapshot = shouldSnapshot || snapshotRequested(event)
            sideEffect(sideEffects)

          case PersistAll(events) =>
            val eventsWithSeqNrsAndTags =
              events.map { event =>
                sequenceNr += 1
                state = eventHandler(state, event)
                val tags = tagger(state, event)
                (event, sequenceNr, tags)
              }

            eventsWithSeqNrsAndTags.foreach {
              case (event, seqNr, tags) =>
                // technically doesn't persist them atomically, but in tests that shouldn't matter
                onEvent(event, seqNr, tags)
                shouldSnapshot = shouldSnapshot || snapshotRequested(event)
            }

            sideEffect(sideEffects)

          // From outside of the behavior, these are equivalent: no state update
          case _: PersistNothing.type | _: Unhandled.type =>
            sideEffect(sideEffects)

          case _: Stash.type =>
            stashedCommands.append(cmd)
            sideEffect(sideEffects)

          case _ =>
            context.log.error("Unexpected effect {}, stopping", curEffect)
            Behaviors.stopped
        }

      def sideEffect(sideEffects: immutable.Seq[SideEffect[State]]): Unit =
        sideEffects.iterator.foreach { effect =>
          effect match {
            case _: Stop.type       => shouldStop = true
            case _: UnstashAll.type => shouldUnstash = true
            case cb: Callback[_]    => cb.sideEffect(state)
          }
        }

      applyEffects(commandHandler(state, cmd).asInstanceOf[EffectImpl[Event, State]], Nil)

      if (shouldSnapshot) {
        onSnapshot(state, sequenceNr)
        sendSignal(SnapshotCompleted(snapshotMetadata()))
      }

      if (shouldStop) Behaviors.stopped
      else if (shouldUnstash && stashedCommands.nonEmpty) {
        val numStashed = stashedCommands.length
        val thisWrappedBehavior = this

        Behaviors.setup { _ =>
          Behaviors.withStash(numStashed) { stash =>
            stashedCommands.foreach { sc =>
              stash.stash(sc)
              ()
            }

            stashedCommands.remove(0, numStashed)
            stash.unstashAll(thisWrappedBehavior)
          }
        }
      } else this
    }
  }

  private class WrappedDurableStateBehavior[Command, State](
      context: ActorContext[Command],
      dsBehavior: DurableStateBehaviorImpl[Command, State],
      initialState: State,
      onPersist: (State, Long, String) => Unit)
      extends AbstractBehavior[Command](context)
      with WithRevisionAccessible {

    import akka.persistence.typed.state.{ DurableStateSignal, RecoveryCompleted }
    import akka.persistence.typed.state.internal._

    override def currentRevision: Long = sequenceNr

    private def commandHandler = dsBehavior.commandHandler
    private def signalHandler = dsBehavior.signalHandler
    private val tag = dsBehavior.tag

    private var sequenceNr: Long = 0
    private var state: State = initialState
    private val stashedCommands = ListBuffer.empty[Command]

    private def sendSignal(signal: DurableStateSignal): Unit =
      signalHandler.applyOrElse(state -> signal, doNothing)

    sendSignal(RecoveryCompleted)

    override def onMessage(cmd: Command): Behavior[Command] = {
      var shouldUnstash = false
      var shouldStop = false

      def persistState(st: State): Unit = {
        sequenceNr += 1
        onPersist(st, sequenceNr, tag)
        state = st
      }

      @tailrec
      def applyEffects(curEffect: EffectImpl[State], sideEffects: immutable.Seq[SideEffect[State]]): Unit =
        curEffect match {
          case CompositeEffect(eff: EffectImpl[_], se) =>
            applyEffects(eff.asInstanceOf[EffectImpl[State]], se ++ sideEffects)

          case Persist(st) =>
            persistState(st)
            sideEffect(sideEffects)

          case _: PersistNothing.type | _: Unhandled.type =>
            sideEffect(sideEffects)

          case _: Stash.type =>
            stashedCommands.append(cmd)
            sideEffect(sideEffects)

          case _ =>
            context.log.error("Unexpected effect, stopping")
            Behaviors.stopped
        }

      def sideEffect(sideEffects: immutable.Seq[SideEffect[State]]): Unit =
        sideEffects.iterator.foreach { effect =>
          effect match {
            case _: Stop.type       => shouldStop = true
            case _: UnstashAll.type => shouldUnstash = true
            case cb: Callback[_]    => cb.sideEffect(state)
          }
        }

      applyEffects(commandHandler(state, cmd).asInstanceOf[EffectImpl[State]], Nil)

      if (shouldStop) Behaviors.stopped
      else if (shouldUnstash && stashedCommands.nonEmpty) {
        val numStashed = stashedCommands.length
        val thisWrappedBehavior = this

        Behaviors.setup { _ =>
          Behaviors.withStash(numStashed) { stash =>
            stashedCommands.foreach { sc =>
              stash.stash(sc)
              () // explicit discard
            }
            stashedCommands.remove(0, numStashed)
            stash.unstashAll(thisWrappedBehavior)
          }
        }
      } else this
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class PersistenceProbeImpl[T] {
  type Element = (T, Long, Set[String])

  val queue = new ConcurrentLinkedQueue[Element]()

  def persist(elem: Element): Unit = { queue.offer(elem); () }

  def rawExtract(): Element =
    queue.poll() match {
      case null => throw new AssertionError("No persistence effects in probe")
      case elem => elem
    }

  def asScala: scaladsl.PersistenceProbe[T] =
    new scaladsl.PersistenceProbe[T] {
      import scaladsl.{ PersistenceEffect, PersistenceProbe }

      def drain(): Seq[PersistenceEffect[T]] = {
        @annotation.tailrec
        def iter(acc: List[PersistenceEffect[T]]): List[PersistenceEffect[T]] = {
          val elem = queue.poll()
          if (elem == null) acc else iter(persistenceEffect(elem) :: acc)
        }

        iter(Nil).reverse
      }

      def extract(): PersistenceEffect[T] = persistenceEffect(rawExtract())

      def expectPersistedType[S <: T: ClassTag](): PersistenceEffect[S] =
        rawExtract() match {
          case (obj: S, sequenceNr, tags) => PersistenceEffect(obj, sequenceNr, tags)
          case (extracted, _, _) =>
            throw new AssertionError(
              s"Expected object of type [${implicitly[ClassTag[S]].runtimeClass.getName}] to be persisted, " +
              s"but actual was of type [${extracted.getClass.getName}]")
        }

      def hasEffects: Boolean = !queue.isEmpty

      def expectPersisted(obj: T): PersistenceProbe[T] =
        rawExtract() match {
          case (persistedObj, _, _) if obj == persistedObj => this
          case (persistedObj, _, _) =>
            throw new AssertionError(s"Expected object [$obj] to be persisted, but actual was [$persistedObj]")
        }

      def expectPersisted(obj: T, tag: String): PersistenceProbe[T] =
        rawExtract() match {
          case (persistedObj, _, tags) if (obj == persistedObj) && (tags(tag)) => this

          case (persistedObj, _, tags) if obj == persistedObj =>
            throw new AssertionError(
              s"Expected persistence with tag [$tag], but actual tags were [${tags.mkString(",")}]")

          case (persistedObj, _, tags) if tags(tag) =>
            throw new AssertionError(s"Expected object [$obj] to be persisted, but actual was [$persistedObj]")

          case (persistedObj, _, tags) =>
            throw new AssertionError(
              s"Expected object [$obj] to be persisted with tag [$tag], " +
              s"but actual object was [$persistedObj] with tags [${tags.mkString(",")}]")
        }

      def expectPersisted(obj: T, tags: Set[String]): PersistenceProbe[T] =
        rawExtract() match {
          case (persistedObj, _, persistedTags) if (obj == persistedObj) && (tags == persistedTags) => this
          case (persistedObj, _, persistedTags) if obj == persistedObj =>
            val unexpected = persistedTags.diff(tags)
            val notPersistedWith = tags.diff(persistedTags)

            throw new AssertionError(
              s"Expected persistence with [${tags.mkString(",")}], " +
              s"but saw unexpected actual tags [${unexpected.mkString(",")}] and " +
              s"did not see actual tags [${notPersistedWith.mkString(",")}]")

          case (persistedObj, _, persistedTags) if tags == persistedTags =>
            throw new AssertionError(s"Expected object [$obj] to be persisted, but actual was [$persistedObj}]")

          case (persistedObj, _, persistedTags) =>
            throw new AssertionError(
              s"Expected object [$obj] to be persisted with tags [${tags.mkString(",")}], " +
              s"but actual object was [$persistedObj] with tags [${persistedTags.mkString(",")}]")
        }

      private def persistenceEffect(elem: Element): PersistenceEffect[T] =
        PersistenceEffect(elem._1, elem._2, elem._3)
    }

  def asJava: javadsl.PersistenceProbe[T] =
    new javadsl.PersistenceProbe[T] {
      import java.util.{ List => JList, Set => JSet }

      import javadsl.{ PersistenceEffect, PersistenceProbe }

      def drain(): JList[PersistenceEffect[T]] = {
        @annotation.tailrec
        def iter(acc: List[PersistenceEffect[T]]): List[PersistenceEffect[T]] = {
          val elem = queue.poll()
          if (elem == null) acc else iter(persistenceEffect(elem) :: acc)
        }

        iter(Nil).reverse.asJava
      }

      def extract(): PersistenceEffect[T] = persistenceEffect(rawExtract())

      def expectPersistedClass[S <: T](clazz: Class[S]): PersistenceEffect[S] =
        rawExtract() match {
          case (obj, sequenceNr, tags) if clazz.isInstance(obj) =>
            PersistenceEffect(clazz.cast(obj), sequenceNr, tags.asJava)

          case (extracted, _, _) =>
            throw new AssertionError(
              s"Expected object of type [${clazz.getName}] to be persisted, " +
              s"but actual was of type [${extracted.getClass.getName}]")
        }

      def hasEffects: Boolean = !queue.isEmpty

      def expectPersisted(obj: T): PersistenceProbe[T] =
        rawExtract() match {
          case (persistedObj, _, _) if obj == persistedObj => this
          case (persistedObj, _, _) =>
            throw new AssertionError(s"Expected object [$obj] to be persisted, but actual was [$persistedObj]")
        }

      def expectPersisted(obj: T, tag: String): PersistenceProbe[T] =
        rawExtract() match {
          case (persistedObj, _, tags) if (obj == persistedObj) && (tags(tag)) => this

          case (persistedObj, _, tags) if obj == persistedObj =>
            throw new AssertionError(
              s"Expected persistence with tag [$tag], but actual tags were [${tags.mkString(",")}]")

          case (persistedObj, _, tags) if tags(tag) =>
            throw new AssertionError(s"Expected object [$obj] to be persisted, but actual was [$persistedObj]")

          case (persistedObj, _, tags) =>
            throw new AssertionError(
              s"Expected object [$obj] to be persisted with tag [$tag], " +
              s"but actual object was [$persistedObj] with tags [${tags.mkString(",")}]")
        }

      def expectPersisted(obj: T, tags: JSet[String]): PersistenceProbe[T] = {
        val sTags = tags.asScala

        // Not sure if a Java Set after asScala-ing will compare equal to a Scala Set...
        def sameTags(persistedTags: Set[String]): Boolean =
          sTags.forall(persistedTags) && persistedTags.forall(sTags)

        rawExtract() match {
          case (persistedObj, _, persistedTags) if (obj == persistedObj) && sameTags(persistedTags) => this

          case (persistedObj, _, persistedTags) if obj == persistedObj =>
            val unexpected = persistedTags.diff(sTags)
            val notPersistedWith = sTags.diff(persistedTags)

            throw new AssertionError(
              s"Expected persistence with [${sTags.mkString(",")}], " +
              s"but saw unexpected actual tags [${unexpected.mkString(",")}] and " +
              s"did not see actual tags [${notPersistedWith.mkString(",")}]")

          case (persistedObj, _, persistedTags) if sameTags(persistedTags) =>
            throw new AssertionError(s"Expected object [$obj] to be persisted, but actual was [$persistedObj}]")

          case (persistedObj, _, persistedTags) =>
            throw new AssertionError(
              s"Expected object [$obj] to be persisted with tags [${sTags.mkString(",")}], " +
              s"but actual object was [$persistedObj] with tags [${persistedTags.mkString(",")}]")
        }
      }

      private def persistenceEffect(element: Element): PersistenceEffect[T] =
        PersistenceEffect(element._1, element._2, element._3.asJava)
    }
}
