/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.nio.channels.{ CancelledKeyException, SelectableChannel, SelectionKey }
import java.nio.channels.ClosedChannelException
import java.nio.channels.SelectionKey._
import java.nio.channels.spi.SelectorProvider
import java.util.{ Iterator => JIterator }
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.control.NonFatal

import com.typesafe.config.Config

import akka.actor._
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.routing.RandomPool
import akka.util.Helpers.Requiring
import akka.util.SerializedSuspendableExecutionContext

abstract class SelectionHandlerSettings(config: Config) {
  import config._

  val MaxChannels: Int = getString("max-channels") match {
    case "unlimited" => -1
    case _           => getInt("max-channels").requiring(_ > 0, "max-channels must be > 0 or 'unlimited'")
  }
  val SelectorAssociationRetries: Int =
    getInt("selector-association-retries").requiring(_ >= 0, "selector-association-retries must be >= 0")

  val SelectorDispatcher: String = getString("selector-dispatcher")
  val WorkerDispatcher: String = getString("worker-dispatcher")
  val TraceLogging: Boolean = getBoolean("trace-logging")

  def MaxChannelsPerSelector: Int
}

/**
 * Interface behind which we hide our selector management logic from the connection actors
 */
private[io] trait ChannelRegistry {

  /**
   * Registers the given channel with the selector, creates a ChannelRegistration instance for it
   * and dispatches it back to the channelActor calling this `register`
   */
  def register(channel: SelectableChannel, initialOps: Int)(implicit channelActor: ActorRef): Unit
}

/**
 * Implementations of this interface are sent as actor messages back to a channel actor as
 * a result of it having called `register` on the `ChannelRegistry`.
 * Enables a channel actor to directly schedule interest setting tasks to the selector management dispatcher.
 */
private[io] trait ChannelRegistration extends NoSerializationVerificationNeeded {
  def enableInterest(op: Int): Unit
  def disableInterest(op: Int): Unit

  /**
   * Explicitly cancel the registration and close the underlying channel. Then run the given `andThen` method.
   * The `andThen` method is run from another thread so make sure it's safe to execute from there.
   */
  def cancelAndClose(andThen: () => Unit): Unit
}

private[io] object SelectionHandler {
  // Let select return every MaxSelectMillis which will automatically cleanup stale entries in the selection set.
  // Otherwise, an idle Selector might block for a long time keeping a reference to the dead connection actor's ActorRef
  // which might keep other stuff in memory.
  // See https://github.com/akka/akka-core/issues/23437
  // As this is basic house-keeping functionality it doesn't seem useful to make the value configurable.
  val MaxSelectMillis = 10000 // wake up once in 10 seconds

  trait HasFailureMessage {
    def failureMessage: Any
  }

  final case class WorkerForCommand(
      apiCommand: HasFailureMessage,
      commander: ActorRef,
      childProps: ChannelRegistry => Props)
      extends NoSerializationVerificationNeeded

  final case class Retry(command: WorkerForCommand, retriesLeft: Int) extends NoSerializationVerificationNeeded {
    require(retriesLeft >= 0)
  }

  case object ChannelConnectable
  case object ChannelAcceptable
  case object ChannelReadable extends DeadLetterSuppression
  case object ChannelWritable extends DeadLetterSuppression

  private[io] abstract class SelectorBasedManager(selectorSettings: SelectionHandlerSettings, nrOfSelectors: Int)
      extends Actor {

    override def supervisorStrategy = connectionSupervisorStrategy

    val selectorPool: ActorRef = {
      val routeeProps =
        Props(new SelectionHandler(selectorSettings)).withDispatcher(context.props.dispatcher).withDeploy(Deploy.local)
      context.actorOf(
        props = RandomPool(nrOfSelectors, routerDispatcher = context.props.dispatcher)
          .props(routeeProps)
          .withDeploy(Deploy.local),
        name = "selectors")
    }

    final def workerForCommandHandler(pf: PartialFunction[HasFailureMessage, ChannelRegistry => Props]): Receive = {
      case cmd: HasFailureMessage if pf.isDefinedAt(cmd) => selectorPool ! WorkerForCommand(cmd, sender(), pf(cmd))
    }
  }

  /**
   * Special supervisor strategy for parents of TCP connection and listener actors.
   * Stops the child on all errors and logs DeathPactExceptions only at debug level.
   */
  private[io] final val connectionSupervisorStrategy: SupervisorStrategy =
    new OneForOneStrategy()(SupervisorStrategy.stoppingStrategy.decider) {
      override def logFailure(
          context: ActorContext,
          child: ActorRef,
          cause: Throwable,
          decision: SupervisorStrategy.Directive): Unit =
        if (cause.isInstanceOf[DeathPactException]) {
          try context.system.eventStream.publish {
            Logging.Debug(child.path.toString, getClass, "Closed after handler termination")
          } catch { case NonFatal(_) => }
        } else super.logFailure(context, child, cause, decision)
    }

  private class ChannelRegistryImpl(
      executionContext: ExecutionContext,
      settings: SelectionHandlerSettings,
      log: LoggingAdapter)
      extends ChannelRegistry {
    private[this] val selector = SelectorProvider.provider.openSelector
    private[this] val wakeUp = new AtomicBoolean(false)

    final val OP_READ_AND_WRITE = OP_READ | OP_WRITE // compile-time constant

    private[this] val select = new Task {
      def tryRun(): Unit = {
        if (selector.select(MaxSelectMillis) > 0) { // This assumes select return value == selectedKeys.size
          val keys = selector.selectedKeys
          val iterator = keys.iterator()
          while (iterator.hasNext) {
            val key = iterator.next()
            if (key.isValid) {
              try {
                // Cache because the performance implications of calling this on different platforms are not clear
                val readyOps = key.readyOps()
                key.interestOps(key.interestOps & ~readyOps) // prevent immediate reselection by always clearing
                val connection = key.attachment.asInstanceOf[ActorRef]
                readyOps match {
                  case OP_READ                   => connection ! ChannelReadable
                  case OP_WRITE                  => connection ! ChannelWritable
                  case OP_READ_AND_WRITE         => { connection ! ChannelWritable; connection ! ChannelReadable }
                  case x if (x & OP_ACCEPT) > 0  => connection ! ChannelAcceptable
                  case x if (x & OP_CONNECT) > 0 => connection ! ChannelConnectable
                  case x                         => log.warning("Invalid readyOps: [{}]", x)
                }
              } catch {
                case _: CancelledKeyException =>
                // can be ignored because this exception is triggered when the key becomes invalid
                // because `channel.close()` in `TcpConnection.postStop` is called from another thread
              }
            }
          }
          keys.clear() // we need to remove the selected keys from the set, otherwise they remain selected
        }
        wakeUp.set(false)
      }

      override def run(): Unit =
        if (selector.isOpen)
          try super.run()
          finally executionContext.execute(this) // re-schedule select behind all currently queued tasks
    }

    executionContext.execute(select) // start selection "loop"

    def register(channel: SelectableChannel, initialOps: Int)(implicit channelActor: ActorRef): Unit = {
      if (settings.TraceLogging) log.debug(s"Scheduling Registering channel $channel with initialOps $initialOps")
      execute {
        new Task {
          def tryRun(): Unit =
            try {
              if (settings.TraceLogging) log.debug(s"Registering channel $channel with initialOps $initialOps")
              val key = channel.register(selector, initialOps, channelActor)
              channelActor ! new ChannelRegistration {
                def enableInterest(ops: Int): Unit = enableInterestOps(key, ops)

                def disableInterest(ops: Int): Unit = disableInterestOps(key, ops)

                def cancelAndClose(andThen: () => Unit): Unit = cancelKeyAndClose(key, andThen)
              }
            } catch {
              case _: ClosedChannelException =>
              // ignore, might happen if a connection is closed in the same moment as an interest is registered
            }
        }
      }
    }

    def shutdown(): Unit =
      execute {
        new Task {
          def tryRun(): Unit = {
            // thorough 'close' of the Selector
            @tailrec def closeNextChannel(it: JIterator[SelectionKey]): Unit = if (it.hasNext) {
              try it.next().channel.close()
              catch { case NonFatal(e) => log.debug("Error closing channel: {}", e) }
              closeNextChannel(it)
            }
            try closeNextChannel(selector.keys.iterator)
            finally selector.close()
          }
        }
      }

    // always set the interest keys on the selector thread,
    // benchmarks show that not doing so results in lock contention
    private def enableInterestOps(key: SelectionKey, ops: Int): Unit =
      execute {
        new Task {
          def tryRun(): Unit = {
            if (settings.TraceLogging) log.debug(s"Enabling $ops on $key")
            val currentOps = key.interestOps
            val newOps = currentOps | ops
            if (newOps != currentOps) key.interestOps(newOps)
          }
        }
      }

    private def cancelKeyAndClose(key: SelectionKey, andThen: () => Unit): Unit =
      execute {
        new Task {
          def tryRun(): Unit = {
            Try(key.cancel())
            Try(key.channel().close())

            // In JDK 11 (and for Windows also in previous JDKs), it is necessary to completely flush a cancelled / closed channel
            // from the selector to close a channel completely on the OS level.
            // We want select to be called before we call the thunk, so we schedule the thunk here which will run it
            // after the next select call.
            // (It's tempting to just call `selectNow` here, instead of registering the thunk, but that can mess up
            // the wakeUp state of the selector leading to selection operations being stuck behind the next selection
            // until that returns regularly the next time.)

            runThunk(andThen)
          }
        }
      }

    private def runThunk(andThen: () => Unit): Unit =
      execute {
        new Task {
          def tryRun(): Unit = andThen()
        }
      }

    private def disableInterestOps(key: SelectionKey, ops: Int): Unit =
      execute {
        new Task {
          def tryRun(): Unit = {
            val currentOps = key.interestOps
            val newOps = currentOps & ~ops
            if (newOps != currentOps) key.interestOps(newOps)
          }
        }
      }

    private def execute(task: Task): Unit = {
      executionContext.execute(task)
      if (wakeUp.compareAndSet(false, true)) // if possible avoid syscall and trade off with LOCK CMPXCHG
        selector.wakeup()
    }

    // FIXME: Add possibility to signal failure of task to someone
    private abstract class Task extends Runnable {
      def tryRun(): Unit
      def run(): Unit = {
        try tryRun()
        catch {
          case _: CancelledKeyException => // ok, can be triggered while setting interest ops
          case NonFatal(e)              => log.error(e, "Error during selector management task: [{}]", e)
        }
      }
    }
  }
}

private[io] class SelectionHandler(settings: SelectionHandlerSettings)
    extends Actor
    with ActorLogging
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
  import SelectionHandler._
  import settings._

  private[this] var sequenceNumber = 0L // should be Long to prevent overflow
  private[this] var childCount = 0
  private[this] val registry = {
    val dispatcher = context.system.dispatchers.lookup(SelectorDispatcher)
    new ChannelRegistryImpl(SerializedSuspendableExecutionContext(dispatcher.throughput)(dispatcher), settings, log)
  }

  def receive: Receive = {
    case cmd: WorkerForCommand => spawnChildWithCapacityProtection(cmd, SelectorAssociationRetries)

    case Retry(cmd, retriesLeft) => spawnChildWithCapacityProtection(cmd, retriesLeft)

    // since our ActorRef is never exposed to the user and we are only assigning watches to our
    // children all incoming `Terminated` events must be for a child of ours
    case _: Terminated => childCount -= 1
  }

  override def postStop(): Unit = registry.shutdown()

  // we can never recover from failures of a connection or listener child
  // and log the failure at debug level
  override def supervisorStrategy = {
    def stoppingDecider: SupervisorStrategy.Decider = {
      case _: Exception => SupervisorStrategy.Stop
    }
    new OneForOneStrategy()(stoppingDecider) {
      override def logFailure(
          context: ActorContext,
          child: ActorRef,
          cause: Throwable,
          decision: SupervisorStrategy.Directive): Unit =
        try {
          val logMessage = cause match {
            case e: ActorInitializationException if (e.getCause ne null) && (e.getCause.getMessage ne null) =>
              e.getCause.getMessage
            case e: ActorInitializationException if e.getCause ne null =>
              e.getCause match {
                case ie: java.lang.reflect.InvocationTargetException => ie.getTargetException.toString
                case t: Throwable                                    => Logging.simpleName(t)
              }
            case e => e.getMessage
          }
          context.system.eventStream.publish(Logging.Debug(child.path.toString, classOf[SelectionHandler], logMessage))
        } catch { case NonFatal(_) => }
    }
  }

  def spawnChildWithCapacityProtection(cmd: WorkerForCommand, retriesLeft: Int): Unit = {
    if (TraceLogging) log.debug("Executing [{}]", cmd)
    if (MaxChannelsPerSelector == -1 || childCount < MaxChannelsPerSelector) {
      val newName = sequenceNumber.toString
      sequenceNumber += 1
      val child = context.actorOf(
        props = cmd.childProps(registry).withDispatcher(WorkerDispatcher).withDeploy(Deploy.local),
        name = newName)
      childCount += 1
      if (MaxChannelsPerSelector > 0) context.watch(child) // we don't need to watch if we aren't limited
    } else {
      if (retriesLeft >= 1) {
        log.debug("Rejecting [{}] with [{}] retries left, retrying...", cmd, retriesLeft)
        context.parent.forward(Retry(cmd, retriesLeft - 1))
      } else {
        log.warning("Rejecting [{}] with no retries left, aborting...", cmd)
        cmd.commander ! cmd.apiCommand.failureMessage // I can't do it, Captain!
      }
    }
  }
}
