/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dungeon

import scala.collection.immutable
import scala.util.control.Exception._
import scala.util.control.NonFatal

import akka.actor.ActorCell
import akka.actor.ActorInterruptedException
import akka.actor.ActorRef
import akka.actor.ActorRefScope
import akka.actor.InternalActorRef
import akka.actor.PostRestartException
import akka.actor.PreRestartException
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.dispatch._
import akka.dispatch.sysmsg._
import akka.event.Logging
import akka.event.Logging.Debug
import akka.event.Logging.Error

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FaultHandling {
  sealed trait FailedInfo
  private case object NoFailedInfo extends FailedInfo
  private final case class FailedRef(ref: ActorRef) extends FailedInfo
  private case object FailedFatally extends FailedInfo
}

private[akka] trait FaultHandling { this: ActorCell =>
  import FaultHandling._

  /* =================
   * T H E   R U L E S
   * =================
   *
   * Actors can be suspended for two reasons:
   * - they fail
   * - their supervisor gets suspended
   *
   * In particular they are not suspended multiple times because of cascading
   * own failures, i.e. while currentlyFailed() they do not fail again. In case
   * of a restart, failures in constructor/preStart count as new failures.
   */

  private def suspendNonRecursive(): Unit = dispatcher.suspend(this)

  private def resumeNonRecursive(): Unit = dispatcher.resume(this)

  /*
   * have we told our supervisor that we Failed() and have not yet heard back?
   * (actually: we might have heard back but not yet acted upon it, in case of
   * a restart with dying children)
   * might well be replaced by ref to a Cancellable in the future (see #2299)
   */
  private var _failed: FailedInfo = NoFailedInfo
  private def isFailed: Boolean = _failed.isInstanceOf[FailedRef]
  private def isFailedFatally: Boolean = _failed eq FailedFatally
  private def perpetrator: ActorRef = _failed match {
    case FailedRef(ref) => ref
    case _              => null
  }
  private def setFailed(perpetrator: ActorRef): Unit = _failed = _failed match {
    case FailedFatally => FailedFatally
    case _             => FailedRef(perpetrator)
  }
  private def clearFailed(): Unit = _failed = _failed match {
    case FailedRef(_) => NoFailedInfo
    case other        => other
  }
  protected def setFailedFatally(): Unit = _failed = FailedFatally

  /**
   * Do re-create the actor in response to a failure.
   */
  protected def faultRecreate(cause: Throwable): Unit =
    if (actor == null) {
      system.eventStream.publish(
        Error(self.path.toString, clazz(actor), "changing Recreate into Create after " + cause))
      faultCreate()
    } else if (isNormal) {
      val failedActor = actor
      if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(failedActor), "restarting"))
      if (failedActor ne null) {
        val optionalMessage = if (currentMessage ne null) Some(currentMessage.message) else None
        try {
          // if the actor fails in preRestart, we can do nothing but log it: it’s best-effort
          if (!isFailedFatally) failedActor.aroundPreRestart(cause, optionalMessage)
        } catch handleNonFatalOrInterruptedException { e =>
          val ex = PreRestartException(self, e, cause, optionalMessage)
          publish(Error(ex, self.path.toString, clazz(failedActor), e.getMessage))
        } finally {
          clearActorFields(failedActor, recreate = true)
        }
      }
      assert(mailbox.isSuspended, "mailbox must be suspended during restart, status=" + mailbox.currentStatus)
      if (!setChildrenTerminationReason(ChildrenContainer.Recreation(cause))) finishRecreate(cause)
    } else {
      // need to keep that suspend counter balanced
      faultResume(causedByFailure = null)
    }

  /**
   * Do suspend the actor in response to a failure of a parent (i.e. the
   * “recursive suspend” feature).
   */
  protected def faultSuspend(): Unit = {
    // done always to keep that suspend counter balanced
    suspendNonRecursive()
    suspendChildren()
  }

  /**
   * Do resume the actor in response to a failure.
   *
   * @param causedByFailure signifies if it was our own failure which
   *        prompted this action.
   */
  protected def faultResume(causedByFailure: Throwable): Unit = {
    if (actor == null) {
      system.eventStream.publish(
        Error(self.path.toString, clazz(actor), "changing Resume into Create after " + causedByFailure))
      faultCreate()
    } else if (isFailedFatally && causedByFailure != null) {
      system.eventStream.publish(
        Error(self.path.toString, clazz(actor), "changing Resume into Restart after " + causedByFailure))
      faultRecreate(causedByFailure)
    } else {
      val perp = perpetrator
      // done always to keep that suspend counter balanced
      // must happen “atomically”
      try resumeNonRecursive()
      finally if (causedByFailure != null) clearFailed()
      resumeChildren(causedByFailure, perp)
    }
  }

  /**
   * Do create the actor in response to a failure.
   */
  protected def faultCreate(): Unit = {
    assert(mailbox.isSuspended, "mailbox must be suspended during failed creation, status=" + mailbox.currentStatus)
    if (!isFailedFatally) assert(perpetrator == self)

    cancelReceiveTimeout()
    cancelReceiveTimeoutTask()

    // stop all children, which will turn childrenRefs into TerminatingChildrenContainer (if there are children)
    children.foreach(stop)

    if (!setChildrenTerminationReason(ChildrenContainer.Creation())) finishCreate()
  }

  private def finishCreate(): Unit = {
    try resumeNonRecursive()
    finally clearFailed()
    try create(None)
    catch handleNonFatalOrInterruptedException { e =>
      handleInvokeFailure(Nil, e)
    }
  }

  protected def terminate(): Unit = {
    cancelReceiveTimeout()
    cancelReceiveTimeoutTask()

    // prevent Deadletter(Terminated) messages
    unwatchWatchedActors(actor)

    // stop all children, which will turn childrenRefs into TerminatingChildrenContainer (if there are children)
    children.foreach(stop)

    if (systemImpl.aborting) {
      // separate iteration because this is a very rare case that should not penalize normal operation
      children.foreach {
        case ref: ActorRefScope if !ref.isLocal =>
          self.sendSystemMessage(DeathWatchNotification(ref, existenceConfirmed = true, addressTerminated = false))
        case _ =>
      }
    }

    val wasTerminating = isTerminating

    if (setChildrenTerminationReason(ChildrenContainer.Termination)) {
      if (!wasTerminating) {
        // do not process normal messages while waiting for all children to terminate
        suspendNonRecursive()
        // do not propagate failures during shutdown to the supervisor
        setFailed(self)
        if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(actor), "stopping"))
      }
    } else {
      setTerminated()
      finishTerminate()
    }
  }

  @InternalStableApi
  final def handleInvokeFailure(childrenNotToSuspend: immutable.Iterable[ActorRef], t: Throwable): Unit = {
    // prevent any further messages to be processed until the actor has been restarted
    if (!isFailed) try {
      suspendNonRecursive()
      // suspend children
      val skip: Set[ActorRef] = currentMessage match {
        case Envelope(Failed(_, _, _), child) => setFailed(child); Set(child)
        case _                                => setFailed(self); Set.empty
      }
      suspendChildren(exceptFor = skip ++ childrenNotToSuspend)
      t match {
        // tell supervisor
        case _: InterruptedException =>
          // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          parent.sendSystemMessage(Failed(self, new ActorInterruptedException(t), uid))
        case _ =>
          // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          parent.sendSystemMessage(Failed(self, t, uid))
      }
    } catch handleNonFatalOrInterruptedException { e =>
      publish(
        Error(
          e,
          self.path.toString,
          clazz(actor),
          "emergency stop: exception in failure handling for " + t.getClass + Logging.stackTraceFor(t)))
      try children.foreach(stop)
      finally finishTerminate()
    }
  }

  private def finishTerminate(): Unit = {
    val a = actor
    /* The following order is crucial for things to work properly. Only change this if you're very confident and lucky.
     *
     * Please note that if a parent is also a watcher then ChildTerminated and Terminated must be processed in this
     * specific order.
     */
    try if (a ne null) a.aroundPostStop()
    catch handleNonFatalOrInterruptedException { e =>
      publish(Error(e, self.path.toString, clazz(a), e.getMessage))
    } finally try stopFunctionRefs()
    finally try dispatcher.detach(this)
    finally try parent.sendSystemMessage(
      DeathWatchNotification(self, existenceConfirmed = true, addressTerminated = false))
    finally try tellWatchersWeDied()
    finally try unwatchWatchedActors(a) // stay here as we expect an emergency stop from handleInvokeFailure
    finally {
      if (system.settings.DebugLifecycle)
        publish(Debug(self.path.toString, clazz(a), "stopped"))

      clearActorFields(a, recreate = false)
      clearFieldsForTermination()
    }
  }

  private def finishRecreate(cause: Throwable): Unit = {
    // need to keep a snapshot of the surviving children before the new actor instance creates new ones
    val survivors = children

    try {
      try resumeNonRecursive()
      finally clearFailed() // must happen in any case, so that failure is propagated

      val freshActor = newActor()

      freshActor.aroundPostRestart(cause)
      checkReceiveTimeout(reschedule = true) // user may have set a receive timeout in preStart which is called from postRestart
      if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(freshActor), "restarted"))

      // only after parent is up and running again do restart the children which were not stopped
      survivors.foreach(
        child =>
          try child.asInstanceOf[InternalActorRef].restart(cause)
          catch handleNonFatalOrInterruptedException { e =>
            publish(Error(e, self.path.toString, clazz(freshActor), "restarting " + child))
          })
    } catch handleNonFatalOrInterruptedException { e =>
      setFailedFatally()
      clearActorFields(actor, recreate = false) // in order to prevent preRestart() from happening again
      handleInvokeFailure(survivors, PostRestartException(self, e, cause))
    }
  }

  final protected def handleFailure(f: Failed): Unit = {
    currentMessage = Envelope(f, f.child, system)
    getChildByRef(f.child) match {
      /*
       * only act upon the failure, if it comes from a currently known child;
       * the UID protects against reception of a Failed from a child which was
       * killed in preRestart and re-created in postRestart
       */
      case Some(stats) if stats.uid == f.uid =>
        if (!actor.supervisorStrategy.handleFailure(this, f.child, f.cause, stats, getAllChildStats)) throw f.cause
      case Some(stats) =>
        publish(
          Debug(
            self.path.toString,
            clazz(actor),
            "dropping Failed(" + f.cause + ") from old child " + f.child + " (uid=" + stats.uid + " != " + f.uid + ")"))
      case None =>
        publish(
          Debug(self.path.toString, clazz(actor), "dropping Failed(" + f.cause + ") from unknown child " + f.child))
    }
  }

  final protected def handleChildTerminated(child: ActorRef): Unit = {
    val status = removeChildAndGetStateChange(child)
    /*
     * if this fails, we do nothing in case of terminating/restarting state,
     * otherwise tell the supervisor etc. (in that second case, the match
     * below will hit the empty default case, too)
     */
    if (actor != null) {
      try actor.supervisorStrategy.handleChildTerminated(this, child, children)
      catch handleNonFatalOrInterruptedException { e =>
        publish(Error(e, self.path.toString, clazz(actor), "handleChildTerminated failed"))
        handleInvokeFailure(Nil, e)
      }
    }
    /*
     * if the removal changed the state of the (terminating) children container,
     * then we are continuing the previously suspended recreate/create/terminate action
     */
    status match {
      case Some(ChildrenContainer.Recreation(cause)) => finishRecreate(cause)
      case Some(ChildrenContainer.Creation())        => finishCreate()
      case Some(ChildrenContainer.Termination)       => finishTerminate()
      case _                                         =>
    }
  }

  final protected def handleNonFatalOrInterruptedException(thunk: (Throwable) => Unit): Catcher[Unit] = {
    case e: InterruptedException =>
      thunk(e)
      Thread.currentThread().interrupt()
    case NonFatal(e) =>
      thunk(e)
  }
}
