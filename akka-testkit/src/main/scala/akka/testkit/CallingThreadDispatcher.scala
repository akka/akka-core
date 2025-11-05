/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import com.typesafe.config.Config
import language.postfixOps

import akka.actor.{
  ActorCell,
  ActorInitializationException,
  ActorRef,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.dispatch.{
  DefaultSystemMessageQueue,
  DispatcherPrerequisites,
  Envelope,
  Mailbox,
  MailboxType,
  MessageDispatcher,
  MessageDispatcherConfigurator,
  MessageQueue,
  TaskInvocation
}
import akka.dispatch.sysmsg.{ Resume, Suspend, SystemMessage }
import akka.util.Switch

/*
 * Locking rules:
 *
 * Normal messages are always queued thread locally.
 * Processing a queue checks suspendSwitch before each invocation, not processing
 * if suspendSwitch.
 * When resuming an actor, all messages are atomically scooped from all threads and
 * queued on the resuming thread's queue, to be processed immediately.
 * Scooping up messages means replacing the ThreadLocal contents with an empty
 * new MessageQueue.
 *
 * All accesses to the queue must be done under the suspendSwitch-switch's lock, so
 * within one of its methods taking a closure argument.
 *
 * System messages always go directly to the actors SystemMessageQueue which isn't thread local.
 */

private[testkit] object CallingThreadDispatcherQueues
    extends ExtensionId[CallingThreadDispatcherQueues]
    with ExtensionIdProvider {
  override def lookup = CallingThreadDispatcherQueues
  override def createExtension(system: ExtendedActorSystem): CallingThreadDispatcherQueues =
    new CallingThreadDispatcherQueues
}

private[testkit] class CallingThreadDispatcherQueues extends Extension {

  // PRIVATE DATA

  private var queues = Map[CallingThreadMailbox, Set[WeakReference[MessageQueue]]]()
  private var lastGC = 0L

  // we have to forget about long-gone threads sometime
  private def gc(): Unit = {
    queues = queues
      .foldLeft(Map.newBuilder[CallingThreadMailbox, Set[WeakReference[MessageQueue]]]) {
        case (m, (k, v)) =>
          val nv = v.filter(_.get ne null)
          if (nv.isEmpty) m else m += (k -> nv)
      }
      .result()
  }

  protected[akka] def registerQueue(mbox: CallingThreadMailbox, q: MessageQueue): Unit = synchronized {
    if (queues contains mbox) {
      val newSet = queues(mbox) + new WeakReference(q)
      queues += mbox -> newSet
    } else {
      queues += mbox -> Set(new WeakReference(q))
    }
    val now = System.nanoTime
    if (now - lastGC > 1000000000L) {
      lastGC = now
      gc()
    }
  }

  protected[akka] def unregisterQueues(mbox: CallingThreadMailbox): Unit = synchronized {
    queues -= mbox
  }

  /*
   * This method must be called with "own" being this thread's queue for the
   * given mailbox. When this method returns, the queue will be entered
   * (active).
   */
  protected[akka] def gatherFromAllOtherQueues(mbox: CallingThreadMailbox, own: MessageQueue): Unit = synchronized {
    if (queues contains mbox) {
      for {
        ref <- queues(mbox)
        q = ref.get
        if (q ne null) && (q ne own)
      } {
        val owner = mbox.actor.self
        var msg = q.dequeue()
        while (msg ne null) {
          // this is safe because this method is only ever called while holding the suspendSwitch monitor
          own.enqueue(owner, msg)
          msg = q.dequeue()
        }
      }
    }
  }
}

object CallingThreadDispatcher {
  val Id = "akka.test.calling-thread-dispatcher"
}

/**
 * Dispatcher which runs invocations on the current thread only. This
 * dispatcher does not create any new threads, but it can be used from
 * different threads concurrently for the same actor. The dispatch strategy is
 * to run on the current thread unless the target actor is either suspendSwitch or
 * already running on the current thread (if it is running on a different
 * thread, then this thread will block until that other invocation is
 * finished); if the invocation is not run, it is queued in a thread-local
 * queue to be executed once the active invocation further up the call stack
 * finishes. This leads to completely deterministic execution order if only one
 * thread is used.
 *
 * Suspending and resuming are global actions for one actor, meaning they can
 * affect different threads, which leads to complications. If messages are
 * queued (thread-locally) during the suspendSwitch period, the only thread to run
 * them upon resume is the thread actually calling the resume method. Hence,
 * all thread-local queues which are not currently being drained (possible,
 * since suspend-queue-resume might happen entirely during an invocation on a
 * different thread) are scooped up into the current thread-local queue which
 * is then executed. It is possible to suspend an actor from within its call
 * stack.
 *
 * @since 1.1
 */
class CallingThreadDispatcher(_configurator: MessageDispatcherConfigurator) extends MessageDispatcher(_configurator) {
  import CallingThreadDispatcher._

  val log = akka.event.Logging(eventStream, classOf[CallingThreadDispatcher])

  override def id: String = Id

  protected[akka] override def createMailbox(actor: akka.actor.Cell, mailboxType: MailboxType) =
    new CallingThreadMailbox(actor, mailboxType)

  protected[akka] override def shutdown(): Unit = {}

  protected[akka] override def throughput = 0
  protected[akka] override def throughputDeadlineTime = Duration.Zero
  protected[akka] override def registerForExecution(
      mbox: Mailbox,
      hasMessageHint: Boolean,
      hasSystemMessageHint: Boolean): Boolean = false

  protected[akka] override def shutdownTimeout = 1 second

  protected[akka] override def register(actor: ActorCell): Unit = {
    super.register(actor)
    actor.mailbox match {
      case mbox: CallingThreadMailbox =>
        val queue = mbox.queue
        runQueue(mbox, queue)
      case x => throw ActorInitializationException("expected CallingThreadMailbox, got " + x.getClass)
    }
  }

  protected[akka] override def unregister(actor: ActorCell): Unit = {
    val mbox = actor.mailbox match {
      case m: CallingThreadMailbox => Some(m)
      case _                       => None
    }
    super.unregister(actor)
    mbox.foreach(CallingThreadDispatcherQueues(actor.system).unregisterQueues)
  }

  protected[akka] override def suspend(actor: ActorCell): Unit = {
    actor.mailbox match {
      case m: CallingThreadMailbox => { m.suspendSwitch.switchOn; m.suspend() }
      case m                       => m.systemEnqueue(actor.self, Suspend())
    }
  }

  protected[akka] override def resume(actor: ActorCell): Unit = {
    actor.mailbox match {
      case mbox: CallingThreadMailbox =>
        val queue = mbox.queue
        val switched = mbox.suspendSwitch.switchOff {
          CallingThreadDispatcherQueues(actor.system).gatherFromAllOtherQueues(mbox, queue)
          mbox.resume()
        }
        if (switched)
          runQueue(mbox, queue)
      case m => m.systemEnqueue(actor.self, Resume(causedByFailure = null))
    }
  }

  protected[akka] override def systemDispatch(receiver: ActorCell, message: SystemMessage): Unit = {
    receiver.mailbox match {
      case mbox: CallingThreadMailbox =>
        mbox.systemEnqueue(receiver.self, message)
        runQueue(mbox, mbox.queue)
      case m => m.systemEnqueue(receiver.self, message)
    }
  }

  protected[akka] override def dispatch(receiver: ActorCell, handle: Envelope): Unit = {
    receiver.mailbox match {
      case mbox: CallingThreadMailbox =>
        val queue = mbox.queue
        val execute = mbox.suspendSwitch.fold {
          queue.enqueue(receiver.self, handle)
          false
        } {
          queue.enqueue(receiver.self, handle)
          true
        }
        if (execute) runQueue(mbox, queue)
      case m => m.enqueue(receiver.self, handle)
    }
  }

  protected[akka] override def executeTask(invocation: TaskInvocation): Unit = { invocation.run() }

  /*
   * This method must be called with this thread's queue.
   *
   * If the catch block is executed, then a non-empty mailbox may be stalled as
   * there is no-one who cares to execute it before the next message is sent or
   * it is suspendSwitch and resumed.
   */
  @tailrec
  private def runQueue(
      mbox: CallingThreadMailbox,
      queue: MessageQueue,
      interruptedEx: InterruptedException = null): Unit = {
    def checkThreadInterruption(intEx: InterruptedException): InterruptedException = {
      if (Thread.interrupted()) { // clear interrupted flag before we continue, exception will be thrown later
        val ie = new InterruptedException("Interrupted during message processing")
        log.error(ie, "Interrupted during message processing")
        ie
      } else intEx
    }

    def throwInterruptionIfExistsOrSet(intEx: InterruptedException): Unit = {
      val ie = checkThreadInterruption(intEx)
      if (ie ne null) {
        Thread.interrupted() // clear interrupted flag before throwing according to java convention
        throw ie
      }
    }

    @tailrec
    def process(intEx: InterruptedException): InterruptedException = {
      var intex = intEx
      val recurse = {
        mbox.processAllSystemMessages()
        val handle = mbox.suspendSwitch.fold[Envelope](null) {
          if (mbox.isClosed) null else queue.dequeue()
        }
        if (handle ne null) {
          try {
            if (Mailbox.debug) println("" + mbox.actor.self + " processing message " + handle)
            mbox.actor.invoke(handle)
            intex = checkThreadInterruption(intex)
            true
          } catch {
            case ie: InterruptedException =>
              log.error(ie, "Interrupted during message processing")
              Thread.interrupted() // clear interrupted flag before we continue, exception will be thrown later
              intex = ie
              true
            case NonFatal(e) =>
              log.error(e, "Error during message processing")
              false
          }
        } else false
      }
      if (recurse) process(intex)
      else intex
    }

    // if we own the lock then we shouldn't do anything since we are processing
    // this actors mailbox at some other level on our call stack
    if (!mbox.ctdLock.isHeldByCurrentThread) {
      var intex = interruptedEx
      val gotLock = try {
        mbox.ctdLock.tryLock(50, TimeUnit.MILLISECONDS)
      } catch {
        case ie: InterruptedException =>
          Thread.interrupted() // clear interrupted flag before we continue, exception will be thrown later
          intex = ie
          false
      }
      if (gotLock) {
        val ie = try {
          process(intex)
        } finally {
          mbox.ctdLock.unlock
        }
        throwInterruptionIfExistsOrSet(ie)
      } else {
        // if we didn't get the lock and our mailbox still has messages, then we need to try again
        if (mbox.hasSystemMessages || mbox.hasMessages) {
          runQueue(mbox, queue, intex)
        } else {
          throwInterruptionIfExistsOrSet(intex)
        }
      }
    }
  }
}

class CallingThreadDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
    extends MessageDispatcherConfigurator(config, prerequisites) {

  private val instance = new CallingThreadDispatcher(this)

  override def dispatcher(): MessageDispatcher = instance
}

class CallingThreadMailbox(_receiver: akka.actor.Cell, val mailboxType: MailboxType)
    extends Mailbox(null)
    with DefaultSystemMessageQueue {

  val system = _receiver.system
  val self = _receiver.self

  private val q = new ThreadLocal[MessageQueue]() {
    override def initialValue = {
      val queue = mailboxType.create(Some(self), Some(system))
      CallingThreadDispatcherQueues(system).registerQueue(CallingThreadMailbox.this, queue)
      queue
    }
  }

  /**
   * This is only a marker to be put in the messageQueue’s stead to make error
   * messages pertaining to violated mailbox type requirements less cryptic.
   */
  override val messageQueue: MessageQueue = q.get

  override def enqueue(receiver: ActorRef, msg: Envelope): Unit = q.get.enqueue(receiver, msg)
  override def dequeue(): Envelope =
    throw new UnsupportedOperationException("CallingThreadMailbox cannot dequeue normally")
  override def hasMessages: Boolean = q.get.hasMessages
  override def numberOfMessages: Int = 0

  def queue = q.get

  val ctdLock = new ReentrantLock
  val suspendSwitch = new Switch

  override def cleanUp(): Unit = {
    /*
     * This is called from dispatcher.unregister, i.e. under this.lock. If
     * another thread obtained a reference to this mailbox and enqueues after
     * the gather operation, tough luck: no guaranteed delivery to deadLetters.
     */
    suspendSwitch.locked {
      val qq = queue
      CallingThreadDispatcherQueues(actor.system).gatherFromAllOtherQueues(this, qq)
      super.cleanUp()
      qq.cleanUp(actor.self, actor.dispatcher.mailboxes.deadLetterMailbox.messageQueue)
      q.remove()
    }
  }
}
