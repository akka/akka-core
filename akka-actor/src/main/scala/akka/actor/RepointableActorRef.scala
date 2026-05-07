/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.{ LinkedList => JLinkedList }
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.control.NonFatal
import akka.actor.dungeon.ChildrenContainer
import akka.annotation.InternalApi
import akka.dispatch._
import akka.dispatch.sysmsg._
import akka.event.Logging.Warning

/**
 * This actor ref starts out with some dummy cell (by default just enqueuing
 * messages into vectors protected by ReentrantLock), it must be initialize()’d
 * before it can be sent to, and it will be activate()’d by its supervisor in
 * response to the Supervise() message, which will replace the contained Cell
 * with a fully functional one, transfer all messages from dummy to real queue
 * and swap out the cell ref.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] class RepointableActorRef(
    val system: ActorSystemImpl,
    val props: Props,
    val dispatcher: MessageDispatcher,
    val mailboxType: MailboxType,
    val supervisor: InternalActorRef,
    val path: ActorPath)
    extends ActorRefWithCell
    with RepointableRef {

  import AbstractActorRef.{ cellHandle, lookupHandle }

  /*
   * H E R E   B E   D R A G O N S !
   *
   * There are two main functions of a Cell: message queueing and child lookup.
   * When switching out the UnstartedCell for its real replacement, the former
   * must be switched after all messages have been drained from the temporary
   * queue into the real mailbox, while the latter must be switched before
   * processing the very first message (i.e. before Cell.start()). Hence there
   * are two refs here, one for each function, and they are switched just so.
   */
  @nowarn @volatile private var _cellDoNotCallMeDirectly: Cell = _
  @nowarn @volatile private var _lookupDoNotCallMeDirectly: Cell = _
  @nowarn private def _preventPrivateUnusedErasure = {
    _cellDoNotCallMeDirectly
    _lookupDoNotCallMeDirectly
  }

  def underlying: Cell = cellHandle.getVolatile(this).asInstanceOf[Cell]
  def lookup = lookupHandle.getVolatile(this).asInstanceOf[Cell]

  @tailrec final def swapCell(next: Cell): Cell = {
    val old = underlying
    if (cellHandle.compareAndSet(this, old, next)) old else swapCell(next)
  }

  @tailrec final def swapLookup(next: Cell): Cell = {
    val old = lookup
    if (lookupHandle.compareAndSet(this, old, next)) old else swapLookup(next)
  }

  /**
   * Initialize: make a dummy cell which holds just a mailbox, then tell our
   * supervisor that we exist so that he can create the real Cell in
   * handleSupervise().
   *
   * Call twice on your own peril!
   *
   * This is protected so that others can have different initialization.
   */
  def initialize(async: Boolean): this.type =
    underlying match {
      case null =>
        swapCell(new UnstartedCell(system, this, props, supervisor))
        swapLookup(underlying)
        supervisor.sendSystemMessage(Supervise(this, async))
        if (!async) point(false)
        this
      case _ => throw new IllegalStateException("initialize called more than once!")
    }

  /**
   * This method is supposed to be called by the supervisor in handleSupervise()
   * to replace the UnstartedCell with the real one. It assumes no concurrent
   * modification of the `underlying` field, though it is safe to send messages
   * at any time.
   */
  def point(catchFailures: Boolean): this.type =
    underlying match {
      case u: UnstartedCell =>
        val cell =
          try newCell(u)
          catch {
            case NonFatal(ex) if catchFailures =>
              val safeDispatcher = system.dispatchers.defaultGlobalDispatcher
              new ActorCell(system, this, props, safeDispatcher, supervisor).initWithFailure(ex)
          }
        /*
         * The problem here was that if the real actor (which will start running
         * at cell.start()) creates children in its constructor, then this may
         * happen before the swapCell in u.replaceWith, meaning that those
         * children cannot be looked up immediately, e.g. if they shall become
         * routees.
         */
        swapLookup(cell)
        cell.start()
        u.replaceWith(cell)
        this
      case null => throw new IllegalStateException("underlying cell is null")
      case _    => this // this happens routinely for things which were created async=false
    }

  /**
   * This is called by activate() to obtain the cell which is to replace the
   * unstarted cell. The cell must be fully functional.
   */
  def newCell(@nowarn("msg=never used") old: UnstartedCell): Cell =
    new ActorCell(system, this, props, dispatcher, supervisor).init(sendSupervise = false, mailboxType)

  def start(): Unit = ()

  def suspend(): Unit = underlying.suspend()

  def resume(causedByFailure: Throwable): Unit = underlying.resume(causedByFailure)

  def stop(): Unit = underlying.stop()

  def restart(cause: Throwable): Unit = underlying.restart(cause)

  def isStarted: Boolean = underlying match {
    case _: UnstartedCell => false
    case null             => throw new IllegalStateException("isStarted called before initialized")
    case _                => true
  }

  @deprecated("Use context.watch(actor) and receive Terminated(actor)", "2.2") def isTerminated: Boolean =
    underlying.isTerminated

  def provider: ActorRefProvider = system.provider

  def isLocal: Boolean = underlying.isLocal

  def getParent: InternalActorRef = underlying.parent

  def getChild(name: Iterator[String]): InternalActorRef =
    if (name.hasNext) {
      name.next() match {
        case ".." => getParent.getChild(name)
        case ""   => getChild(name)
        case other =>
          val (childName, uid) = ActorCell.splitNameAndUid(other)
          lookup.getChildByName(childName) match {
            case Some(crs: ChildRestartStats) if uid == ActorCell.undefinedUid || uid == crs.uid =>
              crs.child.asInstanceOf[InternalActorRef].getChild(name)
            case _ =>
              lookup match {
                case ac: ActorCell => ac.getFunctionRefOrNobody(childName, uid)
                case _             => Nobody
              }
          }
      }
    } else this

  /**
   * Method for looking up a single child beneath this actor.
   * It is racy if called from the outside.
   */
  def getSingleChild(name: String): InternalActorRef = lookup.getSingleChild(name)

  def children: immutable.Iterable[ActorRef] = lookup.childrenRefs.children

  def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit = underlying.sendMessage(message, sender)

  def sendSystemMessage(message: SystemMessage) = underlying.sendSystemMessage(message)

  @throws(classOf[java.io.ObjectStreamException])
  protected def writeReplace(): AnyRef = SerializedActorRef(this)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class UnstartedCell(
    val systemImpl: ActorSystemImpl,
    val self: RepointableActorRef,
    val props: Props,
    val supervisor: InternalActorRef)
    extends Cell {

  /*
   * This lock protects all accesses to this cell’s queues. It also ensures
   * safe switching to the started ActorCell.
   */
  private[this] final val lock = new ReentrantLock

  // use Envelope to keep on-send checks in the same place ACCESS MUST BE PROTECTED BY THE LOCK
  private[this] final val queue = new JLinkedList[Envelope]()

  // ACCESS MUST BE PROTECTED BY THE LOCK
  private[this] var sysmsgQueue: LatestFirstSystemMessageList = SystemMessageList.LNil

  import systemImpl.settings.UnstartedPushTimeout.{ duration => timeout }

  def replaceWith(cell: Cell): Unit = locked {
    try {
      def drainSysmsgQueue(): Unit = {
        // using while in case a sys msg enqueues another sys msg
        while (sysmsgQueue.nonEmpty) {
          var sysQ = sysmsgQueue.reverse
          sysmsgQueue = SystemMessageList.LNil
          while (sysQ.nonEmpty) {
            val msg = sysQ.head
            sysQ = sysQ.tail
            msg.unlink()
            cell.sendSystemMessage(msg)
          }
        }
      }

      drainSysmsgQueue()

      while (!queue.isEmpty) {
        cell.sendMessage(queue.poll())
        // drain sysmsgQueue in case a msg enqueues a sys msg
        drainSysmsgQueue()
      }
    } finally {
      self.swapCell(cell)
    }
  }

  def system: ActorSystem = systemImpl
  def start(): this.type = this
  def suspend(): Unit = sendSystemMessage(Suspend())
  def resume(causedByFailure: Throwable): Unit = sendSystemMessage(Resume(causedByFailure))
  def restart(cause: Throwable): Unit = sendSystemMessage(Recreate(cause))
  def stop(): Unit = sendSystemMessage(Terminate())
  override private[akka] def isTerminated: Boolean = locked {
    val cell = self.underlying
    if (cellIsReady(cell)) cell.isTerminated else false
  }
  def parent: InternalActorRef = supervisor
  def childrenRefs: ChildrenContainer = ChildrenContainer.EmptyChildrenContainer
  def getChildByName(name: String): Option[ChildRestartStats] = None
  override def getSingleChild(name: String): InternalActorRef = Nobody

  def sendMessage(msg: Envelope): Unit = {
    if (lock.tryLock(timeout.length, timeout.unit)) {
      try {
        val cell = self.underlying
        if (cellIsReady(cell)) {
          cell.sendMessage(msg)
        } else if (!queue.offer(msg)) {
          system.eventStream.publish(
            Warning(
              self.path.toString,
              getClass,
              "dropping message of type " + msg.message.getClass + " due to enqueue failure"))
          system.deadLetters.tell(DeadLetter(msg.message, msg.sender, self), msg.sender)
        } else if (Mailbox.debug) println(s"$self temp queueing ${msg.message} from ${msg.sender}")
      } finally lock.unlock()
    } else {
      system.eventStream.publish(
        Warning(
          self.path.toString,
          getClass,
          "dropping message of type" + msg.message.getClass + " due to lock timeout"))
      system.deadLetters.tell(DeadLetter(msg.message, msg.sender, self), msg.sender)
    }
  }

  def sendSystemMessage(msg: SystemMessage): Unit = {
    lock.lock() // we cannot lose system messages, ever, and we cannot throw an Error from here as well
    try {
      val cell = self.underlying
      if (cellIsReady(cell))
        cell.sendSystemMessage(msg)
      else {
        sysmsgQueue ::= msg
        if (Mailbox.debug) println(s"$self temp queueing system $msg")
      }
    } finally lock.unlock()
  }

  def isLocal = true

  private[this] final def cellIsReady(cell: Cell): Boolean = (cell ne this) && (cell ne null)

  def hasMessages: Boolean = locked {
    val cell = self.underlying
    if (cellIsReady(cell)) cell.hasMessages else !queue.isEmpty
  }

  def numberOfMessages: Int = locked {
    val cell = self.underlying
    if (cellIsReady(cell)) cell.numberOfMessages else queue.size
  }

  private[this] final def locked[T](body: => T): T = {
    lock.lock()
    try body
    finally lock.unlock()
  }

}
