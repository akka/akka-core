/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.lang.System.identityHashCode
import java.lang.ref.WeakReference
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NoStackTrace

import SupervisorStrategy.{ Directive, Restart, Resume, Stop }
import com.typesafe.config.{ Config, ConfigFactory }
import language.postfixOps

import akka.actor.SupervisorStrategy.seqThrowable2Decider
import akka.dispatch.{ Dispatcher, DispatcherConfigurator, DispatcherPrerequisites, MessageDispatcher }
import akka.event.Logging
import akka.pattern.ask
import akka.testkit.{ AkkaSpec, DefaultTimeout, EventFilter, ImplicitSender }
import akka.testkit.{ filterEvents, filterException, TestDuration, TestLatch }
import akka.testkit.LongRunningTest
import akka.testkit.TestEvent.Mute
import akka.util.Helpers.ConfigOps

object SupervisorHierarchySpec {

  class FireWorkerException(msg: String) extends Exception(msg)

  /**
   * For testing Supervisor behavior, normally you don't supply the strategy
   * from the outside like this.
   */
  class CountDownActor(countDown: CountDownLatch, override val supervisorStrategy: SupervisorStrategy) extends Actor {

    def receive = {
      case p: Props => sender() ! context.actorOf(p)
    }
    // test relies on keeping children around during restart
    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {}
    override def postRestart(reason: Throwable) = {
      countDown.countDown()
    }
  }

  class Resumer extends Actor {
    override def supervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Resume }
    def receive = {
      case "spawn" => sender() ! context.actorOf(Props[Resumer]())
      case "fail"  => throw new Exception("expected")
      case "ping"  => sender() ! "pong"
    }
  }

  final case class Ready(ref: ActorRef)
  final case class Died(path: ActorPath)
  case object Abort
  case object PingOfDeath
  case object PongOfDeath
  final case class Event(msg: Any, identity: Long) { val time: Long = System.nanoTime }
  final case class ErrorLog(msg: String, log: Vector[Event])
  final case class Failure(
      directive: Directive,
      stop: Boolean,
      depth: Int,
      var failPre: Int,
      var failPost: Int,
      val failConstr: Int,
      stopKids: Int)
      extends RuntimeException("Failure")
      with NoStackTrace {
    override def toString = productPrefix + productIterator.mkString("(", ",", ")")
  }
  final case class Dump(level: Int)

  val config = ConfigFactory.parseString("""
    hierarchy {
      type = "akka.actor.SupervisorHierarchySpec$MyDispatcherConfigurator"
    }
    akka.loglevel = INFO
    akka.actor.debug.fsm = on
  """)

  class MyDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
      extends DispatcherConfigurator(config, prerequisites) {

    private val instance: MessageDispatcher =
      new Dispatcher(
        this,
        config.getString("id"),
        config.getInt("throughput"),
        config.getNanosDuration("throughput-deadline-time"),
        configureExecutor(),
        config.getMillisDuration("shutdown-timeout")) {

        override def suspend(cell: ActorCell): Unit = {
          cell.actor match {
            case h: Hierarchy => h.log :+= Event("suspended " + cell.mailbox.suspendCount, identityHashCode(cell.actor))
            case _            =>
          }
          super.suspend(cell)
        }

        override def resume(cell: ActorCell): Unit = {
          super.resume(cell)
          cell.actor match {
            case h: Hierarchy => h.log :+= Event("resumed " + cell.mailbox.suspendCount, identityHashCode(cell.actor))
            case _            =>
          }
        }

      }

    override def dispatcher(): MessageDispatcher = instance
  }

  /*
   * This stores structural data of the hierarchy which would otherwise be lost
   * upon Restart or would have to be managed by the highest supervisor (which
   * is undesirable).
   */
  final case class HierarchyState(log: Vector[Event], kids: Map[ActorPath, Int], failConstr: Failure)
  val stateCache = new ConcurrentHashMap[ActorPath, HierarchyState]()
  @volatile var ignoreFailConstr = false

  class Hierarchy(size: Int, breadth: Int, listener: ActorRef, myLevel: Int, random: Random) extends Actor {

    var log = Vector.empty[Event]

    stateCache.get(self.path) match {
      case hs @ HierarchyState(l: Vector[Event], _, f: Failure) if f.failConstr > 0 && !ignoreFailConstr =>
        val log = l :+ Event("Failed in constructor", identityHashCode(this))
        stateCache.put(self.path, hs.copy(log = log, failConstr = f.copy(failConstr = f.failConstr - 1)))
        throw f
      case _ =>
    }

    var failed = false
    var suspended = false

    def abort(msg: String): Unit = {
      listener ! ErrorLog(msg, log)
      log = Vector(Event("log sent", identityHashCode(this)))
      context.parent ! Abort
      context.stop(self)
    }

    def setFlags(directive: Directive): Unit = directive match {
      case Restart => failed = true
      case Resume  => suspended = true
      case _       =>
    }

    def suspendCount = context.asInstanceOf[ActorCell].mailbox.suspendCount

    override def preStart(): Unit = {
      log :+= Event("started", identityHashCode(this))
      listener ! Ready(self)
      val s = size - 1 // subtract myself
      val kidInfo: Map[ActorPath, Int] =
        if (s > 0) {
          val kids = random.nextInt(Math.min(breadth, s)) + 1
          val sizes = s / kids
          var rest = s % kids
          val propsTemplate = Props.empty.withDispatcher("hierarchy")
          (1 to kids).iterator.map { (id) =>
            val kidSize = if (rest > 0) {
              rest -= 1; sizes + 1
            } else sizes
            val props =
              Props(new Hierarchy(kidSize, breadth, listener, myLevel + 1, random)).withDeploy(propsTemplate.deploy)
            (context.watch(context.actorOf(props, id.toString)).path, kidSize)
          }.toMap
        } else Map()
      stateCache.put(self.path, HierarchyState(log, kidInfo, null))
    }

    var preRestartCalled = false
    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
      // do not scrap children
      if (preRestartCalled) abort("preRestart called twice")
      else {
        log :+= Event("preRestart " + cause, identityHashCode(this))
        preRestartCalled = true
        cause match {
          case f: Failure =>
            context.children.take(f.stopKids).foreach { child =>
              log :+= Event("killing " + child, identityHashCode(this))
              context.unwatch(child)
              context.stop(child)
            }
            stateCache.put(self.path, stateCache.get(self.path).copy(log = log))
            if (f.failPre > 0) {
              f.failPre -= 1
              throw f
            }
          case _ => stateCache.put(self.path, stateCache.get(self.path).copy(log = log))
        }
      }
    }

    val unwrap: PartialFunction[Throwable, (Throwable, Throwable)] = {
      case x @ PostRestartException(_, f: Failure, _)         => (f, x)
      case x @ ActorInitializationException(_, _, f: Failure) => (f, x)
      case x                                                  => (x, x)
    }
    override val supervisorStrategy = OneForOneStrategy()(unwrap.andThen {
      case (_: Failure, _) if pongsToGo > 0 =>
        log :+= Event("pongOfDeath resuming " + sender(), identityHashCode(this))
        Resume
      case (f: Failure, orig) =>
        if (f.depth > 0) {
          setFlags(f.directive)
          log :+= Event("escalating " + f + " from " + sender(), identityHashCode(this))
          throw f.copy(depth = f.depth - 1)
        }
        val prefix = orig match {
          case _: Failure => "applying "
          case _          => "re-applying "
        }
        log :+= Event(prefix + f + " to " + sender(), identityHashCode(this))
        if (myLevel > 3 && f.failPost == 0 && f.stop) Stop else f.directive
      case (_, x) =>
        log :+= Event("unhandled exception from " + sender() + Logging.stackTraceFor(x), identityHashCode(this))
        sender() ! Dump(0)
        context.system.scheduler.scheduleOnce(1 second, self, Dump(0))(context.dispatcher)
        Resume
    })

    override def postRestart(cause: Throwable): Unit = {
      val state = stateCache.get(self.path)
      log = state.log
      log :+= Event("restarted " + suspendCount + " " + cause, identityHashCode(this))
      state.kids.foreach {
        case (childPath, kidSize) =>
          val name = childPath.name
          if (context.child(name).isEmpty) {
            listener ! Died(childPath)
            val props =
              Props(new Hierarchy(kidSize, breadth, listener, myLevel + 1, random)).withDispatcher("hierarchy")
            context.watch(context.actorOf(props, name))
          }
      }
      if (context.children.size != state.kids.size) {
        abort("invariant violated: " + state.kids.size + " != " + context.children.size)
      }
      cause match {
        case f: Failure if f.failPost > 0                                  => { f.failPost -= 1; throw f }
        case PostRestartException(`self`, f: Failure, _) if f.failPost > 0 => { f.failPost -= 1; throw f }
        case _                                                             =>
      }
    }

    override def postStop(): Unit = {
      if (failed || suspended) {
        listener ! ErrorLog("not resumed (" + failed + ", " + suspended + ")", log)
        val state = stateCache.get(self)
        if (state ne null) stateCache.put(self.path, state.copy(log = log))
      } else {
        stateCache.put(self.path, HierarchyState(log, Map(), null))
      }
    }

    def check(msg: Any): Boolean = {
      suspended = false
      log :+= Event(msg, identityHashCode(Hierarchy.this))
      if (failed) {
        abort("processing message while failed")
        failed = false
        false
      } else if (context.asInstanceOf[ActorCell].mailbox.isSuspended) {
        abort("processing message while suspended")
        false
      } else if (!Thread.currentThread.getName.startsWith("SupervisorHierarchySpec-hierarchy")) {
        abort(
          "running on wrong thread " + Thread.currentThread + " dispatcher=" + context.props.dispatcher + "=>" +
          context.asInstanceOf[ActorCell].dispatcher.id)
        false
      } else true
    }

    var pongsToGo = 0

    def receive = new Receive {
      val handler: Receive = {
        case f: Failure =>
          setFlags(f.directive)
          stateCache.put(self.path, stateCache.get(self.path).copy(failConstr = f.copy()))
          throw f
        case "ping"          => { Thread.sleep((random.nextFloat() * 1.03).toLong); sender() ! "pong" }
        case Dump(0)         => abort("dump")
        case Dump(level)     => context.children.foreach(_ ! Dump(level - 1))
        case Terminated(ref) =>
          /*
           * It might be that we acted upon this death already in postRestart
           * (if the unwatch() came too late), so just ignore in this case.
           */
          val name = ref.path.name
          if (pongsToGo == 0) {
            if (!context.child(name).exists(_ != ref)) {
              listener ! Died(ref.path)
              val kids = stateCache.get(self.path).kids(ref.path)
              val props = Props(new Hierarchy(kids, breadth, listener, myLevel + 1, random)).withDispatcher("hierarchy")
              context.watch(context.actorOf(props, name))
            }
            // Otherwise it is a Terminated from an old child. Ignore.
          } else {
            // WARNING: The Terminated that is logged by this is logged by check() above, too. It is not
            // an indication of duplicate Terminate messages
            log :+= Event(s"${sender()} terminated while pongOfDeath", identityHashCode(Hierarchy.this))
          }
        case Abort => abort("terminating")
        case PingOfDeath =>
          if (size > 1) {
            pongsToGo = context.children.size
            log :+= Event("sending " + pongsToGo + " pingOfDeath", identityHashCode(Hierarchy.this))
            context.children.foreach(_ ! PingOfDeath)
          } else {
            context.stop(self)
            context.parent ! PongOfDeath
          }
        case PongOfDeath =>
          pongsToGo -= 1
          if (pongsToGo == 0) {
            context.stop(self)
            context.parent ! PongOfDeath
          }
      }
      override def isDefinedAt(msg: Any) = handler.isDefinedAt(msg)
      override def apply(msg: Any) = { if (check(msg)) handler(msg) }
    }
  }

  case object Work
  final case class GCcheck(kids: Vector[WeakReference[ActorRef]])

  sealed trait Action
  final case class Ping(ref: ActorRef) extends Action
  final case class Fail(ref: ActorRef, directive: Directive) extends Action

  sealed trait State
  case object Idle extends State
  case object Init extends State
  case object Stress extends State
  case object Finishing extends State
  case object LastPing extends State
  case object Stopping extends State
  case object GC extends State
  case object Failed extends State

  /*
   * This stress test will construct a supervision hierarchy of configurable
   * depth and breadth and then randomly fail and check its actors. The actors
   * perform certain checks internally (verifying that they do not run when
   * suspended, for example), and they are checked for health by the test
   * procedure.
   *
   * Execution happens in phases (which is the reason for FSM):
   *
   * Idle:
   * - upon reception of Init message, construct hierarchy and go to Init state
   *
   * Init:
   * - receive refs of all contained actors
   *
   * Stress:
   * - deal out actions (Fail or "ping"), keeping the hierarchy busy
   * - whenever all actors are in the "pinged" list (i.e. have not yet
   *   answered with a "pong"), delay processing of the next Work() by
   *   100 millis
   * - when receiving a Work() while all actors are "pinged", stop the
   *   hierarchy and go to the Stopping state
   * - make sure to remove all actors which die in the course of the test
   *   from the pinged and idle sets (others will be spawned from within the
   *   hierarchy)
   *
   * Finishing:
   * - after dealing out the last action, wait for the outstanding "pong"
   *   messages
   * - when last "pong" is received, goto LastPing state
   * - upon state timeout, stop the hierarchy and go to the Failed state
   *
   * LastPing:
   * - upon entering this state, send a "ping" to all actors
   * - when last "pong" is received, goto Stopping state
   * - upon state timeout, stop the hierarchy and go to the Failed state
   *
   * Stopping:
   * - upon entering this state, stop the hierarchy
   * - upon termination of the hierarchy send back successful result
   *
   * Whenever an ErrorLog is received, goto Failed state
   *
   * Failed:
   * - accumulate ErrorLog messages
   * - upon termination of the hierarchy send back failed result and print
   *   the logs, merged and in chronological order.
   *
   * Remark about test failures which lead to stopping:
   * The FSM needs to know not the send more things to the dead guy, but it
   * also must not watch all targets, because the dead guy’s supervisor also
   * watches him and creates a new guy in response to the Terminated; given
   * that there is no ordering relationship guaranteed by DeathWatch it could
   * happen that the new guy sends his Ready before the FSM has gotten the
   * Terminated, which would screw things up big time. Solution is to let the
   * supervisor do all, including notifying the FSM of the death of the guy.
   */

  class StressTest(testActor: ActorRef, size: Int, breadth: Int) extends Actor with LoggingFSM[State, Int] {
    import context.system

    val randomSeed = System.nanoTime()
    val random = new Random(randomSeed)

    // don’t escalate from this one!
    override val supervisorStrategy = OneForOneStrategy() {
      case f: Failure                                     => f.directive
      case OriginalRestartException(f: Failure)           => f.directive
      case ActorInitializationException(_, _, f: Failure) => f.directive
      case _                                              => Stop
    }

    var children = Vector.empty[ActorRef]
    var activeChildren = Vector.empty[ActorRef]
    var idleChildren = Vector.empty[ActorRef]
    var pingChildren = Set.empty[ActorRef]

    val nextJob = Iterator.continually(random.nextFloat() match {
      case x if x >= 0.5 =>
        // ping one child
        val pick = ((x - 0.5) * 2 * idleChildren.size).toInt
        val ref = idleChildren(pick)
        idleChildren = idleChildren.take(pick) ++ idleChildren.drop(pick + 1)
        pingChildren += ref
        Ping(ref)
      case x =>
        // fail one child
        val pick = ((if (x >= 0.25f) x - 0.25f else x) * 4 * activeChildren.size).toInt
        Fail(activeChildren(pick), if (x > 0.25) Restart else Resume)
    })

    var hierarchy: ActorRef = _

    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
      throw ActorKilledException("I want to DIE")
    }

    override def postRestart(cause: Throwable): Unit = {
      throw ActorKilledException("I said I wanted to DIE, dammit!")
    }

    override def postStop(): Unit = {
      testActor ! "stressTestStopped"
    }

    // number of Work packages to execute for the test
    startWith(Idle, size * 1000)

    when(Idle) {
      case this.Event(Init, _) =>
        hierarchy = context.watch(
          context.actorOf(Props(new Hierarchy(size, breadth, self, 0, random)).withDispatcher("hierarchy"), "head"))
        startSingleTimer("phase", StateTimeout, 5 seconds)
        goto(Init)
    }

    when(Init) {
      case this.Event(Ready(ref), _) =>
        if (children contains ref) {
          testActor ! "children not unique"
          stop()
        } else {
          children :+= ref
          if (children.size == size) goto(Stress)
          else stay()
        }
      case this.Event(StateTimeout, _) =>
        testActor ! "did not get children list"
        stop()
    }

    onTransition {
      case Init -> Stress =>
        self ! Work
        idleChildren = children
        activeChildren = children
        // set timeout for completion of the whole test (i.e. including Finishing and Stopping)
        startSingleTimer("phase", StateTimeout, 90.seconds.dilated)
    }

    val workSchedule = 50.millis

    private def random012: Int = random.nextFloat() match {
      case x if x > 0.1  => 0
      case x if x > 0.03 => 1
      case _             => 2
    }
    private def bury(path: ActorPath): Unit = {
      val deadGuy = path.elements
      val deadGuySize = deadGuy.size
      val isChild = (other: ActorRef) => other.path.elements.take(deadGuySize) == deadGuy
      activeChildren = activeChildren.filterNot(isChild)
      idleChildren = idleChildren.filterNot(isChild)
      pingChildren = pingChildren.filterNot(isChild)
    }

    var ignoreNotResumedLogs = true

    when(Stress) {
      case this.Event(Work, _) if idleChildren.isEmpty =>
        context.system.scheduler.scheduleOnce(workSchedule, self, Work)(context.dispatcher)
        stay()
      case this.Event(Work, x) if x > 0 =>
        nextJob.next() match {
          case Ping(ref) => ref ! "ping"
          case Fail(ref, dir) =>
            val f = Failure(
              dir,
              stop = random012 > 0,
              depth = random012,
              failPre = random012,
              failPost = random012,
              failConstr = random012,
              stopKids = random012 match {
                case 0 => 0
                case 1 => random.nextInt(breadth / 2)
                case 2 => 1000
              })
            ref ! f
        }
        if (idleChildren.nonEmpty) self ! Work
        else context.system.scheduler.scheduleOnce(workSchedule, self, Work)(context.dispatcher)
        stay().using(x - 1)
      case this.Event(Work, _) => if (pingChildren.isEmpty) goto(LastPing) else goto(Finishing)
      case this.Event(Died(path), _) =>
        bury(path)
        stay()
      case this.Event("pong", _) =>
        pingChildren -= sender()
        idleChildren :+= sender()
        stay()
      case this.Event(StateTimeout, todo) =>
        log.info("dumping state due to StateTimeout")
        log.info(
          "children: " + children.size + " pinged: " + pingChildren.size + " idle: " + idleChildren.size + " work: " + todo)
        pingChildren.foreach(println)
        println(system.asInstanceOf[ActorSystemImpl].printTree)
        pingChildren.foreach(getErrorsUp)
        ignoreNotResumedLogs = false
        hierarchy ! Dump(2)
        goto(Failed)
    }

    onTransition {
      case Stress -> Finishing => ignoreFailConstr = true
    }

    when(Finishing) {
      case this.Event("pong", _) =>
        pingChildren -= sender()
        idleChildren :+= sender()
        if (pingChildren.isEmpty) goto(LastPing) else stay()
      case this.Event(Died(ref), _) =>
        bury(ref)
        if (pingChildren.isEmpty) goto(LastPing) else stay()
    }

    onTransition {
      case _ -> LastPing =>
        idleChildren.foreach(_ ! "ping")
        pingChildren ++= idleChildren
        idleChildren = Vector.empty
    }

    when(LastPing) {
      case this.Event("pong", _) =>
        pingChildren -= sender()
        idleChildren :+= sender()
        if (pingChildren.isEmpty) goto(Stopping) else stay()
      case this.Event(Died(ref), _) =>
        bury(ref)
        if (pingChildren.isEmpty) goto(Stopping) else stay()
    }

    onTransition {
      case _ -> Stopping =>
        ignoreNotResumedLogs = false
        hierarchy ! PingOfDeath
    }

    when(Stopping, stateTimeout = 5.seconds.dilated) {
      case this.Event(PongOfDeath, _) => stay()
      case this.Event(Terminated(r), _) if r == hierarchy =>
        val undead = children.filterNot(_.isTerminated)
        if (undead.nonEmpty) {
          log.info("undead:\n" + undead.mkString("\n"))
          testActor ! "stressTestFailed (" + undead.size + " undead)"
          stop()
        } else if (false) {
          /*
           * This part of the test is normally disabled, because it does not
           * work reliably: even though I found only these weak references
           * using YourKit just now, GC wouldn’t collect them and the test
           * failed. I’m leaving this code in so that manual inspection remains
           * an option (by setting the above condition to “true”).
           */
          val weak = children.map(new WeakReference(_))
          children = Vector.empty
          pingChildren = Set.empty
          idleChildren = Vector.empty
          context.system.scheduler.scheduleOnce(workSchedule, self, GCcheck(weak))(context.dispatcher)
          System.gc()
          goto(GC)
        } else {
          testActor ! "stressTestSuccessful"
          stop()
        }
      case this.Event(StateTimeout, _) =>
        errors :+= self -> ErrorLog("timeout while Stopping", Vector.empty)
        println(system.asInstanceOf[ActorSystemImpl].printTree)
        getErrors(hierarchy, 10)
        printErrors()
        idleChildren.foreach(println)
        testActor ! "timeout in Stopping"
        stop()
      case this.Event(e: ErrorLog, _) =>
        errors :+= sender() -> e
        goto(Failed)
    }

    when(GC, stateTimeout = 10 seconds) {
      case this.Event(GCcheck(weak), _) =>
        val next = weak.filter(_.get ne null)
        if (next.nonEmpty) {
          context.system.scheduler.scheduleOnce(workSchedule, self, GCcheck(next))(context.dispatcher)
          System.gc()
          stay()
        } else {
          testActor ! "stressTestSuccessful"
          stop()
        }
      case this.Event(StateTimeout, _) =>
        testActor ! "timeout in GC"
        stop()
    }

    var errors = Vector.empty[(ActorRef, ErrorLog)]

    when(Failed, stateTimeout = 5.seconds.dilated) {
      case this.Event(e: ErrorLog, _) =>
        if (!e.msg.startsWith("not resumed") || !ignoreNotResumedLogs)
          errors :+= sender() -> e
        stay()
      case this.Event(Terminated(r), _) if r == hierarchy =>
        printErrors()
        testActor ! "stressTestFailed"
        stop()
      case this.Event(StateTimeout, _) =>
        getErrors(hierarchy, 10)
        printErrors()
        testActor ! "timeout in Failed"
        stop()
      case this.Event("pong", _)  => stay() // don’t care?
      case this.Event(Work, _)    => stay()
      case this.Event(Died(_), _) => stay()
    }

    def getErrors(target: ActorRef, depth: Int): Unit = {
      target match {
        case l: LocalActorRef =>
          l.underlying.actor match {
            case h: Hierarchy => errors :+= target -> ErrorLog("forced", h.log)
            case _            => errors :+= target -> ErrorLog("fetched", stateCache.get(target.path).log)
          }
          if (depth > 0) {
            l.underlying.children.foreach(getErrors(_, depth - 1))
          }
        case _ => throw new IllegalArgumentException()
      }
    }

    def getErrorsUp(target: ActorRef): Unit = {
      target match {
        case l: LocalActorRef =>
          l.underlying.actor match {
            case h: Hierarchy => errors :+= target -> ErrorLog("forced", h.log)
            case _            => errors :+= target -> ErrorLog("fetched", stateCache.get(target.path).log)
          }
          if (target != hierarchy) getErrorsUp(l.getParent)
        case _ => throw new IllegalArgumentException()
      }
    }

    def printErrors(): Unit = {
      errors.collect {
        case (origin, ErrorLog("dump", _))                               => getErrors(origin, 1)
        case (origin, ErrorLog(msg, _)) if msg.startsWith("not resumed") => getErrorsUp(origin)
      }
      val merged = errors.sortBy(_._1.toString).flatMap {
        case (ref, ErrorLog(msg, log)) =>
          println("Error: " + ref + " " + msg)
          log.map(l => (l.time, ref, l.identity, l.msg.toString))
      }
      println("random seed: " + randomSeed)
      merged.sorted.distinct.foreach(println)
    }

    whenUnhandled {
      case this.Event(Ready(ref), _) =>
        activeChildren :+= ref
        children :+= ref
        idleChildren :+= ref
        stay()
      case this.Event(e: ErrorLog, _) =>
        if (e.msg.startsWith("not resumed")) stay()
        else {
          errors :+= sender() -> e
          // don’t stop the hierarchy, that is going to happen all by itself and in the right order
          goto(Failed)
        }
      case this.Event(StateTimeout, _) =>
        println("pingChildren:\n" + pingChildren.view.map(_.path.toString).toSeq.sorted.mkString("\n"))
        ignoreNotResumedLogs = false
        // make sure that we get the logs of the remaining pingChildren
        pingChildren.foreach(getErrorsUp)
        // this will ensure that the error logs get printed and we stop the test
        context.stop(hierarchy)
        goto(Failed)
      case this.Event(Abort, _) =>
        log.info("received Abort")
        goto(Failed)
      case this.Event(msg, _) =>
        testActor ! ("received unexpected msg: " + msg)
        stop()
    }

    initialize()
  }

}

class SupervisorHierarchySpec extends AkkaSpec(SupervisorHierarchySpec.config) with DefaultTimeout with ImplicitSender {
  import SupervisorHierarchySpec._

  override def expectedTestDuration = 2.minutes

  "A Supervisor Hierarchy" must {

    "restart manager and workers in AllForOne" taggedAs LongRunningTest in {
      val countDown = new CountDownLatch(4)

      val boss = system.actorOf(Props(new Supervisor(OneForOneStrategy()(List(classOf[Exception])))))

      val managerProps = Props(new CountDownActor(countDown, AllForOneStrategy()(List())))
      val manager = Await.result((boss ? managerProps).mapTo[ActorRef], timeout.duration)

      val workerProps = Props(new CountDownActor(countDown, SupervisorStrategy.defaultStrategy))
      val workerOne = Await.result((manager ? workerProps).mapTo[ActorRef], timeout.duration)
      Await.result((manager ? workerProps).mapTo[ActorRef], timeout.duration) // workerTwo
      Await.result((manager ? workerProps).mapTo[ActorRef], timeout.duration) // workerThree

      filterException[ActorKilledException] {
        workerOne ! Kill

        // manager + all workers should be restarted by only killing a worker
        // manager doesn't trap exits, so boss will restart manager

        assert(countDown.await(2, TimeUnit.SECONDS))
      }
    }

    "send notification to supervisor when permanent failure" taggedAs LongRunningTest in {
      val countDownMessages = new CountDownLatch(1)
      val countDownMax = new CountDownLatch(1)
      val boss = system.actorOf(Props(new Actor {
        override val supervisorStrategy =
          OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 5 seconds)(List(classOf[Throwable]))

        val crasher = context.watch(
          context.actorOf(Props(new CountDownActor(countDownMessages, SupervisorStrategy.defaultStrategy))))

        def receive = {
          case "killCrasher" => crasher ! Kill
          case Terminated(_) => countDownMax.countDown()
        }
      }))

      filterException[ActorKilledException] {
        boss ! "killCrasher"
        boss ! "killCrasher"

        assert(countDownMessages.await(2, TimeUnit.SECONDS))
        assert(countDownMax.await(2, TimeUnit.SECONDS))
      }
    }

    "resume children after Resume" taggedAs LongRunningTest in {
      val boss = system.actorOf(Props[Resumer](), "resumer")
      boss ! "spawn"
      val middle = expectMsgType[ActorRef]
      middle ! "spawn"
      val worker = expectMsgType[ActorRef]
      worker ! "ping"
      expectMsg("pong")
      EventFilter.warning("expected", occurrences = 1).intercept {
        middle ! "fail"
      }
      middle ! "ping"
      expectMsg("pong")
      worker ! "ping"
      expectMsg("pong")
    }

    "suspend children while failing" taggedAs LongRunningTest in {
      val latch = TestLatch()
      val slowResumer = system.actorOf(Props(new Actor {
        override def supervisorStrategy = OneForOneStrategy() {
          case _ => Await.ready(latch, 4.seconds.dilated); SupervisorStrategy.Resume
        }
        def receive = {
          case "spawn" => sender() ! context.actorOf(Props[Resumer]())
        }
      }), "slowResumer")
      slowResumer ! "spawn"
      val boss = expectMsgType[ActorRef]
      boss ! "spawn"
      val middle = expectMsgType[ActorRef]
      middle ! "spawn"
      val worker = expectMsgType[ActorRef]
      worker ! "ping"
      expectMsg("pong")
      EventFilter.warning("expected", occurrences = 1).intercept {
        boss ! "fail"
        awaitCond(worker.asInstanceOf[LocalActorRef].underlying.mailbox.isSuspended)
        worker ! "ping"
        expectNoMessage(2 seconds)
        latch.countDown()
      }
      expectMsg("pong")
    }

    "handle failure in creation when supervision strategy returns Resume and Restart" taggedAs LongRunningTest in {
      val createAttempt = new AtomicInteger(0)
      val preStartCalled = new AtomicInteger(0)
      val postRestartCalled = new AtomicInteger(0)

      filterEvents(
        EventFilter[Failure](),
        EventFilter[ActorInitializationException](),
        EventFilter[IllegalArgumentException]("OH NO!"),
        EventFilter.error(start = "changing Recreate into Create"),
        EventFilter.error(start = "changing Resume into Create")) {
        val failResumer =
          system.actorOf(
            Props(new Actor {
              override def supervisorStrategy = OneForOneStrategy() {
                case _: ActorInitializationException =>
                  if (createAttempt.get % 2 == 0) SupervisorStrategy.Resume else SupervisorStrategy.Restart
              }

              val child = context.actorOf(Props(new Actor {
                val ca = createAttempt.incrementAndGet()

                if (ca <= 6 && ca % 3 == 0)
                  context.actorOf(Props(new Actor { override def receive = { case _ => } }), "workingChild")

                if (ca < 6) {
                  throw new IllegalArgumentException("OH NO!")
                }
                override def preStart() = {
                  preStartCalled.incrementAndGet()
                }
                override def postRestart(reason: Throwable) = {
                  postRestartCalled.incrementAndGet()
                }
                override def receive = {
                  case m => sender() ! m
                }
              }), "failChild")

              override def receive = {
                case m => child.forward(m)
              }
            }),
            "failResumer")

        failResumer ! "blahonga"
        expectMsg("blahonga")
      }
      createAttempt.get should ===(6)
      preStartCalled.get should ===(1)
      postRestartCalled.get should ===(0)
    }

    "survive being stressed" taggedAs LongRunningTest in {
      system.eventStream.publish(
        Mute(
          EventFilter[Failure](),
          EventFilter.warning("Failure"),
          EventFilter[ActorInitializationException](),
          EventFilter[NoSuchElementException]("head of empty list"),
          EventFilter.error(start = "changing Resume into Restart"),
          EventFilter.error(start = "changing Resume into Create"),
          EventFilter.error(start = "changing Recreate into Create"),
          EventFilter.warning(start = "received dead ")))

      val fsm = system.actorOf(Props(new StressTest(testActor, size = 500, breadth = 6)), "stressTest")

      fsm ! FSM.SubscribeTransitionCallBack(system.actorOf(Props(new Actor {
        def receive = {
          case s: FSM.CurrentState[_] => log.info("{}", s)
          case t: FSM.Transition[_]   => log.info("{}", t)
        }
      })))

      fsm ! Init

      expectMsg(110 seconds, "stressTestSuccessful")
      expectMsg("stressTestStopped")
    }
  }
}
