/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.util.concurrent._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor._
import akka.actor.DeadLetter
import akka.actor.IllegalActorStateException
import akka.actor.Terminated
import akka.annotation.InternalApi
import akka.util.{ BoxedType, Timeout }

object TestActor {
  type Ignore = Option[PartialFunction[Any, Boolean]]

  abstract class AutoPilot {
    def run(sender: ActorRef, msg: Any): AutoPilot
    def noAutoPilot: AutoPilot = NoAutoPilot
    def keepRunning: AutoPilot = KeepRunning
  }

  case object NoAutoPilot extends AutoPilot {
    def run(sender: ActorRef, msg: Any): AutoPilot = this
  }

  case object KeepRunning extends AutoPilot {
    def run(sender: ActorRef, msg: Any): AutoPilot = sys.error("must not call")
  }

  final case class SetIgnore(i: Ignore) extends NoSerializationVerificationNeeded
  final case class Watch(ref: ActorRef) extends NoSerializationVerificationNeeded
  final case class UnWatch(ref: ActorRef) extends NoSerializationVerificationNeeded
  final case class SetAutoPilot(ap: AutoPilot) extends NoSerializationVerificationNeeded
  final case class Spawn(props: Props, name: Option[String] = None, strategy: Option[SupervisorStrategy] = None)
      extends NoSerializationVerificationNeeded {
    def apply(context: ActorRefFactory): ActorRef = name match {
      case Some(n) => context.actorOf(props, n)
      case None    => context.actorOf(props)
    }
  }

  trait Message {
    def msg: AnyRef
    def sender: ActorRef
  }
  final case class RealMessage(msg: AnyRef, sender: ActorRef) extends Message
  case object NullMessage extends Message {
    override def msg: AnyRef = throw IllegalActorStateException("last receive did not dequeue a message")
    override def sender: ActorRef = throw IllegalActorStateException("last receive did not dequeue a message")
  }

  val FALSE = (_: Any) => false

  /** INTERNAL API */
  private[TestActor] class DelegatingSupervisorStrategy extends SupervisorStrategy {
    import SupervisorStrategy._

    private var delegates = Map.empty[ActorRef, SupervisorStrategy]

    private def delegate(child: ActorRef) = delegates.get(child).getOrElse(stoppingStrategy)

    def update(child: ActorRef, supervisor: SupervisorStrategy): Unit = delegates += (child -> supervisor)

    override def decider = defaultDecider // not actually invoked

    override def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = {
      delegates -= child
    }

    override def processFailure(
        context: ActorContext,
        restart: Boolean,
        child: ActorRef,
        cause: Throwable,
        stats: ChildRestartStats,
        children: Iterable[ChildRestartStats]): Unit = {
      delegate(child).processFailure(context, restart, child, cause, stats, children)
    }

    override def handleFailure(
        context: ActorContext,
        child: ActorRef,
        cause: Throwable,
        stats: ChildRestartStats,
        children: Iterable[ChildRestartStats]): Boolean = {
      delegate(child).handleFailure(context, child, cause, stats, children)
    }
  }

  // make creator serializable, for VerifySerializabilitySpec
  def props(queue: BlockingDeque[Message]): Props = Props(new TestActor(queue))
}

class TestActor(queue: BlockingDeque[TestActor.Message]) extends Actor {
  import TestActor._

  override val supervisorStrategy: DelegatingSupervisorStrategy = new DelegatingSupervisorStrategy

  var ignore: Ignore = None

  var autopilot: AutoPilot = NoAutoPilot

  def receive = {
    case SetIgnore(ign)      => ignore = ign
    case Watch(ref)          => context.watch(ref)
    case UnWatch(ref)        => context.unwatch(ref)
    case SetAutoPilot(pilot) => autopilot = pilot
    case spawn: Spawn =>
      val actor = spawn(context)
      for (s <- spawn.strategy) supervisorStrategy(actor) = s
      queue.offerLast(RealMessage(actor, self))
    case x: AnyRef =>
      autopilot = autopilot.run(sender(), x) match {
        case KeepRunning => autopilot
        case other       => other
      }
      val observe = ignore.map(ignoreFunc => !ignoreFunc.applyOrElse(x, FALSE)).getOrElse(true)
      if (observe) queue.offerLast(RealMessage(x, sender()))
  }

  override def postStop() = {
    import scala.jdk.CollectionConverters._
    queue.asScala.foreach { m =>
      context.system.deadLetters.tell(DeadLetter(m.msg, m.sender, self), m.sender)
    }
  }
}

/**
 * Implementation trait behind the [[akka.testkit.TestKit]] class: you may use
 * this if inheriting from a concrete class is not possible.
 *
 * This trait requires the concrete class mixing it in to provide an
 * [[akka.actor.ActorSystem]] which is available before this traits’s
 * constructor is run. The recommended way is this:
 *
 * {{{
 * class MyTest extends TestKitBase {
 *   implicit lazy val system = ActorSystem() // may add arguments here
 *   ...
 * }
 * }}}
 */
trait TestKitBase {

  import TestActor.{ Message, NullMessage, RealMessage, Spawn }

  implicit def system: ActorSystem
  def testKitSettings = TestKitExtension(system)

  private val queue = new LinkedBlockingDeque[Message]()
  private[akka] var lastMessage: Message = NullMessage

  def lastSender = lastMessage.sender

  /**
   * Defines the testActor name.
   */
  protected def testActorName: String = "testActor"

  /**
   * ActorRef of the test actor. Access is provided to enable e.g.
   * registration as message target.
   */
  lazy val testActor: ActorRef = {
    val impl = system.asInstanceOf[ExtendedActorSystem]
    val ref = impl.systemActorOf(
      TestActor.props(queue).withDispatcher(CallingThreadDispatcher.Id),
      "%s-%d".format(testActorName, TestKit.testActorId.incrementAndGet))
    awaitCond(ref match {
      case r: RepointableRef => r.isStarted
      case _                 => true
    }, 3.seconds.dilated, 10.millis)
    ref
  }

  private var end: Duration = Duration.Undefined

  /**
   * if last assertion was expectNoMsg, disable timing failure upon within()
   * block end.
   */
  private var lastWasNoMsg = false

  /**
   * Ignore all messages in the test actor for which the given partial
   * function returns true.
   */
  def ignoreMsg(f: PartialFunction[Any, Boolean]): Unit = { testActor ! TestActor.SetIgnore(Some(f)) }

  /**
   * Stop ignoring messages in the test actor.
   */
  def ignoreNoMsg(): Unit = { testActor ! TestActor.SetIgnore(None) }

  /**
   * Have the testActor watch someone (i.e. `context.watch(...)`).
   */
  def watch(ref: ActorRef): ActorRef = {
    testActor ! TestActor.Watch(ref)
    ref
  }

  /**
   * Have the testActor stop watching someone (i.e. `context.unwatch(...)`).
   */
  def unwatch(ref: ActorRef): ActorRef = {
    testActor ! TestActor.UnWatch(ref)
    ref
  }

  /**
   * Install an AutoPilot to drive the testActor: the AutoPilot will be run
   * for each received message and can be used to send or forward messages,
   * etc. Each invocation must return the AutoPilot for the next round.
   */
  def setAutoPilot(pilot: TestActor.AutoPilot): Unit = testActor ! TestActor.SetAutoPilot(pilot)

  /**
   * Obtain current time (`System.nanoTime`) as Duration.
   */
  def now: FiniteDuration = System.nanoTime.nanos

  /**
   * Obtain time remaining for execution of the innermost enclosing `within`
   * block or missing that it returns the properly dilated default for this
   * case from settings (key "akka.test.single-expect-default").
   */
  def remainingOrDefault = remainingOr(testKitSettings.SingleExpectDefaultTimeout.dilated)

  /**
   * Obtain time remaining for execution of the innermost enclosing `within`
   * block or throw an [[AssertionError]] if no `within` block surrounds this
   * call.
   */
  def remaining: FiniteDuration = end match {
    case f: FiniteDuration => f - now
    case _                 => throw new AssertionError("`remaining` may not be called outside of `within`")
  }

  /**
   * Obtain time remaining for execution of the innermost enclosing `within`
   * block or missing that it returns the given duration.
   */
  def remainingOr(duration: FiniteDuration): FiniteDuration = end match {
    case x if x eq Duration.Undefined => duration
    case x if !x.isFinite             => throw new IllegalArgumentException("`end` cannot be infinite")
    case f: FiniteDuration            => f - now
    case _                            => throw new IllegalArgumentException("`end` cannot be infinite")
  }

  private def remainingOrDilated(max: Duration): FiniteDuration = max match {
    case x if x eq Duration.Undefined => remainingOrDefault
    case x if !x.isFinite             => throw new IllegalArgumentException("max duration cannot be infinite")
    case f: FiniteDuration            => f.dilated
    case _                            => throw new IllegalArgumentException("max duration cannot be infinite")
  }

  /**
   * Query queue status.
   */
  def msgAvailable = !queue.isEmpty

  /**
   * Await until the given condition evaluates to `true` or the timeout
   * expires, whichever comes first.
   *
   * If no timeout is given, take it from the innermost enclosing `within`
   * block.
   *
   * Note that the timeout is scaled using Duration.dilated,
   * which uses the configuration entry "akka.test.timefactor".
   */
  def awaitCond(
      p: => Boolean,
      max: Duration = Duration.Undefined,
      interval: Duration = 100.millis,
      message: String = ""): Unit = {
    val _max = remainingOrDilated(max)
    val stop = now + _max

    @tailrec
    def poll(t: Duration): Unit = {
      if (!p) {
        assert(now < stop, s"timeout ${_max} expired: $message")
        Thread.sleep(t.toMillis)
        poll((stop - now) min interval)
      }
    }

    poll(_max min interval)
  }

  /**
   * Evaluate the given assert every `interval` until it does not throw an exception and return the
   * result.
   *
   * If the `max` timeout expires the last exception is thrown.
   *
   * If no timeout is given, take it from the innermost enclosing `within`
   * block.
   *
   * Note that the timeout is scaled using Duration.dilated,
   * which uses the configuration entry "akka.test.timefactor".
   */
  def awaitAssert[A](a: => A, max: Duration = Duration.Undefined, interval: Duration = 100.millis): A = {
    val _max = remainingOrDilated(max)
    val stop = now + _max

    @tailrec
    def poll(t: Duration): A = {
      // cannot use null-ness of result as signal it failed
      // because Java API and not wanting to return a value will be "return null"
      var failed = false
      val result: A =
        try {
          val aRes = a
          failed = false
          aRes
        } catch {
          case NonFatal(e) =>
            failed = true
            if ((now + t) >= stop) throw e
            else null.asInstanceOf[A]
        }

      if (!failed) result
      else {
        Thread.sleep(t.toMillis)
        poll((stop - now) min interval)
      }
    }

    poll(_max min interval)
  }

  /**
   * Evaluate the given assert every `interval` until exception is thrown or `max` timeout is expired.
   *
   * Returns the result of last evaluation of the assertion.
   *
   * If no timeout is given, take it from the innermost enclosing `within`
   * block.
   *
   * Note that the timeout is scaled using Duration.dilated,
   * which uses the configuration entry "akka.test.timefactor".
   */
  def assertForDuration[A](a: => A, max: FiniteDuration, interval: Duration = 100.millis): A = {
    val _max = remainingOrDilated(max)
    val stop = now + _max

    @tailrec
    def poll(t: Duration): A = {
      // cannot use null-ness of result as signal it failed
      // because Java API and not wanting to return a value will be "return null"
      val instantNow = now
      val result =
        try {
          a
        } catch {
          case e: Throwable => throw e
        }

      if (instantNow < stop) {
        Thread.sleep(t.toMillis)
        poll((stop - now) min interval)
      } else {
        result
      }
    }

    poll(max min interval)
  }

  /**
   * Execute code block while bounding its execution time between `min` and
   * `max`. `within` blocks may be nested. All methods in this trait which
   * take maximum wait times are available in a version which implicitly uses
   * the remaining time governed by the innermost enclosing `within` block.
   *
   * Note that the timeout is scaled using Duration.dilated, which uses the
   * configuration entry "akka.test.timefactor", while the min Duration is not.
   *
   * {{{
   * val ret = within(50 millis) {
   *   test ! "ping"
   *   expectMsgClass(classOf[String])
   * }
   * }}}
   */
  def within[T](min: FiniteDuration, max: FiniteDuration)(f: => T): T = {
    val _max = max.dilated
    val start = now
    val rem = if (end == Duration.Undefined) Duration.Inf else end - start
    assert(rem >= min, s"required min time $min not possible, only ${format(min.unit, rem)} left")

    lastWasNoMsg = false

    val max_diff = _max min rem
    val prev_end = end
    end = start + max_diff

    val ret = try f
    finally end = prev_end

    val diff = now - start
    assert(min <= diff, s"block took ${format(min.unit, diff)}, should at least have been $min")
    if (!lastWasNoMsg) {
      assert(diff <= max_diff, s"block took ${format(_max.unit, diff)}, exceeding ${format(_max.unit, max_diff)}")
    }

    ret
  }

  /**
   * Same as calling `within(0 seconds, max)(f)`.
   */
  def within[T](max: FiniteDuration)(f: => T): T = within(0 seconds, max)(f)

  /**
   * Same as `expectMsg(remainingOrDefault, obj)`, but correctly treating the timeFactor.
   */
  def expectMsg[T](obj: T): T = expectMsg_internal(remainingOrDefault, obj)

  /**
   * Receive one message from the test actor and assert that it equals the
   * given object. Wait time is bounded by the given duration, with an
   * AssertionFailure being thrown in case of timeout.
   *
   * @return the received object
   */
  def expectMsg[T](max: FiniteDuration, obj: T): T = expectMsg_internal(max.dilated, obj)

  /**
   * Receive one message from the test actor and assert that it equals the
   * given object. Wait time is bounded by the given duration, with an
   * AssertionFailure being thrown in case of timeout.
   *
   * @return the received object
   */
  def expectMsg[T](max: FiniteDuration, hint: String, obj: T): T = expectMsg_internal(max.dilated, obj, Some(hint))

  private def expectMsg_internal[T](max: Duration, obj: T, hint: Option[String] = None): T = {
    val o = receiveOne(max)
    val hintOrEmptyString = hint.map(": " + _).getOrElse("")
    assert(o ne null, s"timeout ($max) during expectMsg while waiting for $obj" + hintOrEmptyString)
    assert(obj == o, s"expected $obj, found $o" + hintOrEmptyString)
    o.asInstanceOf[T]
  }

  /**
   * Receive one message from the test actor and assert that the given
   * partial function accepts it. Wait time is bounded by the given duration,
   * with an AssertionFailure being thrown in case of timeout.
   *
   * Use this variant to implement more complicated or conditional
   * processing.
   *
   * @return the received object as transformed by the partial function
   */
  def expectMsgPF[T](max: Duration = Duration.Undefined, hint: String = "")(f: PartialFunction[Any, T]): T = {
    val _max = remainingOrDilated(max)
    val o = receiveOne(_max)
    assert(o ne null, s"timeout (${_max}) during expectMsg: $hint")
    assert(f.isDefinedAt(o), s"expected: $hint but got unexpected message $o")
    f(o)
  }

  /**
   * Receive one message from the test actor and assert that it is the Terminated message of the given ActorRef.
   * Before calling this method, you have to `watch` the target actor ref.
   * Wait time is bounded by the given duration, with an AssertionFailure being thrown in case of timeout.
   *
   * @param target the actor ref expected to be Terminated
   * @param max wait no more than max time, otherwise throw AssertionFailure
   * @return the received Terminated message
   */
  def expectTerminated(target: ActorRef, max: Duration = Duration.Undefined): Terminated =
    expectMsgPF(max, "Terminated " + target) {
      case t @ Terminated(`target`) => t
    }

  /**
   * Hybrid of expectMsgPF and receiveWhile: receive messages while the
   * partial function matches and returns false. Use it to ignore certain
   * messages while waiting for a specific message.
   *
   * @return the last received message, i.e. the first one for which the
   *         partial function returned true
   */
  def fishForMessage(max: Duration = Duration.Undefined, hint: String = "")(f: PartialFunction[Any, Boolean]): Any = {
    val _max = remainingOrDilated(max)
    val end = now + _max
    @tailrec
    def recv: Any = {
      val o = receiveOne(end - now)
      assert(o ne null, s"timeout (${_max}) during fishForMessage, hint: $hint")
      assert(f.isDefinedAt(o), s"fishForMessage($hint) found unexpected message $o")
      if (f(o)) o else recv
    }
    recv
  }

  /**
   * Waits for specific message that partial function matches while ignoring all other messages coming in the meantime.
   * Use it to ignore any number of messages while waiting for a specific one.
   *
   * @return result of applying partial function to the last received message,
   *         i.e. the first one for which the partial function is defined
   */
  def fishForSpecificMessage[T](max: Duration = Duration.Undefined, hint: String = "")(
      f: PartialFunction[Any, T]): T = {
    val _max = remainingOrDilated(max)
    val end = now + _max
    @tailrec
    def recv: T = {
      val o = receiveOne(end - now)
      assert(o ne null, s"timeout (${_max}) during fishForSpecificMessage, hint: $hint")
      if (f.isDefinedAt(o)) f(o) else recv
    }
    recv
  }

  /**
   * Same as `expectMsgType[T](remainingOrDefault)`, but correctly treating the timeFactor.
   */
  def expectMsgType[T](implicit t: ClassTag[T]): T =
    expectMsgClass_internal(remainingOrDefault, t.runtimeClass.asInstanceOf[Class[T]])

  /**
   * Receive one message from the test actor and assert that it conforms to the
   * given type (after erasure). Wait time is bounded by the given duration,
   * with an AssertionFailure being thrown in case of timeout.
   *
   * @return the received object
   */
  def expectMsgType[T](max: FiniteDuration)(implicit t: ClassTag[T]): T =
    expectMsgClass_internal(max.dilated, t.runtimeClass.asInstanceOf[Class[T]])

  /**
   * Same as `expectMsgClass(remainingOrDefault, c)`, but correctly treating the timeFactor.
   */
  def expectMsgClass[C](c: Class[C]): C = expectMsgClass_internal(remainingOrDefault, c)

  /**
   * Receive one message from the test actor and assert that it conforms to
   * the given class. Wait time is bounded by the given duration, with an
   * AssertionFailure being thrown in case of timeout.
   *
   * @return the received object
   */
  def expectMsgClass[C](max: FiniteDuration, c: Class[C]): C = expectMsgClass_internal(max.dilated, c)

  private def expectMsgClass_internal[C](max: FiniteDuration, c: Class[C]): C = {
    val o = receiveOne(max)
    assert(o ne null, s"timeout ($max) during expectMsgClass waiting for $c")
    assert(BoxedType(c).isInstance(o), s"expected $c, found ${o.getClass} ($o)")
    o.asInstanceOf[C]
  }

  /**
   * Same as `expectMsgAnyOf(remainingOrDefault, obj...)`, but correctly treating the timeFactor.
   */
  def expectMsgAnyOf[T](obj: T*): T = expectMsgAnyOf_internal(remainingOrDefault, obj: _*)

  /**
   * Receive one message from the test actor and assert that it equals one of
   * the given objects. Wait time is bounded by the given duration, with an
   * AssertionFailure being thrown in case of timeout.
   *
   * @return the received object
   */
  def expectMsgAnyOf[T](max: FiniteDuration, obj: T*): T = expectMsgAnyOf_internal(max.dilated, obj: _*)

  private def expectMsgAnyOf_internal[T](max: FiniteDuration, obj: T*): T = {
    val o = receiveOne(max)
    assert(o ne null, s"timeout ($max) during expectMsgAnyOf waiting for ${obj.mkString("(", ", ", ")")}")
    assert(obj.exists(_ == o), s"found unexpected $o")
    o.asInstanceOf[T]
  }

  /**
   * Same as `expectMsgAnyClassOf(remainingOrDefault, obj...)`, but correctly treating the timeFactor.
   */
  def expectMsgAnyClassOf[C](obj: Class[_ <: C]*): C = expectMsgAnyClassOf_internal(remainingOrDefault, obj: _*)

  /**
   * Receive one message from the test actor and assert that it conforms to
   * one of the given classes. Wait time is bounded by the given duration,
   * with an AssertionFailure being thrown in case of timeout.
   *
   * @return the received object
   */
  def expectMsgAnyClassOf[C](max: FiniteDuration, obj: Class[_ <: C]*): C =
    expectMsgAnyClassOf_internal(max.dilated, obj: _*)

  private def expectMsgAnyClassOf_internal[C](max: FiniteDuration, obj: Class[_ <: C]*): C = {
    val o = receiveOne(max)
    assert(o ne null, s"timeout ($max) during expectMsgAnyClassOf waiting for ${obj.mkString("(", ", ", ")")}")
    assert(obj.exists(c => BoxedType(c).isInstance(o)), s"found unexpected $o")
    o.asInstanceOf[C]
  }

  /**
   * Same as `expectMsgAllOf(remainingOrDefault, obj...)`, but correctly treating the timeFactor.
   */
  def expectMsgAllOf[T](obj: T*): immutable.Seq[T] = expectMsgAllOf_internal(remainingOrDefault, obj: _*)

  /**
   * Receive a number of messages from the test actor matching the given
   * number of objects and assert that for each given object one is received
   * which equals it and vice versa. This construct is useful when the order in
   * which the objects are received is not fixed. Wait time is bounded by the
   * given duration, with an AssertionFailure being thrown in case of timeout.
   *
   * <pre>
   *   dispatcher ! SomeWork1()
   *   dispatcher ! SomeWork2()
   *   expectMsgAllOf(1 second, Result1(), Result2())
   * </pre>
   */
  def expectMsgAllOf[T](max: FiniteDuration, obj: T*): immutable.Seq[T] = expectMsgAllOf_internal(max.dilated, obj: _*)

  private def checkMissingAndUnexpected(
      missing: Seq[Any],
      unexpected: Seq[Any],
      missingMessage: String,
      unexpectedMessage: String): Unit = {
    assert(
      missing.isEmpty && unexpected.isEmpty,
      (if (missing.isEmpty) "" else missing.mkString(missingMessage + " [", ", ", "] ")) +
      (if (unexpected.isEmpty) "" else unexpected.mkString(unexpectedMessage + " [", ", ", "]")))
  }

  private def expectMsgAllOf_internal[T](max: FiniteDuration, obj: T*): immutable.Seq[T] = {
    val recv = receiveN_internal(obj.size, max)
    val missing = obj.filterNot(x => recv.exists(x == _))
    val unexpected = recv.filterNot(x => obj.exists(x == _))
    checkMissingAndUnexpected(missing, unexpected, "not found", "found unexpected")
    recv.asInstanceOf[immutable.Seq[T]]
  }

  /**
   * Same as `expectMsgAllClassOf(remainingOrDefault, obj...)`, but correctly treating the timeFactor.
   */
  def expectMsgAllClassOf[T](obj: Class[_ <: T]*): immutable.Seq[T] =
    internalExpectMsgAllClassOf(remainingOrDefault, obj: _*)

  /**
   * Receive a number of messages from the test actor matching the given
   * number of classes and assert that for each given class one is received
   * which is of that class (equality, not conformance). This construct is
   * useful when the order in which the objects are received is not fixed.
   * Wait time is bounded by the given duration, with an AssertionFailure
   * being thrown in case of timeout.
   */
  def expectMsgAllClassOf[T](max: FiniteDuration, obj: Class[_ <: T]*): immutable.Seq[T] =
    internalExpectMsgAllClassOf(max.dilated, obj: _*)

  private def internalExpectMsgAllClassOf[T](max: FiniteDuration, obj: Class[_ <: T]*): immutable.Seq[T] = {
    val recv = receiveN_internal(obj.size, max)
    val missing = obj.filterNot(x => recv.exists(_.getClass eq BoxedType(x)))
    val unexpected = recv.filterNot(x => obj.exists(c => BoxedType(c) eq x.getClass))
    checkMissingAndUnexpected(missing, unexpected, "not found", "found non-matching object(s)")
    recv.asInstanceOf[immutable.Seq[T]]
  }

  /**
   * Same as `expectMsgAllConformingOf(remainingOrDefault, obj...)`, but correctly treating the timeFactor.
   */
  def expectMsgAllConformingOf[T](obj: Class[_ <: T]*): immutable.Seq[T] =
    internalExpectMsgAllConformingOf(remainingOrDefault, obj: _*)

  /**
   * Receive a number of messages from the test actor matching the given
   * number of classes and assert that for each given class one is received
   * which conforms to that class (and vice versa). This construct is useful
   * when the order in which the objects are received is not fixed.  Wait time
   * is bounded by the given duration, with an AssertionFailure being thrown in
   * case of timeout.
   *
   * Beware that one object may satisfy all given class constraints, which
   * may be counter-intuitive.
   */
  def expectMsgAllConformingOf[T](max: FiniteDuration, obj: Class[_ <: T]*): immutable.Seq[T] =
    internalExpectMsgAllConformingOf(max.dilated, obj: _*)

  private def internalExpectMsgAllConformingOf[T](max: FiniteDuration, obj: Class[_ <: T]*): immutable.Seq[T] = {
    val recv = receiveN_internal(obj.size, max)
    val missing = obj.filterNot(x => recv.exists(BoxedType(x).isInstance(_)))
    val unexpected = recv.filterNot(x => obj.exists(c => BoxedType(c).isInstance(x)))
    checkMissingAndUnexpected(missing, unexpected, "not found", "found non-matching object(s)")
    recv.asInstanceOf[immutable.Seq[T]]
  }

  /**
   * Assert that no message is received for the specified time.
   * Supplied value is not dilated.
   */
  def expectNoMessage(max: FiniteDuration) = {
    expectNoMsg_internal(max)
  }

  /**
   * Assert that no message is received. Waits for the default period configured as
   * `akka.test.expect-no-message-default`.
   * That timeout is scaled using the configuration entry "akka.test.timefactor".
   */
  def expectNoMessage(): Unit =
    expectNoMsg_internal(testKitSettings.ExpectNoMessageDefaultTimeout.dilated)

  private def expectNoMsg_internal(max: FiniteDuration): Unit = {
    val finish = System.nanoTime() + max.toNanos
    val pollInterval = 100.millis

    def leftNow = (finish - System.nanoTime()).nanos

    var elem: AnyRef = queue.peekFirst()
    var left = leftNow
    while (left.toNanos > 0 && elem == null) {
      //Use of (left / 2) gives geometric series limited by finish time similar to (1/2)^n limited by 1,
      //so it is very precise
      Thread.sleep(pollInterval.toMillis min (left / 2).toMillis)
      left = leftNow
      if (left.toNanos > 0) {
        elem = queue.peekFirst()
      }
    }

    if (elem ne null) {
      // we pop the message, such that subsequent expectNoMessage calls can
      // assert on the next period without a message
      queue.pop()

      val diff = (max.toNanos - left.toNanos).nanos
      val m = s"assertion failed: received unexpected message $elem after ${diff.toMillis} millis"
      throw new java.lang.AssertionError(m)
    } else {
      lastWasNoMsg = true
    }
  }

  /**
   * Receive a series of messages until one does not match the given partial
   * function or the idle timeout is met (disabled by default) or the overall
   * maximum duration is elapsed or expected messages count is reached.
   * Returns the sequence of messages.
   *
   * Note that it is not an error to hit the `max` duration in this case.
   *
   * One possible use of this method is for testing whether messages of
   * certain characteristics are generated at a certain rate:
   *
   * {{{
   * test ! ScheduleTicks(100 millis)
   * val series = receiveWhile(750 millis) {
   *     case Tick(count) => count
   * }
   * assert(series == (1 to 7).toList)
   * }}}
   */
  def receiveWhile[T](max: Duration = Duration.Undefined, idle: Duration = Duration.Inf, messages: Int = Int.MaxValue)(
      f: PartialFunction[AnyRef, T]): immutable.Seq[T] = {
    val stop = now + remainingOrDilated(max)
    var msg: Message = NullMessage

    @tailrec
    def doit(acc: List[T], count: Int): List[T] = {
      if (count >= messages) acc.reverse
      else {
        receiveOne((stop - now) min idle)
        lastMessage match {
          case NullMessage =>
            lastMessage = msg
            acc.reverse
          case RealMessage(o, _) if f.isDefinedAt(o) =>
            msg = lastMessage
            doit(f(o) :: acc, count + 1)
          case RealMessage(_, _) =>
            queue.offerFirst(lastMessage)
            lastMessage = msg
            acc.reverse
          case unexpected => throw new RuntimeException(s"Unexpected: $unexpected") // exhaustiveness check
        }
      }
    }

    val ret = doit(Nil, 0)
    lastWasNoMsg = true
    ret
  }

  /**
   * Same as `receiveN(n, remaining)` but correctly taking into account
   * Duration.timeFactor.
   */
  def receiveN(n: Int): immutable.Seq[AnyRef] = receiveN_internal(n, remainingOrDefault)

  /**
   * Receive N messages in a row before the given deadline.
   */
  def receiveN(n: Int, max: FiniteDuration): immutable.Seq[AnyRef] = receiveN_internal(n, max.dilated)

  private def receiveN_internal(n: Int, max: Duration): immutable.Seq[AnyRef] = {
    val stop = max + now
    for { x <- 1 to n } yield {
      val timeout = stop - now
      val o = receiveOne(timeout)
      assert(o ne null, s"timeout ($max) while expecting $n messages (got ${x - 1})")
      o
    }
  }

  /**
   * Receive one message from the internal queue of the TestActor. If the given
   * duration is zero, the queue is polled (non-blocking).
   *
   * This method does NOT automatically scale its Duration parameter!
   */
  def receiveOne(max: Duration): AnyRef = {
    val message =
      if (max == 0.seconds) {
        queue.pollFirst
      } else if (max.isFinite) {
        queue.pollFirst(max.length, max.unit)
      } else {
        queue.takeFirst
      }
    lastWasNoMsg = false
    message match {
      case null =>
        lastMessage = NullMessage
        null
      case RealMessage(msg, _) =>
        lastMessage = message
        msg
      case unexpected => throw new RuntimeException(s"Unexpected: $unexpected") // exhaustiveness check
    }
  }

  /**
   * Shut down an actor system and wait for termination.
   * On failure debug output will be logged about the remaining actors in the system.
   *
   * If verifySystemShutdown is true, then an exception will be thrown on failure.
   */
  def shutdown(
      actorSystem: ActorSystem = system,
      duration: Duration = Duration.Undefined,
      verifySystemShutdown: Boolean = false): Unit = {
    TestKit.shutdownActorSystem(actorSystem, duration, verifySystemShutdown)
  }

  /**
   * Spawns an actor as a child of this test actor, and returns the child's ActorRef.
   * @param props Props to create the child actor
   * @param name Actor name for the child actor
   * @param supervisorStrategy Strategy should decide what to do with failures in the actor.
   */
  def childActorOf(props: Props, name: String, supervisorStrategy: SupervisorStrategy): ActorRef = {
    testActor ! Spawn(props, Some(name), Some(supervisorStrategy))
    expectMsgType[ActorRef]
  }

  /**
   * Spawns an actor as a child of this test actor with an auto-generated name, and returns the child's ActorRef.
   * @param props Props to create the child actor
   * @param supervisorStrategy Strategy should decide what to do with failures in the actor.
   */
  def childActorOf(props: Props, supervisorStrategy: SupervisorStrategy): ActorRef = {
    testActor ! Spawn(props, None, Some(supervisorStrategy))
    expectMsgType[ActorRef]
  }

  /**
   * Spawns an actor as a child of this test actor with a stopping supervisor strategy, and returns the child's ActorRef.
   * @param props Props to create the child actor
   * @param name Actor name for the child actor
   */
  def childActorOf(props: Props, name: String): ActorRef = {
    testActor ! Spawn(props, Some(name), None)
    expectMsgType[ActorRef]
  }

  /**
   * Spawns an actor as a child of this test actor with an auto-generated name and stopping supervisor strategy, returning the child's ActorRef.
   * @param props Props to create the child actor
   */
  def childActorOf(props: Props): ActorRef = {
    testActor ! Spawn(props, None, None)
    expectMsgType[ActorRef]
  }

  private def format(u: TimeUnit, d: Duration) = "%.3f %s".format(d.toUnit(u), u.toString.toLowerCase)
}

/**
 * Test kit for testing actors. Inheriting from this class enables reception of
 * replies from actors, which are queued by an internal actor and can be
 * examined using the `expectMsg...` methods. Assertions and bounds concerning
 * timing are available in the form of `within` blocks.
 *
 * {{{
 * class Test extends TestKit(ActorSystem()) {
 *   try {
 *
 *     val test = system.actorOf(Props[SomeActor])
 *
 *       within (1.second) {
 *         test ! SomeWork
 *         expectMsg(Result1) // bounded to 1 second
 *         expectMsg(Result2) // bounded to the remainder of the 1 second
 *       }
 *
 *     } finally {
 *       system.terminate()
 *     }
 *
 *   } finally {
 *     system.terminate()
 *   }
 * }
 * }}}
 *
 * Beware of two points:
 *
 *  - the ActorSystem passed into the constructor needs to be shutdown,
 *    otherwise thread pools and memory will be leaked
 *  - this class is not thread-safe (only one actor with one queue, one stack
 *    of `within` blocks); it is expected that the code is executed from a
 *    constructor as shown above, which makes this a non-issue, otherwise take
 *    care not to run tests within a single test class instance in parallel.
 *
 * It should be noted that for CI servers and the like all maximum Durations
 * are scaled using their Duration.dilated method, which uses the
 * TestKitExtension.Settings.TestTimeFactor settable via akka.conf entry "akka.test.timefactor".
 *
 * @since 1.1
 */
class TestKit(_system: ActorSystem) extends TestKitBase {
  implicit val system: ActorSystem = _system
}

object TestKit {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] val testActorId = new AtomicInteger(0)

  /**
   * Await until the given condition evaluates to `true` or the timeout
   * expires, whichever comes first.
   */
  def awaitCond(p: => Boolean, max: Duration, interval: Duration = 100.millis, noThrow: Boolean = false): Boolean = {
    val stop = now + max

    @tailrec
    def poll(): Boolean = {
      if (!p) {
        val toSleep = stop - now
        if (toSleep <= Duration.Zero) {
          if (noThrow) false
          else throw new AssertionError(s"timeout $max expired")
        } else {
          Thread.sleep((toSleep min interval).toMillis)
          poll()
        }
      } else true
    }

    poll()
  }

  /**
   * Obtain current timestamp as Duration for relative measurements (using System.nanoTime).
   */
  def now: Duration = System.nanoTime().nanos

  /**
   * Shut down an actor system and wait for termination.
   * On failure debug output will be logged about the remaining actors in the system.
   *
   * The `duration` is dilated by the timefactor.
   *
   * If verifySystemShutdown is true, then an exception will be thrown on failure.
   */
  def shutdownActorSystem(
      actorSystem: ActorSystem,
      duration: Duration = Duration.Undefined,
      verifySystemShutdown: Boolean = false): Unit = {
    val d = duration match {
      case f: FiniteDuration => f.dilated(actorSystem)
      case _                 => 10.seconds.dilated(actorSystem).min(10.seconds)
    }
    actorSystem.terminate()
    try Await.ready(actorSystem.whenTerminated, d)
    catch {
      case _: TimeoutException =>
        val msg = "Failed to stop [%s] within [%s] \n%s".format(
          actorSystem.name,
          d,
          actorSystem.asInstanceOf[ActorSystemImpl].printTree)
        if (verifySystemShutdown) throw new RuntimeException(msg)
        else println(msg)
    }
  }
}

/**
 * TestKit-based probe which allows sending, reception and reply.
 */
class TestProbe(_application: ActorSystem, name: String) extends TestKit(_application) {

  def this(_application: ActorSystem) = this(_application, "testProbe")

  /**
   * Shorthand to get the testActor.
   */
  def ref = testActor

  protected override def testActorName = name

  /**
   * Send message to an actor while using the probe's TestActor as the sender.
   * Replies will be available for inspection with all of TestKit's assertion
   * methods.
   */
  def send(actor: ActorRef, msg: Any): Unit = actor.!(msg)(testActor)

  /**
   * Forward this message as if in the TestActor's receive method with self.forward.
   */
  def forward(actor: ActorRef, msg: Any = lastMessage.msg): Unit = actor.!(msg)(lastMessage.sender)

  /**
   * Get sender of last received message.
   */
  def sender() = lastMessage.sender

  /**
   * Send message to the sender of the last dequeued message.
   */
  def reply(msg: Any): Unit = sender().!(msg)(ref)

}

object TestProbe {
  def apply()(implicit system: ActorSystem) = new TestProbe(system)
  def apply(name: String)(implicit system: ActorSystem) = new TestProbe(system, name)
}

trait ImplicitSender { this: TestKitBase =>
  implicit def self: ActorRef = testActor
}

trait DefaultTimeout { this: TestKitBase =>
  implicit val timeout: Timeout = testKitSettings.DefaultTimeout
}
