/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.atomic.AtomicReference

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

import akka.{ Done, NotUsed }
import akka.stream.stage._

/**
 * Creates shared or single kill switches which can be used to control completion of graphs from the outside.
 *  - The factory ``shared()`` returns a [[SharedKillSwitch]] which provides a [[Graph]] of [[FlowShape]] that can be
 *    used in arbitrary number of graphs and materializations. The switch simultaneously
 *    controls completion in all of those graphs.
 *  - The factory ``single()`` returns a [[Graph]] of [[FlowShape]] that materializes to a [[UniqueKillSwitch]]
 *    which is always unique
 *    to that materialized Flow itself.
 *
 * Creates a [[SharedKillSwitch]] that can be used to externally control the completion of various streams.
 *
 */
object KillSwitches {

  /**
   * Creates a new [[SharedKillSwitch]] with the given name that can be used to control the completion of multiple
   * streams from the outside simultaneously.
   *
   * @see SharedKillSwitch
   */
  def shared(name: String): SharedKillSwitch = new SharedKillSwitch(name)

  /**
   * Creates a new [[Graph]] of [[FlowShape]] that materializes to an external switch that allows external completion
   * of that unique materialization. Different materializations result in different, independent switches.
   *
   * For a Bidi version see [[KillSwitch#singleBidi]]
   */
  def single[T]: Graph[FlowShape[T, T], UniqueKillSwitch] =
    UniqueKillSwitchStage.asInstanceOf[Graph[FlowShape[T, T], UniqueKillSwitch]]

  /**
   * Creates a new [[Graph]] of [[FlowShape]] that materializes to an external switch that allows external completion
   * of that unique materialization. Different materializations result in different, independent switches.
   *
   * For a Flow version see [[KillSwitch#single]]
   */
  def singleBidi[T1, T2]: Graph[BidiShape[T1, T1, T2, T2], UniqueKillSwitch] =
    UniqueBidiKillSwitchStage.asInstanceOf[Graph[BidiShape[T1, T1, T2, T2], UniqueKillSwitch]]

  abstract class KillableGraphStageLogic(val terminationSignal: Future[Done], _shape: Shape)
      extends GraphStageLogic(_shape) {
    override def preStart(): Unit = {
      terminationSignal.value match {
        case Some(status) => onSwitch(status)
        case _            =>
          // callback.invoke is a simple actor send, so it is fine to run on the invoking thread
          terminationSignal.onComplete(getAsyncCallback[Try[Done]](onSwitch).invoke)(ExecutionContext.parasitic)
      }
    }

    private def onSwitch(mode: Try[Done]): Unit = mode match {
      case Success(_)  => completeStage()
      case Failure(ex) => failStage(ex)
    }
  }

  private[stream] object UniqueKillSwitchStage
      extends GraphStageWithMaterializedValue[FlowShape[Any, Any], UniqueKillSwitch] {
    override val initialAttributes = Attributes.name("breaker")
    override val shape = FlowShape(Inlet[Any]("KillSwitch.in"), Outlet[Any]("KillSwitch.out"))
    override def toString: String = "UniqueKillSwitchFlow"

    override def createLogicAndMaterializedValue(attr: Attributes) = {
      val promise = Promise[Done]()
      val switch = new UniqueKillSwitch(promise)

      val logic = new KillableGraphStageLogic(promise.future, shape) with InHandler with OutHandler {
        override def onPush(): Unit = push(shape.out, grab(shape.in))
        override def onPull(): Unit = pull(shape.in)
        setHandlers(shape.in, shape.out, this)
      }

      (logic, switch)
    }
  }

  private[stream] object UniqueBidiKillSwitchStage
      extends GraphStageWithMaterializedValue[BidiShape[Any, Any, Any, Any], UniqueKillSwitch] {

    override val initialAttributes = Attributes.name("breaker")
    override val shape = BidiShape(
      Inlet[Any]("KillSwitchBidi.in1"),
      Outlet[Any]("KillSwitchBidi.out1"),
      Inlet[Any]("KillSwitchBidi.in2"),
      Outlet[Any]("KillSwitchBidi.out2"))
    override def toString: String = "UniqueKillSwitchBidi"

    override def createLogicAndMaterializedValue(attr: Attributes) = {
      val promise = Promise[Done]()
      val switch = new UniqueKillSwitch(promise)

      val logic = new KillableGraphStageLogic(promise.future, shape) {

        setHandlers(shape.in1, shape.out1, new InHandler with OutHandler {
          override def onPush(): Unit = push(shape.out1, grab(shape.in1))
          override def onUpstreamFinish(): Unit = complete(shape.out1)
          override def onUpstreamFailure(ex: Throwable): Unit = fail(shape.out1, ex)
          override def onPull(): Unit = pull(shape.in1)
          override def onDownstreamFinish(cause: Throwable): Unit = cancel(shape.in1, cause)
        })

        setHandlers(shape.in2, shape.out2, new InHandler with OutHandler {
          override def onPush(): Unit = push(shape.out2, grab(shape.in2))
          override def onUpstreamFinish(): Unit = complete(shape.out2)
          override def onUpstreamFailure(ex: Throwable): Unit = fail(shape.out2, ex)
          override def onPull(): Unit = pull(shape.in2)
          override def onDownstreamFinish(cause: Throwable): Unit = cancel(shape.in2, cause)
        })

      }

      (logic, switch)
    }
  }

}

/**
 * A [[KillSwitch]] allows completion of [[Graph]]s from the outside by completing [[Graph]]s of [[FlowShape]] linked
 * to the switch. Depending on whether the [[KillSwitch]] is a [[UniqueKillSwitch]] or a [[SharedKillSwitch]] one or
 * multiple streams might be linked with the switch. For details see the documentation of the concrete subclasses of
 * this interface.
 */
//#kill-switch
trait KillSwitch {

  /**
   * After calling [[KillSwitch#shutdown]] the linked [[Graph]]s of [[FlowShape]] are completed normally.
   */
  def shutdown(): Unit

  /**
   * After calling [[KillSwitch#abort]] the linked [[Graph]]s of [[FlowShape]] are failed.
   */
  def abort(ex: Throwable): Unit
}
//#kill-switch

private[stream] final class TerminationSignal {
  final class Listener private[TerminationSignal] {
    private[TerminationSignal] val promise = Promise[Done]()
    def future: Future[Done] = promise.future
    def unregister(): Unit = removeListener(this)
  }

  private[this] val _listeners = TrieMap.empty[Listener, NotUsed]
  private[this] val _completedWith: AtomicReference[Option[Try[Done]]] = new AtomicReference(None)

  def tryComplete(result: Try[Done]): Unit = {
    if (_completedWith.compareAndSet(None, Some(result))) {
      for ((listener, _) <- _listeners) listener.promise.tryComplete(result)
    }
  }

  def createListener(): Listener = {
    val listener = new Listener
    if (_completedWith.get.isEmpty) {
      _listeners += (listener -> NotUsed)
    }
    _completedWith.get match {
      case Some(result) => listener.promise.tryComplete(result)
      case None         => // Ignore.
    }
    listener
  }

  private def removeListener(listener: Listener): Unit = {
    _listeners -= listener
  }
}

/**
 * A [[UniqueKillSwitch]] is always a result of a materialization (unlike [[SharedKillSwitch]] which is constructed
 * before any materialization) and it always controls that graph and operator which yielded the materialized value.
 *
 * After calling [[UniqueKillSwitch#shutdown]] the running instance of the [[Graph]] of [[FlowShape]] that materialized to the
 * [[UniqueKillSwitch]] will complete its downstream and cancel its upstream (unless if finished or failed already in which
 * case the command is ignored). Subsequent invocations of completion commands will be ignored.
 *
 * After calling [[UniqueKillSwitch#abort]] the running instance of the [[Graph]] of [[FlowShape]] that materialized to the
 * [[UniqueKillSwitch]] will fail its downstream with the provided exception and cancel its upstream
 * (unless if finished or failed already in which case the command is ignored). Subsequent invocations of
 * completion commands will be ignored.
 *
 * It is also possible to individually cancel, complete or fail upstream and downstream parts by calling the corresponding
 * methods.
 */
final class UniqueKillSwitch private[stream] (private val promise: Promise[Done]) extends KillSwitch {

  /**
   * After calling [[UniqueKillSwitch#shutdown]] the running instance of the [[Graph]] of [[FlowShape]] that materialized to the
   * [[UniqueKillSwitch]] will complete its downstream and cancel its upstream (unless if finished or failed already in which
   * case the command is ignored). Subsequent invocations of completion commands will be ignored.
   */
  def shutdown(): Unit = promise.trySuccess(Done)

  /**
   * After calling [[UniqueKillSwitch#abort]] the running instance of the [[Graph]] of [[FlowShape]] that materialized to the
   * [[UniqueKillSwitch]] will fail its downstream with the provided exception and cancel its upstream
   * (unless if finished or failed already in which case the command is ignored). Subsequent invocations of
   * completion commands will be ignored.
   */
  def abort(ex: Throwable): Unit = promise.tryFailure(ex)

  override def toString: String = s"SingleKillSwitch($hashCode)"
}

/**
 * A [[SharedKillSwitch]] is a provider for [[Graph]]s of [[FlowShape]] that can be completed or failed from the outside.
 * A [[Graph]] returned by the switch can be materialized arbitrary amount of times: every newly materialized [[Graph]]
 * belongs to the switch from which it was acquired. Multiple [[SharedKillSwitch]] instances are isolated from each other,
 * shutting down or aborting on instance does not affect the [[Graph]]s provided by another instance.
 *
 * After calling [[SharedKillSwitch#shutdown]] all materialized, running instances of all [[Graph]]s provided by the
 * [[SharedKillSwitch]] will complete their downstreams and cancel their upstreams (unless if finished or failed already in which
 * case the command is ignored). Subsequent invocations of [[SharedKillSwitch#shutdown]] and [[SharedKillSwitch#abort]] will be
 * ignored.
 *
 * After calling [[SharedKillSwitch#abort]] all materialized, running instances of all [[Graph]]s provided by the
 * [[SharedKillSwitch]] will fail their downstreams with the provided exception and cancel their upstreams
 * (unless it finished or failed already in which case the command is ignored). Subsequent invocations of
 * [[SharedKillSwitch#shutdown]] and [[SharedKillSwitch#abort]] will be ignored.
 *
 * The [[Graph]]s provided by the [[SharedKillSwitch]] do not modify the passed through elements in any way or affect
 * backpressure in the stream. All provided [[Graph]]s provide the parent [[SharedKillSwitch]] as materialized value.
 *
 * This class is thread-safe, the instance can be passed safely among threads and its methods may be invoked concurrently.
 */
final class SharedKillSwitch private[stream] (val name: String) extends KillSwitch {
  private[this] val terminationSignal = new TerminationSignal
  private[this] val _flow: Graph[FlowShape[Any, Any], SharedKillSwitch] = new SharedKillSwitchFlow

  /**
   * After calling [[SharedKillSwitch#shutdown]] all materialized, running instances of all [[Graph]]s provided by the
   * [[SharedKillSwitch]] will complete their downstreams and cancel their upstreams (unless if finished or failed already in which
   * case the command is ignored). Subsequent invocations of [[SharedKillSwitch#shutdown]] and [[SharedKillSwitch#abort]] will be
   * ignored.
   */
  def shutdown(): Unit = terminationSignal.tryComplete(Success(Done))

  /**
   * After calling [[SharedKillSwitch#abort]] all materialized, running instances of all [[Graph]]s provided by the
   * [[SharedKillSwitch]] will fail their downstreams with the provided exception and cancel their upstreams
   * (unless it finished or failed already in which case the command is ignored). Subsequent invocations of
   * [[SharedKillSwitch#shutdown]] and [[SharedKillSwitch#abort]] will be ignored.
   *
   * These provided [[Graph]]s materialize to their owning switch. This might make certain integrations simpler than
   * passing around the switch instance itself.
   *
   * @param reason The exception to be used for failing the linked [[Graph]]s
   */
  def abort(reason: Throwable): Unit = terminationSignal.tryComplete(Failure(reason))

  /**
   * Returns a typed Flow of a requested type that will be linked to this [[SharedKillSwitch]] instance. By invoking
   * [[SharedKillSwitch#shutdown]] or [[SharedKillSwitch#abort]] all running instances of all provided [[Graph]]s by this
   * switch will be stopped normally or failed.
   *
   * @tparam T Type of the elements the Flow will forward
   * @return   A reusable [[Graph]] that is linked with the switch. The materialized value provided is this switch itself.
   */
  def flow[T]: Graph[FlowShape[T, T], SharedKillSwitch] = _flow.asInstanceOf[Graph[FlowShape[T, T], SharedKillSwitch]]

  override def toString: String = s"KillSwitch($name)"

  private class SharedKillSwitchFlow extends GraphStageWithMaterializedValue[FlowShape[Any, Any], SharedKillSwitch] {
    override val shape: FlowShape[Any, Any] = FlowShape(Inlet[Any]("KillSwitch.in"), Outlet[Any]("KillSwitch.out"))

    override def toString: String = s"SharedKillSwitchFlow(switch: $name)"

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes): (GraphStageLogic, SharedKillSwitch) = {
      val shutdownListener = terminationSignal.createListener()
      val logic = new KillSwitches.KillableGraphStageLogic(shutdownListener.future, shape)
        with InHandler
        with OutHandler {
        setHandler(shape.in, this)
        setHandler(shape.out, this)

        override def onPush(): Unit = push(shape.out, grab(shape.in))
        override def onPull(): Unit = pull(shape.in)

        override def postStop(): Unit = {
          shutdownListener.unregister()
          super.postStop()
        }
      }

      (logic, SharedKillSwitch.this)
    }

  }
}
