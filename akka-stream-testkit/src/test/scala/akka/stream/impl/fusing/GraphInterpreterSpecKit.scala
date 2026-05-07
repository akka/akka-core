/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import scala.annotation.nowarn
import scala.collection.{ Map => SMap }
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.Props
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.Supervision.Decider
import akka.stream.impl.fusing.GraphInterpreter.{
  Connection,
  DownstreamBoundaryStageLogic,
  Failed,
  UpstreamBoundaryStageLogic
}
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler, _ }
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE

/**
 * INTERNAL API
 */
@InternalApi
private[akka] case class NoMaterializer(system: ActorSystem) extends Materializer {
  override def withNamePrefix(name: String): Materializer =
    throw new UnsupportedOperationException("NoMaterializer cannot be named")
  override def materialize[Mat](runnable: Graph[ClosedShape, Mat]): Mat =
    throw new UnsupportedOperationException("NoMaterializer cannot materialize")
  override def materialize[Mat](runnable: Graph[ClosedShape, Mat], defaultAttributes: Attributes): Mat =
    throw new UnsupportedOperationException("NoMaterializer cannot materialize")

  override def executionContext: ExecutionContextExecutor =
    throw new UnsupportedOperationException("NoMaterializer does not provide an ExecutionContext")

  def scheduleOnce(delay: FiniteDuration, task: Runnable): Cancellable =
    throw new UnsupportedOperationException("NoMaterializer cannot schedule a single event")

  def schedulePeriodically(initialDelay: FiniteDuration, interval: FiniteDuration, task: Runnable): Cancellable =
    throw new UnsupportedOperationException("NoMaterializer cannot schedule a repeated event")

  override def scheduleWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      task: Runnable): Cancellable =
    throw new UnsupportedOperationException("NoMaterializer cannot scheduleWithFixedDelay")

  override def scheduleAtFixedRate(
      initialDelay: FiniteDuration,
      interval: FiniteDuration,
      task: Runnable): Cancellable =
    throw new UnsupportedOperationException("NoMaterializer cannot scheduleAtFixedRate")

  override def shutdown(): Unit = throw new UnsupportedOperationException("NoMaterializer cannot shutdown")

  override def isShutdown: Boolean = throw new UnsupportedOperationException("NoMaterializer cannot shutdown")

  override private[akka] def logger = throw new UnsupportedOperationException("NoMaterializer does not have a logger")

  override private[akka] def supervisor =
    throw new UnsupportedOperationException("NoMaterializer does not have a supervisor")

  override private[akka] def actorOf(context: MaterializationContext, props: Props) =
    throw new UnsupportedOperationException("NoMaterializer cannot spawn actors")

  override def settings: ActorMaterializerSettings =
    throw new UnsupportedOperationException("NoMaterializer does not have settings")
}

@nowarn
object GraphInterpreterSpecKit {

  /**
   * Create logics and enumerate stages and ports
   *
   * @param stages Stages to "materialize" into graph stage logic instances
   * @param upstreams Upstream boundary logics that are already instances of graph stage logic and should be
   *                  part of the graph, is placed before the rest of the stages
   * @param downstreams Downstream boundary logics, is placed after the other stages
   * @param attributes Optional set of attributes to pass to the stages when creating the logics
   * @return Created logics and the maps of all inlets respective outlets to those logics
   */
  private[stream] def createLogics(
      stages: Array[GraphStageWithMaterializedValue[_ <: Shape, _]],
      upstreams: Array[UpstreamBoundaryStageLogic[_]],
      downstreams: Array[DownstreamBoundaryStageLogic[_]],
      attributes: Array[Attributes] = Array.empty)(implicit system: ActorSystem)
      : (Array[GraphStageLogic], SMap[Inlet[_], GraphStageLogic], SMap[Outlet[_], GraphStageLogic]) = {
    if (attributes.nonEmpty && attributes.length != stages.length)
      throw new IllegalArgumentException("Attributes must be either empty or one per stage")

    @nowarn("msg=deprecated")
    val defaultAttributes = ActorMaterializerSettings(system).toAttributes

    var inOwners = SMap.empty[Inlet[_], GraphStageLogic]
    var outOwners = SMap.empty[Outlet[_], GraphStageLogic]

    val logics = new Array[GraphStageLogic](upstreams.length + stages.length + downstreams.length)
    var idx = 0

    while (idx < upstreams.length) {
      val upstream = upstreams(idx)
      upstream.attributes = defaultAttributes
      upstream.stageId = idx
      logics(idx) = upstream
      upstream.out.id = 0
      outOwners = outOwners + (upstream.out -> upstream)
      idx += 1
    }

    var stageIdx = 0
    while (stageIdx < stages.length) {
      val stage = stages(stageIdx)
      setPortIds(stage.shape)

      val stageAttributes = defaultAttributes and {
        if (attributes.nonEmpty) stage.traversalBuilder.attributes and attributes(stageIdx)
        else stage.traversalBuilder.attributes
      }

      val logic = stage.createLogicAndMaterializedValue(stageAttributes)._1
      logic.attributes = stageAttributes
      logic.stageId = idx

      var inletIdx = 0
      while (inletIdx < stage.shape.inlets.length) {
        val inlet = stage.shape.inlets(inletIdx)
        inlet.id = inletIdx
        inOwners = inOwners + (inlet -> logic)
        inletIdx += 1
      }

      var outletIdx = 0
      while (outletIdx < stage.shape.outlets.length) {
        val outlet = stage.shape.outlets(outletIdx)
        outlet.id = outletIdx
        outOwners = outOwners + (outlet -> logic)
        outletIdx += 1
      }
      logics(idx) = logic

      idx += 1
      stageIdx += 1
    }

    var downstreamIdx = 0
    while (downstreamIdx < downstreams.length) {
      val downstream = downstreams(downstreamIdx)
      downstream.attributes = defaultAttributes
      downstream.stageId = idx
      logics(idx) = downstream
      downstream.in.id = 0
      inOwners = inOwners + (downstream.in -> downstream)

      idx += 1
      downstreamIdx += 1
    }

    (logics, inOwners, outOwners)
  }

  /**
   * Create connections given a list of flow logics where each one has one connection to the next one
   */
  private[stream] def createLinearFlowConnections(logics: Seq[GraphStageLogic]): Array[Connection] = {
    require(logics.length >= 2, s"$logics is too short to create a linear flow")
    logics
      .sliding(2)
      .zipWithIndex
      .map {
        case (window, idx) =>
          val outOwner = window(0)
          val inOwner = window(1)

          val connection = new Connection(
            id = idx,
            outOwner = outOwner,
            outHandler = outOwner.outHandler(0),
            inOwner = inOwner,
            inHandler = inOwner.inHandler(0))

          outOwner.portToConn(outOwner.inCount) = connection
          inOwner.portToConn(0) = connection

          connection
      }
      .toArray
  }

  /**
   * Create interpreter connections for all the given `connectedPorts`.
   */
  private[stream] def createConnections(
      connectedPorts: Seq[(Outlet[_], Inlet[_])],
      inOwners: SMap[Inlet[_], GraphStageLogic],
      outOwners: SMap[Outlet[_], GraphStageLogic]): Array[Connection] = {

    val connections = new Array[Connection](connectedPorts.size)
    connectedPorts.zipWithIndex.foreach {
      case ((outlet, inlet), idx) =>
        val outOwner = outOwners(outlet)
        val inOwner = inOwners(inlet)

        val connection = new Connection(
          id = idx,
          outOwner = outOwner,
          outHandler = outOwner.outHandler(outlet.id),
          inOwner = inOwner,
          inHandler = inOwner.inHandler(inlet.id))

        connections(idx) = connection
        inOwner.portToConn(inlet.id) = connection
        outOwner.portToConn(outOwner.inCount + outlet.id) = connection
    }
    connections
  }

  private def setPortIds(shape: Shape): Unit = {
    shape.inlets.zipWithIndex.foreach {
      case (inlet, idx) => inlet.id = idx
    }
    shape.outlets.zipWithIndex.foreach {
      case (outlet, idx) => outlet.id = idx
    }
  }

  private def setPortIds(stage: GraphStageWithMaterializedValue[_ <: Shape, _]): Unit = {
    stage.shape.inlets.zipWithIndex.foreach { case (inlet, idx)  => inlet.id = idx }
    stage.shape.outlets.zipWithIndex.foreach { case (inlet, idx) => inlet.id = idx }
  }

  private def setLogicIds(logics: Array[GraphStageLogic]): Unit = {
    logics.zipWithIndex.foreach { case (logic, idx) => logic.stageId = idx }
  }

}

trait GraphInterpreterSpecKit extends StreamSpec {

  import GraphInterpreterSpecKit._
  val logger = Logging(system, "InterpreterSpecKit")
  @nowarn("msg=deprecated")
  val defaultAttributes = ActorMaterializerSettings(system).toAttributes

  abstract class Builder {
    private var _interpreter: GraphInterpreter = _

    protected def interpreter: GraphInterpreter = _interpreter

    def stepAll(): Unit = interpreter.execute(eventLimit = Int.MaxValue)

    def step(): Unit = interpreter.execute(eventLimit = 1)

    object Upstream extends UpstreamBoundaryStageLogic[Int] {
      override val out = Outlet[Int]("up")
      out.id = 0
      override def toString = "Upstream"

      setHandler(out, new OutHandler {
        override def onPull() = {
          // TODO handler needed but should it do anything?
        }

        override def toString = "Upstream.OutHandler"
      })
    }

    object Downstream extends DownstreamBoundaryStageLogic[Int] {
      override val in = Inlet[Int]("down")
      in.id = 0
      setHandler(in, new InHandler {
        override def onPush() = {
          // TODO handler needed but should it do anything?
        }

        override def toString = "Downstream.InHandler"
      })

      override def toString = "Downstream"
    }

    class AssemblyBuilder(operators: Seq[GraphStageWithMaterializedValue[_ <: Shape, _]]) {
      private var upstreams = Vector.empty[UpstreamBoundaryStageLogic[_]]
      private var downstreams = Vector.empty[DownstreamBoundaryStageLogic[_]]
      private var connectedPorts = Vector.empty[(Outlet[_], Inlet[_])]

      def connect[T](upstream: UpstreamBoundaryStageLogic[T], in: Inlet[T]): AssemblyBuilder = {
        upstreams :+= upstream
        connectedPorts :+= upstream.out -> in
        this
      }

      def connect[T](out: Outlet[T], downstream: DownstreamBoundaryStageLogic[T]): AssemblyBuilder = {
        downstreams :+= downstream
        connectedPorts :+= out -> downstream.in
        this
      }

      def connect[T](out: Outlet[T], in: Inlet[T]): AssemblyBuilder = {
        connectedPorts :+= out -> in
        this
      }

      def init(): Unit = {
        val (logics, inOwners, outOwners) = createLogics(operators.toArray, upstreams.toArray, downstreams.toArray)
        val conns = createConnections(connectedPorts, inOwners, outOwners)

        manualInit(logics.toArray, conns)
      }
    }

    def manualInit(logics: Array[GraphStageLogic], connections: Array[Connection]): Unit = {
      // set some default attributes where missing
      logics.foreach { l =>
        if (l.attributes == Attributes.none) l.attributes = defaultAttributes
      }

      _interpreter = new GraphInterpreter(
        NoMaterializer(system),
        logger,
        logics,
        connections,
        onAsyncInput = (_, _, _, _) => (),
        fuzzingMode = false,
        context = null)
      _interpreter.init(null)
    }

    def builder(operators: GraphStageWithMaterializedValue[_ <: Shape, _]*): AssemblyBuilder =
      new AssemblyBuilder(operators.toVector)

  }

  abstract class TestSetup extends Builder {

    sealed trait TestEvent {
      def source: GraphStageLogic
    }

    case class OnComplete(source: GraphStageLogic) extends TestEvent
    case class Cancel(source: GraphStageLogic, cause: Throwable) extends TestEvent
    case class OnError(source: GraphStageLogic, cause: Throwable) extends TestEvent
    case class OnNext(source: GraphStageLogic, elem: Any) extends TestEvent
    case class RequestOne(source: GraphStageLogic) extends TestEvent
    case class RequestAnother(source: GraphStageLogic) extends TestEvent
    case class PreStart(source: GraphStageLogic) extends TestEvent
    case class PostStop(source: GraphStageLogic) extends TestEvent

    protected var lastEvent: Set[TestEvent] = Set.empty

    def lastEvents(): Set[TestEvent] = {
      val result = lastEvent
      clearEvents()
      result
    }

    def clearEvents(): Unit = lastEvent = Set.empty

    class UpstreamProbe[T](override val toString: String) extends UpstreamBoundaryStageLogic[T] {
      val out = Outlet[T]("out")
      out.id = 0

      setHandler(out, new OutHandler {
        override def onPull(): Unit = lastEvent += RequestOne(UpstreamProbe.this)
        override def onDownstreamFinish(cause: Throwable): Unit = lastEvent += Cancel(UpstreamProbe.this, cause)
        override def toString = s"${UpstreamProbe.this.toString}.outHandler"
      })

      def onNext(elem: T, eventLimit: Int = Int.MaxValue): Unit = {
        if (GraphInterpreter.Debug) println(s"----- NEXT: $this $elem")
        push(out, elem)
        interpreter.execute(eventLimit)
      }

      def onComplete(eventLimit: Int = Int.MaxValue): Unit = {
        if (GraphInterpreter.Debug) println(s"----- COMPLETE $this")
        complete(out)
        interpreter.execute(eventLimit)
      }

      def onFailure(eventLimit: Int = Int.MaxValue, ex: Throwable): Unit = {
        if (GraphInterpreter.Debug) println(s"----- FAIL $this")
        fail(out, ex)
        interpreter.execute(eventLimit)
      }
    }

    class DownstreamProbe[T](override val toString: String) extends DownstreamBoundaryStageLogic[T] {
      val in = Inlet[T]("in")
      in.id = 0

      setHandler(in, new InHandler {
        override def onPush(): Unit = lastEvent += OnNext(DownstreamProbe.this, grab(in))
        override def onUpstreamFinish(): Unit = lastEvent += OnComplete(DownstreamProbe.this)
        override def onUpstreamFailure(ex: Throwable): Unit = lastEvent += OnError(DownstreamProbe.this, ex)
        override def toString = s"${DownstreamProbe.this.toString}.inHandler"
      })

      def requestOne(eventLimit: Int = Int.MaxValue): Unit = {
        if (GraphInterpreter.Debug) println(s"----- REQ $this")
        pull(in)
        interpreter.execute(eventLimit)
      }

      def cancel(eventLimit: Int = Int.MaxValue): Unit = {
        if (GraphInterpreter.Debug) println(s"----- CANCEL $this")
        cancel(in)
        interpreter.execute(eventLimit)
      }
    }

  }

  abstract class PortTestSetup(chasing: Boolean = false) extends TestSetup {
    val out = new UpstreamPortProbe[Int]
    val in = new DownstreamPortProbe[Int]

    class EventPropagateStage extends GraphStage[FlowShape[Int, Int]] {
      val in = Inlet[Int]("Propagate.in")
      val out = Outlet[Int]("Propagate.out")
      in.id = 0
      out.id = 0
      override val shape: FlowShape[Int, Int] = FlowShape(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) with InHandler with OutHandler {
          override def onPush(): Unit = push(out, grab(in))
          override def onPull(): Unit = pull(in)
          override def onUpstreamFinish(): Unit = complete(out)
          override def onUpstreamFailure(ex: Throwable): Unit = fail(out, ex)
          override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)

          setHandlers(in, out, this)
        }
      override def toString = "EventPropagateStage"
    }

    // step() means different depending whether we have a stage between the two probes or not
    override def step(): Unit = interpreter.execute(eventLimit = if (!chasing) 1 else 2)

    class UpstreamPortProbe[T] extends UpstreamProbe[T]("upstreamPort") {
      def isAvailable: Boolean = isAvailable(this.out)
      def isClosed: Boolean = isClosed(this.out)

      def push(elem: T): Unit = push(this.out, elem)
      def complete(): Unit = complete(this.out)
      def fail(ex: Throwable): Unit = fail(this.out, ex)
    }

    class DownstreamPortProbe[T] extends DownstreamProbe[T]("upstreamPort") {
      def isAvailable: Boolean = isAvailable(this.in)
      def hasBeenPulled: Boolean = hasBeenPulled(this.in)
      def isClosed: Boolean = isClosed(this.in)

      def pull(): Unit = pull(this.in)
      def cancel(): Unit = cancel(this.in)
      def grab(): T = grab(this.in)

      setHandler(this.in, new InHandler {

        // Modified onPush that does not grab() automatically the element. This accesses some internals.
        override def onPush(): Unit = {
          val internalEvent = portToConn(DownstreamPortProbe.this.in.id).slot

          internalEvent match {
            case Failed(_, elem) => lastEvent += OnNext(DownstreamPortProbe.this, elem)
            case elem            => lastEvent += OnNext(DownstreamPortProbe.this, elem)
          }
        }

        override def onUpstreamFinish() = lastEvent += OnComplete(DownstreamPortProbe.this)
        override def onUpstreamFailure(ex: Throwable) = lastEvent += OnError(DownstreamPortProbe.this, ex)
      })
    }

    val (logics, connections) =
      if (!chasing) {
        val logics = Array[GraphStageLogic](out, in)
        setLogicIds(logics)
        val connections = GraphInterpreterSpecKit.createLinearFlowConnections(logics.toIndexedSeq)
        (logics, connections)
      } else {
        val propagateStage = new EventPropagateStage
        setPortIds(propagateStage)
        val logics = Array[GraphStageLogic](out, propagateStage.createLogic(Attributes.none), in)
        setLogicIds(logics)
        val connections = GraphInterpreterSpecKit.createLinearFlowConnections(logics.toIndexedSeq)
        (logics, connections)
      }

    manualInit(logics, connections)
  }

  abstract class FailingStageSetup(initFailOnNextEvent: Boolean = false) extends TestSetup {

    val upstream = new UpstreamPortProbe[Int]
    val downstream = new DownstreamPortProbe[Int]

    private var _failOnNextEvent: Boolean = initFailOnNextEvent
    private var _failOnPostStop: Boolean = false

    def failOnNextEvent(): Unit = _failOnNextEvent = true
    def failOnPostStop(): Unit = _failOnPostStop = true

    def testException = TE("test")

    private val stagein = Inlet[Int]("sandwitch.in")
    private val stageout = Outlet[Int]("sandwitch.out")
    private val stageshape = FlowShape(stagein, stageout)

    // Must be lazy because I turned this stage "inside-out" therefore changing initialization order
    // to make tests a bit more readable
    lazy val insideOutStage: GraphStageLogic = new GraphStageLogic(stageshape) {
      private def mayFail(task: => Unit): Unit = {
        if (!_failOnNextEvent) task
        else {
          _failOnNextEvent = false
          throw testException
        }
      }

      setHandler(stagein, new InHandler {
        override def onPush(): Unit = mayFail(push(stageout, grab(stagein)))
        override def onUpstreamFinish(): Unit = mayFail(completeStage())
        override def onUpstreamFailure(ex: Throwable): Unit = mayFail(failStage(ex))
        override def toString = "insideOutStage.stagein"
      })

      setHandler(stageout, new OutHandler {
        override def onPull(): Unit = mayFail(pull(stagein))
        override def onDownstreamFinish(cause: Throwable): Unit = mayFail(completeStage())
        override def toString = "insideOutStage.stageout"
      })

      override def preStart(): Unit = mayFail(lastEvent += PreStart(insideOutStage))
      override def postStop(): Unit =
        if (!_failOnPostStop) lastEvent += PostStop(insideOutStage)
        else throw testException

      override def toString = "insideOutStage"
    }

    private val sandwitchStage = new GraphStage[FlowShape[Int, Int]] {
      override def shape = stageshape
      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = insideOutStage
      override def toString = "sandwitchStage"
    }

    class UpstreamPortProbe[T] extends UpstreamProbe[T]("upstreamPort") {
      def push(elem: T): Unit = push(out, elem)
      def complete(): Unit = complete(out)
      def fail(ex: Throwable): Unit = fail(out, ex)
    }

    class DownstreamPortProbe[T] extends DownstreamProbe[T]("upstreamPort") {
      def pull(): Unit = pull(in)
      def cancel(): Unit = cancel(in)
    }

    builder(sandwitchStage).connect(upstream, stagein).connect(stageout, downstream).init()
  }

  abstract class OneBoundedSetupWithDecider[T](decider: Decider, ops: GraphStageWithMaterializedValue[Shape, Any]*)
      extends Builder {

    val upstream = new UpstreamOneBoundedProbe[T]
    val downstream = new DownstreamOneBoundedPortProbe[T]
    var lastEvent = Set.empty[TestEvent]

    sealed trait TestEvent

    case object OnComplete extends TestEvent
    case class Cancel(cause: Throwable) extends TestEvent
    case class OnError(cause: Throwable) extends TestEvent
    case class OnNext(elem: Any) extends TestEvent
    case object RequestOne extends TestEvent
    case object RequestAnother extends TestEvent

    private def run() = interpreter.execute(Int.MaxValue)

    private def initialize(): Unit = {
      val supervision = ActorAttributes.supervisionStrategy(decider)
      val attributes = Array.fill[Attributes](ops.length)(supervision)
      val (logics, _, _) = createLogics(ops.toArray, Array(upstream), Array(downstream), attributes)
      val connections = createLinearFlowConnections(logics.toIndexedSeq)
      manualInit(logics, connections)
    }

    initialize()
    run() // Detached stages need the prefetch

    def lastEvents(): Set[TestEvent] = {
      val events = lastEvent
      lastEvent = Set.empty
      events
    }

    class UpstreamOneBoundedProbe[TT] extends UpstreamBoundaryStageLogic[TT] {
      val out = Outlet[TT]("out")
      out.id = 0

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (lastEvent.contains(RequestOne)) lastEvent += RequestAnother
            else lastEvent += RequestOne
          }

          override def onDownstreamFinish(cause: Throwable): Unit = lastEvent += Cancel(cause)
        })

      def onNext(elem: TT): Unit = {
        push(out, elem)
        run()
      }
      def onComplete(): Unit = {
        complete(out)
        run()
      }

      def onNextAndComplete(elem: TT): Unit = {
        push(out, elem)
        complete(out)
        run()
      }

      def onError(ex: Throwable): Unit = {
        fail(out, ex)
        run()
      }
    }

    class DownstreamOneBoundedPortProbe[TT] extends DownstreamBoundaryStageLogic[TT] {
      val in = Inlet[TT]("in")
      in.id = 0

      setHandler(in, new InHandler {

        // Modified onPush that does not grab() automatically the element. This accesses some internals.
        override def onPush(): Unit = {
          lastEvent += OnNext(grab(in))
        }

        override def onUpstreamFinish() = lastEvent += OnComplete
        override def onUpstreamFailure(ex: Throwable) = lastEvent += OnError(ex)
      })

      def requestOne(): Unit = {
        pull(in)
        run()
      }

      def cancel(): Unit = {
        cancel(in, SubscriptionWithCancelException.NoMoreElementsNeeded)
        run()
      }

    }

  }

  abstract class OneBoundedSetup[T](_ops: GraphStageWithMaterializedValue[Shape, Any]*)
      extends OneBoundedSetupWithDecider[T](Supervision.stoppingDecider, _ops: _*)
}
