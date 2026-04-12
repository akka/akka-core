/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration

import org.reactivestreams.Processor
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

import akka.NotUsed
import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.Deploy
import akka.actor.PoisonPill
import akka.actor.Props
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.stream._
import akka.stream.Attributes.InputBuffer
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout.AtomicModule
import akka.stream.impl.fusing._
import akka.stream.impl.fusing.ActorGraphInterpreter.ActorOutputBoundary
import akka.stream.impl.fusing.ActorGraphInterpreter.BatchingActorInputBoundary
import akka.stream.impl.fusing.GraphInterpreter.Connection
import akka.stream.impl.io.TLSActor
import akka.stream.impl.io.TlsModule
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PhasedFusingActorMaterializer {
  // Compile time constant
  final val Debug = false
  final val Mailbox = "akka.stream.materializer.mailbox"

  val DefaultPhase: Phase[Any] = new Phase[Any] {
    override def apply(
        settings: ActorMaterializerSettings,
        effectiveAttributes: Attributes,
        materializer: PhasedFusingActorMaterializer,
        islandName: String): PhaseIsland[Any] =
      new GraphStageIsland(effectiveAttributes, materializer, islandName, subflowFuser = OptionVal.None)
        .asInstanceOf[PhaseIsland[Any]]
  }

  val DefaultPhases: Map[IslandTag, Phase[Any]] = Map[IslandTag, Phase[Any]](
    SinkModuleIslandTag -> new Phase[Any] {
      override def apply(
          settings: ActorMaterializerSettings,
          effectiveAttributes: Attributes,
          materializer: PhasedFusingActorMaterializer,
          islandName: String): PhaseIsland[Any] =
        new SinkModulePhase(materializer, islandName).asInstanceOf[PhaseIsland[Any]]
    },
    SourceModuleIslandTag -> new Phase[Any] {
      override def apply(
          settings: ActorMaterializerSettings,
          effectiveAttributes: Attributes,
          materializer: PhasedFusingActorMaterializer,
          islandName: String): PhaseIsland[Any] =
        new SourceModulePhase(materializer, islandName).asInstanceOf[PhaseIsland[Any]]
    },
    ProcessorModuleIslandTag -> new Phase[Any] {
      override def apply(
          settings: ActorMaterializerSettings,
          effectiveAttributes: Attributes,
          materializer: PhasedFusingActorMaterializer,
          islandName: String): PhaseIsland[Any] =
        new ProcessorModulePhase().asInstanceOf[PhaseIsland[Any]]
    },
    TlsModuleIslandTag -> new Phase[Any] {
      def apply(
          settings: ActorMaterializerSettings,
          effectiveAttributes: Attributes,
          materializer: PhasedFusingActorMaterializer,
          islandName: String): PhaseIsland[Any] =
        new TlsModulePhase(materializer, islandName).asInstanceOf[PhaseIsland[Any]]
    },
    GraphStageTag -> DefaultPhase)

  def apply(
      context: ActorContext,
      namePrefix: String,
      settings: ActorMaterializerSettings,
      attributes: Attributes): PhasedFusingActorMaterializer = {
    val haveShutDown = new AtomicBoolean(false)

    val dispatcher = attributes.mandatoryAttribute[ActorAttributes.Dispatcher].dispatcher
    val supervisorProps =
      StreamSupervisor
        .props(attributes, haveShutDown)
        .withDispatcher(dispatcher)
        .withMailbox(Mailbox)
        .withDeploy(Deploy.local)

    // FIXME why do we need a global unique name for the child?
    val streamSupervisor = context.actorOf(supervisorProps, StreamSupervisor.nextName())

    new PhasedFusingActorMaterializer(
      context.system,
      settings,
      attributes,
      context.system.dispatchers,
      streamSupervisor,
      haveShutDown,
      FlowNames(context.system).name.copy(namePrefix))
  }
}

private final case class SegmentInfo(
    globalislandOffset: Int, // The island to which the segment belongs
    length: Int, // How many slots are contained by the segment
    globalBaseOffset: Int, // The global slot where this segment starts
    relativeBaseOffset: Int, // the local offset of the slot where this segment starts
    phase: PhaseIsland[Any]) {

  override def toString: String =
    s"""
       | Segment
       |  globalislandOffset = $globalislandOffset
       |  length             = $length
       |  globalBaseOffset   = $globalBaseOffset
       |  relativeBaseOffset = $relativeBaseOffset
       |  phase              = $phase
       """.stripMargin
}

private final case class ForwardWire(
    islandGlobalOffset: Int,
    from: OutPort,
    toGlobalOffset: Int,
    outStage: Any,
    phase: PhaseIsland[Any]) {

  override def toString: String =
    s"ForwardWire(islandId = $islandGlobalOffset, from = $from, toGlobal = $toGlobalOffset, phase = $phase)"
}

private final case class SavedIslandData(
    islandGlobalOffset: Int,
    lastVisitedOffset: Int,
    skippedSlots: Int,
    phase: PhaseIsland[Any])

@InternalApi private[akka] class IslandTracking(
    phases: Map[IslandTag, Phase[Any]],
    settings: ActorMaterializerSettings,
    attributes: Attributes,
    defaultPhase: Phase[Any],
    materializer: PhasedFusingActorMaterializer,
    islandNamePrefix: String) {

  import PhasedFusingActorMaterializer.Debug

  private var islandNameCounter = 0
  private def nextIslandName(): String = {
    val s = islandNamePrefix + islandNameCounter
    islandNameCounter += 1
    s
  }

  private var currentGlobalOffset = 0
  private var currentSegmentGlobalOffset = 0
  private var currentIslandGlobalOffset = 0
  // The number of slots that belong to segments of other islands encountered so far, from the
  // beginning of the island
  private var currentIslandSkippedSlots = 0

  private var segments: java.util.ArrayList[SegmentInfo] = _
  private var activePhases: java.util.ArrayList[PhaseIsland[Any]] = _
  private var forwardWires: java.util.ArrayList[ForwardWire] = _
  private var islandStateStack: java.util.ArrayList[SavedIslandData] = _

  private var currentPhase: PhaseIsland[Any] = defaultPhase.apply(settings, attributes, materializer, nextIslandName())

  @InternalApi private[akka] def getCurrentPhase: PhaseIsland[Any] = currentPhase
  @InternalApi private[akka] def getCurrentOffset: Int = currentGlobalOffset

  private def completeSegment(): Unit = {
    val length = currentGlobalOffset - currentSegmentGlobalOffset

    if (activePhases eq null) {
      activePhases = new util.ArrayList[PhaseIsland[Any]](8)
      islandStateStack = new java.util.ArrayList[SavedIslandData](4)
    }

    if (length > 0) {
      // We just finished a segment by entering an island.
      val previousSegment = SegmentInfo(
        globalislandOffset = currentIslandGlobalOffset,
        length = currentGlobalOffset - currentSegmentGlobalOffset,
        globalBaseOffset = currentSegmentGlobalOffset,
        relativeBaseOffset = currentSegmentGlobalOffset - currentIslandGlobalOffset - currentIslandSkippedSlots,
        currentPhase)

      // Segment tracking is by demand, we only allocate this list if it is used.
      // If there are no islands, then there is no need to track segments
      if (segments eq null) segments = new java.util.ArrayList[SegmentInfo](8)
      segments.add(previousSegment)
      if (Debug) println(s"Completed segment $previousSegment")
    } else {
      if (Debug) println(s"Skipped zero length segment")
    }
  }

  @InternalApi private[akka] def enterIsland(tag: IslandTag, attributes: Attributes): Unit = {
    completeSegment()
    val previousPhase = currentPhase
    val previousIslandOffset = currentIslandGlobalOffset
    islandStateStack.add(
      SavedIslandData(previousIslandOffset, currentGlobalOffset, currentIslandSkippedSlots, previousPhase))

    currentPhase = phases(tag)(settings, attributes, materializer, nextIslandName())
    activePhases.add(currentPhase)

    // Resolve the phase to be used to materialize this island
    currentIslandGlobalOffset = currentGlobalOffset

    // The base offset of this segment is the current global offset
    currentSegmentGlobalOffset = currentGlobalOffset
    currentIslandSkippedSlots = 0
    if (Debug) println(s"Entering island starting at offset = $currentIslandGlobalOffset phase = $currentPhase")
  }

  @InternalApi private[akka] def exitIsland(): Unit = {
    val parentIsland = islandStateStack.remove(islandStateStack.size() - 1)
    completeSegment()

    // We start a new segment
    currentSegmentGlobalOffset = currentGlobalOffset

    // We restore data for the island
    currentIslandGlobalOffset = parentIsland.islandGlobalOffset
    currentPhase = parentIsland.phase
    currentIslandSkippedSlots = parentIsland.skippedSlots + (currentGlobalOffset - parentIsland.lastVisitedOffset)

    if (Debug) println(s"Exited to island starting at offset = $currentIslandGlobalOffset phase = $currentPhase")
  }

  @InternalApi private[akka] def wireIn(in: InPort, logic: Any): Unit = {
    // The slot for this InPort always belong to the current segment, so resolving its local
    // offset/slot is simple
    val localInSlot = currentGlobalOffset - currentIslandGlobalOffset - currentIslandSkippedSlots
    if (Debug) println(s"  wiring port $in inOffs absolute = $currentGlobalOffset local = $localInSlot")

    // Assign the logic belonging to the current port to its calculated local slot in the island
    currentPhase.assignPort(in, localInSlot, logic)

    // Check if there was any forward wiring that has this offset/slot as its target
    // First try to find such wiring
    var forwardWire: ForwardWire = null
    if ((forwardWires ne null) && !forwardWires.isEmpty) {
      var i = 0
      while (i < forwardWires.size()) {
        forwardWire = forwardWires.get(i)
        if (forwardWire.toGlobalOffset == currentGlobalOffset) {
          if (Debug) println(s"    there is a forward wire to this slot $forwardWire")
          forwardWires.remove(i)
          i = Int.MaxValue // Exit the loop
        } else {
          forwardWire = null // Didn't found it yet
          i += 1
        }
      }
    }

    // If there is a forward wiring we need to resolve it
    if (forwardWire ne null) {
      // The forward wire ends up in the same island
      if (forwardWire.phase eq currentPhase) {
        if (Debug)
          println(s"    in-island forward wiring from port ${forwardWire.from} wired to local slot = $localInSlot")
        forwardWire.phase.assignPort(forwardWire.from, localInSlot, forwardWire.outStage)
      } else {
        if (Debug)
          println(s"    cross island forward wiring from port ${forwardWire.from} wired to local slot = $localInSlot")
        val publisher = forwardWire.phase.createPublisher(forwardWire.from, forwardWire.outStage)
        currentPhase.takePublisher(localInSlot, publisher, null)
      }
    }

    currentGlobalOffset += 1
  }

  @InternalApi private[akka] def wireOut(out: OutPort, absoluteOffset: Int, logic: Any): Unit = {
    if (Debug) println(s"  wiring $out to absolute = $absoluteOffset")

    //          <-------- backwards, visited stuff
    //                   ------------> forward, not yet visited
    // ---------------- .. (we are here)
    // First check if we are wiring backwards. This is important since we can only do resolution for backward wires.
    // In other cases we need to record the forward wire and resolve it later once its target inSlot has been visited.
    if (absoluteOffset < currentGlobalOffset) {
      if (Debug) println("    backward wiring")

      if (absoluteOffset >= currentSegmentGlobalOffset) {
        // Wiring is in the same segment, no complex lookup needed
        val localInSlot = absoluteOffset - currentIslandGlobalOffset - currentIslandSkippedSlots
        if (Debug)
          println(
            s"    in-segment wiring to local ($absoluteOffset - $currentIslandGlobalOffset - $currentIslandSkippedSlots) = $localInSlot")
        currentPhase.assignPort(out, localInSlot, logic)
      } else {
        // Wiring is cross-segment, but we don't know if it is cross-island or not yet
        // We must find the segment to which this slot belongs first
        var i = segments.size() - 1
        var targetSegment: SegmentInfo = segments.get(i)
        // Skip segments that have a higher offset than our slot, until we find the containing segment
        while (i > 0 && targetSegment.globalBaseOffset > absoluteOffset) {
          i -= 1
          targetSegment = segments.get(i)
        }

        // Independently of the target island the local slot for the target island is calculated the same:
        //   - Calculate the relative offset of the local slot in the segment
        //   - calculate the island relative offset by adding the island relative base offset of the segment
        val distanceFromSegmentStart = absoluteOffset - targetSegment.globalBaseOffset
        val localInSlot = distanceFromSegmentStart + targetSegment.relativeBaseOffset

        if (targetSegment.phase eq currentPhase) {
          if (Debug) println(s"    cross-segment, in-island wiring to local slot $localInSlot")
          currentPhase.assignPort(out, localInSlot, logic)
        } else {
          if (Debug) println(s"    cross-island wiring to local slot $localInSlot in target island")
          val publisher = currentPhase.createPublisher(out, logic)
          targetSegment.phase.takePublisher(localInSlot, publisher, null)
        }
      }
    } else {
      // We need to record the forward wiring so we can resolve it later

      // The forward wire tracking data structure is only allocated when needed. Many graphs have no forward wires
      // even though it might have islands.
      if (forwardWires eq null) {
        forwardWires = new java.util.ArrayList[ForwardWire](8)
      }

      val forwardWire = ForwardWire(
        islandGlobalOffset = currentIslandGlobalOffset,
        from = out,
        toGlobalOffset = absoluteOffset,
        logic,
        currentPhase)

      if (Debug) println(s"    wiring is forward, recording $forwardWire")
      forwardWires.add(forwardWire)
    }

  }

  @InternalApi private[akka] def allNestedIslandsReady(): Unit = {
    if (activePhases ne null) {
      var i = 0
      while (i < activePhases.size()) {
        activePhases.get(i).onIslandReady()
        i += 1
      }
    }
  }

}

/**
 * INTERNAL API
 *
 * `defaultAttributes` for the materializer, based on the [[ActorMaterializerSettings]] and
 * are always seen as least specific, so any attribute specified in the graph "wins" over these.
 * In addition to that this also guarantees that the attributes `InputBuffer`, `SupervisionStrategy`,
 * and `Dispatcher` is _always_ present in the attributes and can be accessed through `Attributes.mandatoryAttribute`
 *
 * When these attributes are needed later in the materialization process it is important that
 * they are gotten through the attributes and not through the [[ActorMaterializerSettings]]
 */
@InternalApi private[akka] case class PhasedFusingActorMaterializer(
    system: ActorSystem,
    override val settings: ActorMaterializerSettings,
    defaultAttributes: Attributes, // see description above
    dispatchers: Dispatchers,
    supervisor: ActorRef,
    haveShutDown: AtomicBoolean,
    flowNames: SeqActorName)
    extends ExtendedActorMaterializer {
  import PhasedFusingActorMaterializer._

  private val _logger = Logging.getLogger(system, this)
  override def logger: LoggingAdapter = _logger
  private val fuzzingWarningDisabled =
    system.settings.config.hasPath("akka.stream.secret-test-fuzzing-warning-disable")

  override def shutdown(): Unit =
    if (haveShutDown.compareAndSet(false, true)) supervisor ! PoisonPill

  override def isShutdown: Boolean = haveShutDown.get()

  override def withNamePrefix(name: String): PhasedFusingActorMaterializer = this.copy(flowNames = flowNames.copy(name))

  private[this] def createFlowName(): String = flowNames.next()

  // note that this will never be overridden on a per-graph-stage basis regardless of more specific attributes
  override lazy val executionContext: ExecutionContextExecutor =
    dispatchers.lookup(defaultAttributes.mandatoryAttribute[ActorAttributes.Dispatcher].dispatcher)

  override def scheduleWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      task: Runnable): Cancellable =
    system.scheduler.scheduleWithFixedDelay(initialDelay, delay)(task)(executionContext)

  override def scheduleAtFixedRate(
      initialDelay: FiniteDuration,
      interval: FiniteDuration,
      task: Runnable): Cancellable =
    system.scheduler.scheduleAtFixedRate(initialDelay, interval)(task)(executionContext)

  override def schedulePeriodically(
      initialDelay: FiniteDuration,
      interval: FiniteDuration,
      task: Runnable): Cancellable =
    system.scheduler.scheduleAtFixedRate(initialDelay, interval)(task)(executionContext)

  override def scheduleOnce(delay: FiniteDuration, task: Runnable): Cancellable =
    system.scheduler.scheduleOnce(delay, task)(executionContext)

  override def materialize[Mat](_runnableGraph: Graph[ClosedShape, Mat]): Mat =
    materialize(_runnableGraph, defaultAttributes)

  @InternalStableApi
  override def materialize[Mat](_runnableGraph: Graph[ClosedShape, Mat], defaultAttributes: Attributes): Mat =
    materialize(
      _runnableGraph,
      defaultAttributes,
      PhasedFusingActorMaterializer.DefaultPhase,
      PhasedFusingActorMaterializer.DefaultPhases)

  override def materialize[Mat](
      graph: Graph[ClosedShape, Mat],
      defaultAttributes: Attributes,
      defaultPhase: Phase[Any],
      phases: Map[IslandTag, Phase[Any]]): Mat = {
    if (isShutdown) throw new IllegalStateException("Trying to materialize stream after materializer has been shutdown")

    // combine default attributes with top-level runnable/closed graph shape attributes so that per-stream
    // attributes overriding defaults are used also for the top level interpreter etc.
    val defaultAndGraphAttributes = defaultAttributes and graph.traversalBuilder.attributes
    if (defaultAndGraphAttributes.mandatoryAttribute[ActorAttributes.FuzzingMode].enabled && !fuzzingWarningDisabled) {
      _logger.warning(
        "Fuzzing mode is enabled on this system. If you see this warning on your production system then " +
        "set 'akka.stream.materializer.debug.fuzzing-mode' to off.")
    }

    val islandTracking = new IslandTracking(
      phases,
      settings,
      defaultAndGraphAttributes,
      defaultPhase,
      this,
      islandNamePrefix = createFlowName() + "-")

    var current: Traversal = graph.traversalBuilder.traversal
    val attributesStack = new java.util.ArrayDeque[Attributes](8)
    attributesStack.addLast(defaultAndGraphAttributes)

    val traversalStack = new java.util.ArrayDeque[Traversal](16)
    traversalStack.addLast(current)

    val matValueStack = new java.util.ArrayDeque[Any](8)

    if (Debug) {
      println(s"--- Materializing layout:")
      TraversalBuilder.printTraversal(current)
      println(s"--- Start materialization")
    }

    // Due to how Concat works, we need a stack. This probably can be optimized for the most common cases.
    while (!traversalStack.isEmpty) {
      current = traversalStack.removeLast()

      while (current ne EmptyTraversal) {
        var nextStep: Traversal = EmptyTraversal
        current match {
          case MaterializeAtomic(mod, outToSlot) =>
            if (Debug) println(s"materializing module: $mod")
            val matAndStage = islandTracking.getCurrentPhase.materializeAtomic(mod, attributesStack.getLast)
            val logic = matAndStage._1
            val matValue = matAndStage._2
            if (Debug) println(s"  materialized value is $matValue")
            matValueStack.addLast(matValue)

            val stageGlobalOffset = islandTracking.getCurrentOffset

            wireInlets(islandTracking, mod, logic)
            wireOutlets(islandTracking, mod, logic, stageGlobalOffset, outToSlot)

            if (Debug) println(s"PUSH: $matValue => $matValueStack")

          case Concat(first, next) =>
            if (next ne EmptyTraversal) traversalStack.add(next)
            nextStep = first
          case Pop =>
            val popped = matValueStack.removeLast()
            if (Debug) println(s"POP: $popped => $matValueStack")
          case PushNotUsed =>
            matValueStack.addLast(NotUsed)
            if (Debug) println(s"PUSH: NotUsed => $matValueStack")
          case transform: Transform =>
            val prev = matValueStack.removeLast()
            val result = transform(prev)
            matValueStack.addLast(result)
            if (Debug) println(s"TRFM: $matValueStack")
          case compose: Compose =>
            val second = matValueStack.removeLast()
            val first = matValueStack.removeLast()
            val result = compose(first, second)
            matValueStack.addLast(result)
            if (Debug) println(s"COMP: $matValueStack")
          case PushAttributes(attr) =>
            attributesStack.addLast(attributesStack.getLast and attr)
            if (Debug) println(s"ATTR PUSH: $attr")
          case PopAttributes =>
            attributesStack.removeLast()
            if (Debug) println(s"ATTR POP")
          case EnterIsland(tag) =>
            islandTracking.enterIsland(tag, attributesStack.getLast)
          case ExitIsland =>
            islandTracking.exitIsland()
          case _ =>
        }
        current = nextStep
      }
    }

    def shutdownWhileMaterializingFailure =
      new IllegalStateException("Materializer shutdown while materializing stream")
    try {
      islandTracking.getCurrentPhase.onIslandReady()
      islandTracking.allNestedIslandsReady()

      if (Debug) println("--- Finished materialization")
      matValueStack.peekLast().asInstanceOf[Mat]

    } finally {
      if (isShutdown) throw shutdownWhileMaterializingFailure
    }

  }

  private def wireInlets(
      islandTracking: IslandTracking,
      mod: StreamLayout.AtomicModule[Shape, Any],
      logic: Any): Unit = {
    val inlets = mod.shape.inlets
    if (inlets.nonEmpty) {
      if (Shape.hasOnePort(inlets)) {
        // optimization, duplication to avoid iterator allocation
        islandTracking.wireIn(inlets.head, logic)
      } else {
        val ins = inlets.iterator
        while (ins.hasNext) {
          val in = ins.next()
          islandTracking.wireIn(in, logic)
        }
      }
    }
  }

  private def wireOutlets(
      islandTracking: IslandTracking,
      mod: StreamLayout.AtomicModule[Shape, Any],
      logic: Any,
      stageGlobalOffset: Int,
      outToSlot: Array[Int]): Unit = {
    val outlets = mod.shape.outlets
    if (outlets.nonEmpty) {
      if (Shape.hasOnePort(outlets)) {
        // optimization, duplication to avoid iterator allocation
        val out = outlets.head
        val absoluteTargetSlot = stageGlobalOffset + outToSlot(out.id)
        if (Debug) println(s"  wiring offset: ${outToSlot.mkString("[", ",", "]")}")
        islandTracking.wireOut(out, absoluteTargetSlot, logic)
      } else {
        val outs = outlets.iterator
        while (outs.hasNext) {
          val out = outs.next()
          val absoluteTargetSlot = stageGlobalOffset + outToSlot(out.id)
          if (Debug) println(s"  wiring offset: ${outToSlot.mkString("[", ",", "]")}")
          islandTracking.wireOut(out, absoluteTargetSlot, logic)
        }
      }
    }
  }

  override def makeLogger(logSource: Class[Any]): LoggingAdapter =
    Logging(system, logSource)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] override def actorOf(context: MaterializationContext, props: Props): ActorRef = {
    val effectiveProps = props.dispatcher match {
      case Dispatchers.DefaultDispatcherId =>
        props
          .withDispatcher(context.effectiveAttributes.mandatoryAttribute[ActorAttributes.Dispatcher].dispatcher)
          .withMailbox(Mailbox)
      case _ => props
    }

    actorOf(effectiveProps, context.islandName)
  }

}

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] trait IslandTag

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] trait Phase[M] {
  def apply(
      settings: ActorMaterializerSettings,
      effectiveAttributes: Attributes,
      materializer: PhasedFusingActorMaterializer,
      islandName: String): PhaseIsland[M]
}

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] trait PhaseIsland[M] {

  def name: String

  @InternalStableApi
  def materializeAtomic(mod: AtomicModule[Shape, Any], attributes: Attributes): (M, Any)

  @InternalStableApi
  def assignPort(in: InPort, slot: Int, logic: M): Unit

  @InternalStableApi
  def assignPort(out: OutPort, slot: Int, logic: M): Unit

  @InternalStableApi
  def createPublisher(out: OutPort, logic: M): Publisher[Any]

  @InternalStableApi
  def takePublisher(slot: Int, publisher: Publisher[Any], attributes: Attributes): Unit

  def onIslandReady(): Unit

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object GraphStageTag extends IslandTag

/**
 * INTERNAL API
 */
@InternalApi private[akka] object GraphStageIsland {
  private val logicArrayType = Array.empty[GraphStageLogic]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class GraphStageIsland(
    effectiveAttributes: Attributes,
    materializer: PhasedFusingActorMaterializer,
    islandName: String,
    subflowFuser: OptionVal[GraphInterpreterShell => ActorRef])
    extends PhaseIsland[GraphStageLogic] {

  private[this] val logics = new util.ArrayList[GraphStageLogic](16)

  private var connections = new Array[Connection](16)
  private var maxConnections = 0
  private var outConnections: List[Connection] = Nil
  private var fullIslandName: OptionVal[String] = OptionVal.None

  val shell = new GraphInterpreterShell(connections = null, logics = null, effectiveAttributes, materializer)

  override def name: String = "Fusing GraphStages phase"

  override def materializeAtomic(mod: AtomicModule[Shape, Any], attributes: Attributes): (GraphStageLogic, Any) = {
    // TODO: bail on unknown types
    val stageModule = mod.asInstanceOf[GraphStageModule[Shape, Any]]
    val stage = stageModule.stage
    val matAndLogic = stage.createLogicAndMaterializedValue(attributes, materializer)
    val logic = matAndLogic._1
    logic.originalStage = OptionVal.Some(stage)
    logic.attributes = attributes
    logics.add(logic)
    logic.stageId = logics.size() - 1
    fullIslandName match {
      case OptionVal.None => fullIslandName = OptionVal.Some(islandName + "-" + logic.attributes.nameForActorRef())
      case _              => // already set
    }
    matAndLogic
  }

  def conn(slot: Int): Connection = {
    maxConnections = math.max(slot, maxConnections)
    if (maxConnections >= connections.length) {
      connections = java.util.Arrays.copyOf(connections, connections.length * 2)
    }
    val c = connections(slot)
    if (c ne null) c
    else {
      val c2 = new Connection(0, null, null, null, null)
      connections(slot) = c2
      c2
    }
  }

  def outConn(): Connection = {
    val connection = new Connection(0, null, null, null, null)
    outConnections ::= connection
    connection
  }

  override def assignPort(in: InPort, slot: Int, logic: GraphStageLogic): Unit = {
    val connection = conn(slot)
    connection.inOwner = logic
    connection.id = slot
    connection.inHandler = logic.handlers(in.id).asInstanceOf[InHandler]
    if (connection.inHandler eq null) failOnMissingHandler(logic)
    logic.portToConn(in.id) = connection
  }

  override def assignPort(out: OutPort, slot: Int, logic: GraphStageLogic): Unit = {
    val connection = conn(slot)
    connection.outOwner = logic
    connection.id = slot
    connection.outHandler = logic.handlers(logic.inCount + out.id).asInstanceOf[OutHandler]
    if (connection.outHandler eq null) failOnMissingHandler(logic)
    logic.portToConn(logic.inCount + out.id) = connection
  }

  override def createPublisher(out: OutPort, logic: GraphStageLogic): Publisher[Any] = {
    val boundary = new ActorOutputBoundary(shell, out.toString)
    logics.add(boundary)
    boundary.stageId = logics.size() - 1
    boundary.attributes = logic.attributes.and(DefaultAttributes.outputBoundary)

    val connection = outConn()
    boundary.portToConn(boundary.in.id) = connection
    connection.inHandler = boundary.handlers(0).asInstanceOf[InHandler]
    connection.inOwner = boundary

    connection.outOwner = logic
    connection.id = -1 // Will be filled later
    connection.outHandler = logic.handlers(logic.inCount + out.id).asInstanceOf[OutHandler]
    if (connection.outHandler eq null) failOnMissingHandler(logic)
    logic.portToConn(logic.inCount + out.id) = connection

    boundary.publisher
  }

  override def takePublisher(slot: Int, publisher: Publisher[Any], attributes: Attributes): Unit = {
    val connection = conn(slot)
    val bufferSize = connection.inOwner.attributes.mandatoryAttribute[InputBuffer].max
    val boundary =
      new BatchingActorInputBoundary(bufferSize, shell, publisher, "publisher.in")
    logics.add(boundary)
    boundary.stageId = logics.size() - 1
    boundary.attributes = connection.inOwner.attributes.and(DefaultAttributes.inputBoundary)

    boundary.portToConn(boundary.out.id + boundary.inCount) = connection
    connection.outHandler = boundary.handlers(0).asInstanceOf[OutHandler]
    connection.outOwner = boundary
  }

  override def onIslandReady(): Unit = {

    val totalConnections = maxConnections + outConnections.size + 1
    val finalConnections = java.util.Arrays.copyOf(connections, totalConnections)

    var i = maxConnections + 1
    var outConns = outConnections
    while (i < totalConnections) {
      val conn = outConns.head
      outConns = outConns.tail
      if (conn.inHandler eq null) failOnMissingHandler(conn.inOwner)
      else if (conn.outHandler eq null) failOnMissingHandler(conn.outOwner)
      finalConnections(i) = conn
      conn.id = i
      i += 1
    }

    shell.connections = finalConnections
    shell.logics = logics.toArray(GraphStageIsland.logicArrayType)

    subflowFuser match {
      case OptionVal.Some(fuseIntoExistingInterpreter) =>
        fuseIntoExistingInterpreter(shell)

      case _ =>
        val props = ActorGraphInterpreter
          .props(shell)
          .withDispatcher(effectiveAttributes.mandatoryAttribute[ActorAttributes.Dispatcher].dispatcher)
          .withMailbox(PhasedFusingActorMaterializer.Mailbox)

        val actorName = fullIslandName match {
          case OptionVal.Some(n) => n
          case _                 => islandName
        }

        val ref = materializer.actorOf(props, actorName)
        if (PhasedFusingActorMaterializer.Debug) {
          println(s"Spawned actor [$ref] with shell: $shell")
        }
    }
  }

  private def failOnMissingHandler(logic: GraphStageLogic): Unit = {
    val missingHandlerIdx = logic.handlers.indexWhere(_.asInstanceOf[AnyRef] eq null)
    val isIn = missingHandlerIdx < logic.inCount
    val portLabel = logic.originalStage match {
      case OptionVal.Some(stage) =>
        if (isIn) s"in port [${stage.shape.inlets(missingHandlerIdx)}]"
        else s"out port [${stage.shape.outlets(missingHandlerIdx - logic.inCount)}"
      case _ =>
        if (isIn) s"in port id [$missingHandlerIdx]"
        else s"out port id [$missingHandlerIdx]"
    }
    throw new IllegalStateException(s"No handler defined in stage [${logic.toString}] for $portLabel." +
    " All inlets and outlets must be assigned a handler with setHandler in the constructor of your graph stage logic.")
  }

  override def toString: String = "GraphStagePhase"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SourceModuleIslandTag extends IslandTag

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class SourceModulePhase(
    materializer: PhasedFusingActorMaterializer,
    islandName: String)
    extends PhaseIsland[Publisher[Any]] {
  override def name: String = "SourceModule phase"

  override def materializeAtomic(mod: AtomicModule[Shape, Any], attributes: Attributes): (Publisher[Any], Any) = {
    mod
      .asInstanceOf[SourceModule[Any, Any]]
      .create(MaterializationContext(materializer, attributes, islandName + "-" + attributes.nameOrDefault()))
  }

  override def assignPort(in: InPort, slot: Int, logic: Publisher[Any]): Unit = ()

  override def assignPort(out: OutPort, slot: Int, logic: Publisher[Any]): Unit = ()

  override def createPublisher(out: OutPort, logic: Publisher[Any]): Publisher[Any] = logic

  override def takePublisher(slot: Int, publisher: Publisher[Any], attributes: Attributes): Unit =
    throw new UnsupportedOperationException("A Source cannot take a Publisher")

  override def onIslandReady(): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SinkModuleIslandTag extends IslandTag

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class SinkModulePhase(materializer: PhasedFusingActorMaterializer, islandName: String)
    extends PhaseIsland[AnyRef] {
  override def name: String = "SinkModule phase"
  private var subscriberOrVirtualPublisher: AnyRef = _

  override def materializeAtomic(mod: AtomicModule[Shape, Any], attributes: Attributes): (AnyRef, Any) = {
    val subAndMat =
      mod
        .asInstanceOf[SinkModule[Any, Any]]
        .create(MaterializationContext(materializer, attributes, islandName + "-" + attributes.nameOrDefault()))

    subscriberOrVirtualPublisher = subAndMat._1
    (subscriberOrVirtualPublisher, subAndMat._2)
  }

  override def assignPort(in: InPort, slot: Int, logic: AnyRef): Unit = ()

  override def assignPort(out: OutPort, slot: Int, logic: AnyRef): Unit = ()

  override def createPublisher(out: OutPort, logic: AnyRef): Publisher[Any] = {
    throw new UnsupportedOperationException("A Sink cannot create a Publisher")
  }

  override def takePublisher(slot: Int, publisher: Publisher[Any], attributes: Attributes): Unit = {
    subscriberOrVirtualPublisher match {
      case v: VirtualPublisher[_]        => v.registerPublisher(publisher)
      case s: Subscriber[Any] @unchecked => publisher.subscribe(s)
      case _                             => throw new IllegalStateException() // won't happen, compiler exhaustiveness check pleaser
    }
  }

  override def onIslandReady(): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ProcessorModuleIslandTag extends IslandTag

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class ProcessorModulePhase() extends PhaseIsland[Processor[Any, Any]] {
  override def name: String = "ProcessorModulePhase"
  private[this] var processor: Processor[Any, Any] = _

  override def materializeAtomic(mod: AtomicModule[Shape, Any], attributes: Attributes): (Processor[Any, Any], Any) = {
    val procAndMat = mod.asInstanceOf[ProcessorModule[Any, Any, Any]].createProcessor()
    processor = procAndMat._1
    procAndMat
  }

  override def assignPort(in: InPort, slot: Int, logic: Processor[Any, Any]): Unit = ()
  override def assignPort(out: OutPort, slot: Int, logic: Processor[Any, Any]): Unit = ()

  override def createPublisher(out: OutPort, logic: Processor[Any, Any]): Publisher[Any] = logic
  override def takePublisher(slot: Int, publisher: Publisher[Any], attributes: Attributes): Unit =
    publisher.subscribe(processor)

  override def onIslandReady(): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TlsModuleIslandTag extends IslandTag

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class TlsModulePhase(materializer: PhasedFusingActorMaterializer, islandName: String)
    extends PhaseIsland[NotUsed] {
  def name: String = "TlsModulePhase"

  var tlsActor: ActorRef = _
  var publishers: Vector[ActorPublisher[Any]] = _

  def materializeAtomic(mod: AtomicModule[Shape, Any], attributes: Attributes): (NotUsed, Any) = {
    val tls = mod.asInstanceOf[TlsModule]

    val dispatcher = attributes.mandatoryAttribute[ActorAttributes.Dispatcher].dispatcher
    val maxInputBuffer = attributes.mandatoryAttribute[Attributes.InputBuffer].max

    val props =
      TLSActor
        .props(maxInputBuffer, tls.createSSLEngine, tls.verifySession, tls.closing)
        .withDispatcher(dispatcher)
        .withMailbox(PhasedFusingActorMaterializer.Mailbox)
    tlsActor = materializer.actorOf(props, "TLS-for-" + islandName)
    def factory(id: Int) = new ActorPublisher[Any](tlsActor) {
      override val wakeUpMsg = FanOut.SubstreamSubscribePending(id)
    }
    publishers = Vector.tabulate(2)(factory)
    tlsActor ! FanOut.ExposedPublishers(publishers)
    (NotUsed, NotUsed)
  }
  def assignPort(in: InPort, slot: Int, logic: NotUsed): Unit = ()
  def assignPort(out: OutPort, slot: Int, logic: NotUsed): Unit = ()

  def createPublisher(out: OutPort, logic: NotUsed): Publisher[Any] =
    publishers(out.id)

  override def takePublisher(slot: Int, publisher: Publisher[Any], attributes: Attributes): Unit =
    publisher.subscribe(FanIn.SubInput[Any](tlsActor, 1 - slot))

  def onIslandReady(): Unit = ()
}
