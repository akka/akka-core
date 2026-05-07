/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.implicitNotFound
import scala.concurrent.{ ExecutionContextExecutor, Future, Promise }
import scala.util.control.NonFatal

import akka.ConfigurationException
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.dispatch.{ Mailboxes, RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.dispatch.Dispatchers
import akka.dispatch.sysmsg._
import akka.event._
import akka.routing._
import akka.serialization.Serialization
import akka.util.Collections.EmptyImmutableSeq
import akka.util.Helpers
import akka.util.OptionVal

/**
 * Interface for all ActorRef providers to implement.
 * Not intended for extension outside of Akka.
 */
@DoNotInherit trait ActorRefProvider {

  /**
   * Reference to the supervisor of guardian and systemGuardian; this is
   * exposed so that the ActorSystemImpl can use it as lookupRoot, i.e.
   * for anchoring absolute actor look-ups.
   */
  def rootGuardian: InternalActorRef

  /**
   * Reference to the supervisor of guardian and systemGuardian at the specified address;
   * this is exposed so that the ActorRefFactory can use it as lookupRoot, i.e.
   * for anchoring absolute actor selections.
   */
  def rootGuardianAt(address: Address): ActorRef

  /**
   * Reference to the supervisor used for all top-level user actors.
   */
  def guardian: LocalActorRef

  /**
   * Reference to the supervisor used for all top-level system actors.
   */
  def systemGuardian: LocalActorRef

  /**
   * Dead letter destination for this provider.
   */
  def deadLetters: ActorRef

  /** INTERNAL API */
  @InternalApi private[akka] def ignoreRef: ActorRef

  /**
   * The root path for all actors within this actor system, not including any remote address information.
   */
  def rootPath: ActorPath

  /**
   * The Settings associated with this ActorRefProvider
   */
  def settings: ActorSystem.Settings

  /**
   * INTERNAL API: Initialization of an ActorRefProvider happens in two steps: first
   * construction of the object with settings, eventStream, etc.
   * and then—when the ActorSystem is constructed—the second phase during
   * which actors may be created (e.g. the guardians).
   */
  private[akka] def init(system: ActorSystemImpl): Unit

  /**
   * The Deployer associated with this ActorRefProvider
   */
  def deployer: Deployer

  /**
   * Generates and returns a unique actor path below “/temp”.
   */
  def tempPath(): ActorPath

  /**
   * Generates and returns a unique actor path starting with `prefix` below “/temp”.
   */
  def tempPath(prefix: String): ActorPath

  /**
   * Returns the actor reference representing the “/temp” path.
   */
  def tempContainer: InternalActorRef

  /**
   * INTERNAL API: Registers an actorRef at a path returned by tempPath(); do NOT pass in any other path.
   */
  private[akka] def registerTempActor(actorRef: InternalActorRef, path: ActorPath): Unit

  /**
   * Unregister a temporary actor from the “/temp” path (i.e. obtained from tempPath()); do NOT pass in any other path.
   */
  def unregisterTempActor(path: ActorPath): Unit

  /**
   * INTERNAL API: Actor factory with create-only semantics: will create an actor as
   * described by props with the given supervisor and path (may be different
   * in case of remote supervision). If systemService is true, deployment is
   * bypassed (local-only). If ``Some(deploy)`` is passed in, it should be
   * regarded as taking precedence over the nominally applicable settings,
   * but it should be overridable from external configuration; the lookup of
   * the latter can be suppressed by setting ``lookupDeploy`` to ``false``.
   */
  private[akka] def actorOf(
      system: ActorSystemImpl,
      props: Props,
      supervisor: InternalActorRef,
      path: ActorPath,
      systemService: Boolean,
      deploy: Option[Deploy],
      lookupDeploy: Boolean,
      async: Boolean): InternalActorRef

  /**
   * Create actor reference for a specified path. If no such
   * actor exists, it will be (equivalent to) a dead letter reference.
   */
  def resolveActorRef(path: String): ActorRef

  /**
   * Create actor reference for a specified path. If no such
   * actor exists, it will be (equivalent to) a dead letter reference.
   */
  def resolveActorRef(path: ActorPath): ActorRef

  /**
   * This Future is completed upon termination of this ActorRefProvider, which
   * is usually initiated by stopping the guardian via ActorSystem.stop().
   */
  def terminationFuture: Future[Terminated]

  /**
   * Obtain the address which is to be used within sender references when
   * sending to the given other address or none if the other address cannot be
   * reached from this system (i.e. no means of communication known; no
   * attempt is made to verify actual reachability).
   */
  def getExternalAddressFor(addr: Address): Option[Address]

  /**
   * Obtain the external address of the default transport.
   */
  def getDefaultAddress: Address

  /** INTERNAL API */
  @InternalApi private[akka] def serializationInformation: Serialization.Information

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def addressString: String

  def systemUid: Long

}

/**
 * Interface implemented by ActorSystem and ActorContext, the only two places
 * from which you can get fresh actors.
 *
 * Not for user extension
 */
@DoNotInherit
@implicitNotFound(
  "implicit ActorRefFactory required: if outside of an Actor you need an implicit ActorSystem, inside of an actor this should be the implicit ActorContext")
trait ActorRefFactory {

  /**
   * INTERNAL API
   */
  protected def systemImpl: ActorSystemImpl

  /**
   * INTERNAL API
   */
  protected def provider: ActorRefProvider

  /**
   * Returns the default MessageDispatcher associated with this ActorRefFactory
   */
  implicit def dispatcher: ExecutionContextExecutor

  /**
   * Parent of all children created by this interface.
   *
   * INTERNAL API
   */
  protected def guardian: InternalActorRef

  /**
   * INTERNAL API
   */
  protected def lookupRoot: InternalActorRef

  /**
   * Create new actor as child of this context and give it an automatically
   * generated name (currently similar to base64-encoded integer count,
   * reversed and with “$” prepended, may change in the future).
   *
   * See [[akka.actor.Props]] for details on how to obtain a `Props` object.
   *
   * @throws akka.ConfigurationException if deployment, dispatcher
   *   or mailbox configuration is wrong
   * @throws UnsupportedOperationException if invoked on an ActorSystem that
   *   uses a custom user guardian
   */
  def actorOf(props: Props): ActorRef

  /**
   * Create new actor as child of this context with the given name, which must
   * not be null, empty or start with “$”. If the given name is already in use,
   * an `InvalidActorNameException` is thrown.
   *
   * See [[akka.actor.Props]] for details on how to obtain a `Props` object.
   *
   * @throws akka.actor.InvalidActorNameException if the given name is
   *   invalid or already in use
   * @throws akka.ConfigurationException if deployment, dispatcher
   *   or mailbox configuration is wrong
   * @throws UnsupportedOperationException if invoked on an ActorSystem that
   *   uses a custom user guardian
   */
  def actorOf(props: Props, name: String): ActorRef

  /**
   * Construct an [[akka.actor.ActorSelection]] from the given path, which is
   * parsed for wildcards (these are replaced by regular expressions
   * internally). No attempt is made to verify the existence of any part of
   * the supplied path, it is recommended to send a message and gather the
   * replies in order to resolve the matching set of actors.
   */
  def actorSelection(path: String): ActorSelection = path match {
    case RelativeActorPath(elems) =>
      if (elems.isEmpty) ActorSelection(provider.deadLetters, "")
      else if (elems.head.isEmpty) ActorSelection(provider.rootGuardian, elems.tail)
      else ActorSelection(lookupRoot, elems)
    case ActorPathExtractor(address, elems) =>
      ActorSelection(provider.rootGuardianAt(address), elems)
    case _ =>
      ActorSelection(provider.deadLetters, "")
  }

  /**
   * Construct an [[akka.actor.ActorSelection]] from the given path, which is
   * parsed for wildcards (these are replaced by regular expressions
   * internally). No attempt is made to verify the existence of any part of
   * the supplied path, it is recommended to send a message and gather the
   * replies in order to resolve the matching set of actors.
   */
  def actorSelection(path: ActorPath): ActorSelection =
    ActorSelection(provider.rootGuardianAt(path.address), path.elements)

  /**
   * Stop the actor pointed to by the given [[akka.actor.ActorRef]]; this is
   * an asynchronous operation, i.e. involves a message send.
   * If this method is applied to the `self` reference from inside an Actor
   * then that Actor is guaranteed to not process any further messages after
   * this call; please note that the processing of the current message will
   * continue, this method does not immediately terminate this actor.
   */
  def stop(actor: ActorRef): Unit
}

/**
 * Internal Akka use only, used in implementation of system.stop(child).
 */
@InternalApi
private[akka] final case class StopChild(child: ActorRef)

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object SystemGuardian {

  /**
   * For the purpose of orderly shutdown it's possible
   * to register interest in the termination of systemGuardian
   * and receive a notification `TerminationHook`
   * before systemGuardian is stopped. The registered hook is supposed
   * to reply with `TerminationHookDone` and the
   * systemGuardian will not stop until all registered hooks have replied.
   */
  case object RegisterTerminationHook
  case object TerminationHook
  case object TerminationHookDone
}

private[akka] object LocalActorRefProvider {

  /*
   * Root and user guardian
   */
  private class Guardian(override val supervisorStrategy: SupervisorStrategy)
      extends Actor
      with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

    def receive = {
      case Terminated(_)    => context.stop(self)
      case StopChild(child) => context.stop(child)
    }

    // guardian MUST NOT lose its children during restart
    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {}
  }

  /**
   * System guardian
   */
  private class SystemGuardian(override val supervisorStrategy: SupervisorStrategy, val guardian: ActorRef)
      extends Actor
      with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
    import akka.actor.SystemGuardian._

    var terminationHooks = Set.empty[ActorRef]

    def receive = {
      case Terminated(`guardian`) =>
        // time for the systemGuardian to stop, but first notify all the
        // termination hooks, they will reply with TerminationHookDone
        // and when all are done the systemGuardian is stopped
        context.become(terminating)
        terminationHooks.foreach { _ ! TerminationHook }
        stopWhenAllTerminationHooksDone()
      case Terminated(a) =>
        // a registered, and watched termination hook terminated before
        // termination process of guardian has started
        terminationHooks -= a
      case StopChild(child) => context.stop(child)
      case RegisterTerminationHook if sender() != context.system.deadLetters =>
        terminationHooks += sender()
        context.watch(sender())
    }

    def terminating: Receive = {
      case Terminated(a)       => stopWhenAllTerminationHooksDone(a)
      case TerminationHookDone => stopWhenAllTerminationHooksDone(sender())
    }

    def stopWhenAllTerminationHooksDone(remove: ActorRef): Unit = {
      terminationHooks -= remove
      stopWhenAllTerminationHooksDone()
    }

    def stopWhenAllTerminationHooksDone(): Unit =
      if (terminationHooks.isEmpty) {
        context.system.eventStream.stopDefaultLoggers(context.system)
        context.stop(self)
      }

    // guardian MUST NOT lose its children during restart
    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {}
  }

}

/**
 * Local ActorRef provider.
 *
 * INTERNAL API!
 *
 * Depending on this class is not supported, only the [[ActorRefProvider]] interface is supported.
 */
private[akka] class LocalActorRefProvider private[akka] (
    _systemName: String,
    override val settings: ActorSystem.Settings,
    val eventStream: EventStream,
    val dynamicAccess: DynamicAccess,
    override val deployer: Deployer,
    _deadLetters: Option[ActorPath => InternalActorRef])
    extends ActorRefProvider {

  // this is the constructor needed for reflectively instantiating the provider
  def this(
      _systemName: String,
      settings: ActorSystem.Settings,
      eventStream: EventStream,
      dynamicAccess: DynamicAccess) =
    this(_systemName, settings, eventStream, dynamicAccess, new Deployer(settings, dynamicAccess), None)

  override val rootPath: ActorPath = RootActorPath(Address("akka", _systemName))

  private[akka] val log: MarkerLoggingAdapter =
    Logging.withMarker(eventStream, classOf[LocalActorRefProvider])

  /*
   * This dedicated logger is used whenever a deserialization failure occurs
   * and can therefore be disabled/enabled independently
   */
  private[akka] val logDeser: MarkerLoggingAdapter =
    Logging.withMarker(eventStream, getClass.getName + ".Deserialization")

  override val deadLetters: InternalActorRef =
    _deadLetters
      .getOrElse((p: ActorPath) => new DeadLetterActorRef(this, p, eventStream))
      .apply(rootPath / "deadLetters")

  override val ignoreRef: ActorRef = new IgnoreActorRef(this)

  private[this] final val terminationPromise: Promise[Terminated] = Promise[Terminated]()

  def terminationFuture: Future[Terminated] = terminationPromise.future

  /*
   * generate name for temporary actor refs
   */
  private val tempNumber = new AtomicLong

  private val tempNode = rootPath / "temp"

  override def tempPath(): ActorPath = tempPath("")

  override def tempPath(prefix: String): ActorPath = {
    val builder = new java.lang.StringBuilder()
    if (prefix.nonEmpty) {
      builder.append(prefix)
    }
    builder.append("$")
    Helpers.base64(tempNumber.getAndIncrement(), builder)
    tempNode / builder.toString
  }

  /**
   * Top-level anchor for the supervision hierarchy of this actor system. Will
   * receive only Supervise/ChildTerminated system messages or Failure message.
   */
  private[akka] val theOneWhoWalksTheBubblesOfSpaceTime: InternalActorRef = new MinimalActorRef {
    val causeOfTermination: Promise[Terminated] = Promise[Terminated]()

    val path = rootPath / "bubble-walker"

    def provider: ActorRefProvider = LocalActorRefProvider.this

    def isWalking = !causeOfTermination.future.isCompleted

    override def stop(): Unit = {
      causeOfTermination.trySuccess(
        Terminated(provider.rootGuardian)(existenceConfirmed = true, addressTerminated = true)) //Idempotent
      terminationPromise.completeWith(causeOfTermination.future) // Signal termination downstream, idempotent
    }

    /**
     * INTERNAL API
     */
    @InternalApi
    override private[akka] def isTerminated: Boolean = !isWalking

    override def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit =
      if (isWalking)
        message match {
          case null => throw InvalidMessageException("Message is null")
          case _    => log.error(s"$this received unexpected message [$message]")
        }

    override def sendSystemMessage(message: SystemMessage): Unit = if (isWalking) {
      message match {
        case Failed(child: InternalActorRef, ex, _) =>
          log.error(ex, s"guardian $child failed, shutting down!")
          causeOfTermination.tryFailure(ex)
          child.stop()
        case Supervise(_, _)           => // TODO register child in some map to keep track of it and enable shutdown after all dead
        case _: DeathWatchNotification => stop()
        case _                         => log.error(s"$this received unexpected system message [$message]")
      }
    }
  }

  /*
   * The problem is that ActorRefs need a reference to the ActorSystem to
   * provide their service. Hence they cannot be created while the
   * constructors of ActorSystem and ActorRefProvider are still running.
   * The solution is to split out that last part into an init() method,
   * but it also requires these references to be @volatile and lazy.
   */
  @volatile
  private var system: ActorSystemImpl = _

  @volatile
  private var extraNames: Map[String, InternalActorRef] = Map()

  /**
   * Higher-level providers (or extensions) might want to register new synthetic
   * top-level paths for doing special stuff. This is the way to do just that.
   * Just be careful to complete all this before ActorSystem.start() finishes,
   * or before you start your own auto-spawned actors.
   */
  def registerExtraNames(_extras: Map[String, InternalActorRef]): Unit = extraNames ++= _extras

  private def guardianSupervisorStrategyConfigurator =
    dynamicAccess
      .createInstanceFor[SupervisorStrategyConfigurator](settings.SupervisorStrategyClass, EmptyImmutableSeq)
      .get

  /**
   * Overridable supervision strategy to be used by the “/user” guardian.
   */
  protected def rootGuardianStrategy: SupervisorStrategy = OneForOneStrategy() {
    case ex =>
      log.error(ex, "guardian failed, shutting down system")
      SupervisorStrategy.Stop
  }

  /**
   * Overridable supervision strategy to be used by the “/user” guardian.
   */
  protected def guardianStrategy: SupervisorStrategy = guardianSupervisorStrategyConfigurator.create()

  /**
   * Overridable supervision strategy to be used by the “/user” guardian.
   */
  protected def systemGuardianStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  private def internalDispatcher = system.dispatchers.internalDispatcher

  private lazy val defaultMailbox = system.mailboxes.lookup(Mailboxes.DefaultMailboxId)

  override lazy val rootGuardian: LocalActorRef =
    new LocalActorRef(
      system,
      Props(new LocalActorRefProvider.Guardian(rootGuardianStrategy)),
      internalDispatcher,
      defaultMailbox,
      theOneWhoWalksTheBubblesOfSpaceTime,
      rootPath) {
      override def getParent: InternalActorRef = this
      override def getSingleChild(name: String): InternalActorRef = name match {
        case "temp"        => tempContainer
        case "deadLetters" => deadLetters
        case other         => extraNames.getOrElse(other, super.getSingleChild(other))
      }
    }

  override def rootGuardianAt(address: Address): ActorRef =
    if (address == rootPath.address) rootGuardian
    else deadLetters

  override lazy val guardian: LocalActorRef = {
    val cell = rootGuardian.underlying
    cell.reserveChild("user")
    // make user provided guardians not run on internal dispatcher
    val dispatcher =
      system.guardianProps match {
        case None => internalDispatcher
        case Some(props) =>
          val dispatcherId =
            if (props.deploy.dispatcher == Deploy.DispatcherSameAsParent) Dispatchers.DefaultDispatcherId
            else props.dispatcher
          system.dispatchers.lookup(dispatcherId)
      }
    val ref = new LocalActorRef(
      system,
      system.guardianProps.getOrElse(Props(new LocalActorRefProvider.Guardian(guardianStrategy))),
      dispatcher,
      system.guardianProps match {
        case Some(props) => system.mailboxes.lookup(props.mailbox)
        case None        => defaultMailbox
      },
      rootGuardian,
      rootPath / "user")
    cell.initChild(ref)
    ref.start()
    ref
  }

  override lazy val systemGuardian: LocalActorRef = {
    val cell = rootGuardian.underlying
    cell.reserveChild("system")
    val ref = new LocalActorRef(
      system,
      Props(new LocalActorRefProvider.SystemGuardian(systemGuardianStrategy, guardian)),
      internalDispatcher,
      defaultMailbox,
      rootGuardian,
      rootPath / "system")
    cell.initChild(ref)
    ref.start()
    ref
  }

  lazy val tempContainer: VirtualPathContainer = new VirtualPathContainer(system.provider, tempNode, rootGuardian, log)

  def registerTempActor(actorRef: InternalActorRef, path: ActorPath): Unit = {
    assert(path.parent eq tempNode, "cannot registerTempActor() with anything not obtained from tempPath()")
    tempContainer.addChild(path.name, actorRef)
  }

  def unregisterTempActor(path: ActorPath): Unit = {
    assert(path.parent eq tempNode, "cannot unregisterTempActor() with anything not obtained from tempPath()")
    tempContainer.removeChild(path.name)
  }

  private[akka] def init(_system: ActorSystemImpl): Unit = {
    system = _system
    rootGuardian.start()
    // chain death watchers so that killing guardian stops the application
    systemGuardian.sendSystemMessage(Watch(guardian, systemGuardian))
    rootGuardian.sendSystemMessage(Watch(systemGuardian, rootGuardian))
    eventStream.startDefaultLoggers(_system)
  }

  def resolveActorRef(path: String): ActorRef = path match {
    case ActorPathExtractor(address, elems) if address == rootPath.address => resolveActorRef(rootGuardian, elems)
    case _ =>
      logDeser.debug("Resolve (deserialization) of unknown (invalid) path [{}], using deadLetters.", path)
      deadLetters
  }

  def resolveActorRef(path: ActorPath): ActorRef = {
    if (path.root == rootPath) resolveActorRef(rootGuardian, path.elements)
    else {
      logDeser.debug(
        "Resolve (deserialization) of foreign path [{}] doesn't match root path [{}], using deadLetters.",
        path,
        rootPath)
      deadLetters
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] def resolveActorRef(ref: InternalActorRef, pathElements: Iterable[String]): InternalActorRef =
    if (pathElements.isEmpty) {
      logDeser.debug("Resolve (deserialization) of empty path doesn't match an active actor, using deadLetters.")
      deadLetters
    } else
      ref.getChild(pathElements.iterator) match {
        case Nobody =>
          if (log.isDebugEnabled)
            logDeser.debug(
              "Resolve (deserialization) of path [{}] doesn't match an active actor. " +
              "It has probably been stopped, using deadLetters.",
              pathElements.mkString("/"))
          new EmptyLocalActorRef(system.provider, ref.path / pathElements, eventStream)
        case x => x
      }

  def actorOf(
      system: ActorSystemImpl,
      props: Props,
      supervisor: InternalActorRef,
      path: ActorPath,
      systemService: Boolean,
      deploy: Option[Deploy],
      lookupDeploy: Boolean,
      async: Boolean): InternalActorRef = {
    props.deploy.routerConfig match {
      case NoRouter =>
        if (settings.DebugRouterMisconfiguration) {
          deployer.lookup(path).foreach { d =>
            if (d.routerConfig != NoRouter)
              log.warning(
                "Configuration says that [{}] should be a router, but code disagrees. Remove the config or add a routerConfig to its Props.",
                path)
          }
        }

        def parentDispatcher: String = supervisor match {
          case withCell: ActorRefWithCell => withCell.underlying.props.dispatcher
          case _                          => Deploy.NoDispatcherGiven
        }

        val props2 =
          // mailbox and dispatcher defined in deploy should override props
          (if (lookupDeploy) deployer.lookup(path) else deploy) match {
            case Some(d) =>
              (d.dispatcher, d.mailbox) match {
                case (Deploy.NoDispatcherGiven, Deploy.NoMailboxGiven)      => props
                case (Deploy.DispatcherSameAsParent, Deploy.NoMailboxGiven) => props.withDispatcher(parentDispatcher)
                case (dsp, Deploy.NoMailboxGiven)                           => props.withDispatcher(dsp)
                case (Deploy.NoDispatcherGiven, mbx)                        => props.withMailbox(mbx)
                case (Deploy.DispatcherSameAsParent, mbx)                   => props.withDispatcher(parentDispatcher).withMailbox(mbx)
                case (dsp, mbx)                                             => props.withDispatcher(dsp).withMailbox(mbx)
              }
            case _ =>
              // no deployment config found
              if (props.deploy.dispatcher == Deploy.DispatcherSameAsParent)
                props.withDispatcher(parentDispatcher)
              else
                props
          }

        if (!system.dispatchers.hasDispatcher(props2.dispatcher))
          throw new ConfigurationException(s"Dispatcher [${props2.dispatcher}] not configured for path $path")

        try {
          val dispatcher = system.dispatchers.lookup(props2.dispatcher)
          val mailboxType = system.mailboxes.getMailboxType(props2, dispatcher.configurator.config)

          if (async)
            new RepointableActorRef(system, props2, dispatcher, mailboxType, supervisor, path).initialize(async)
          else new LocalActorRef(system, props2, dispatcher, mailboxType, supervisor, path)
        } catch {
          case NonFatal(e) =>
            throw new ConfigurationException(
              s"configuration problem while creating [$path] with dispatcher [${props2.dispatcher}] and mailbox [${props2.mailbox}]",
              e)
        }

      case router =>
        val lookup = if (lookupDeploy) deployer.lookup(path) else None
        val r = (router :: deploy.map(_.routerConfig).toList ::: lookup.map(_.routerConfig).toList).reduce((a, b) =>
          b.withFallback(a))
        val p = props.withRouter(r)

        if (!system.dispatchers.hasDispatcher(p.dispatcher))
          throw new ConfigurationException(s"Dispatcher [${p.dispatcher}] not configured for routees of $path")
        if (!system.dispatchers.hasDispatcher(r.routerDispatcher))
          throw new ConfigurationException(s"Dispatcher [${p.dispatcher}] not configured for router of $path")

        val routerProps = Props(
          p.deploy.copy(dispatcher = p.routerConfig.routerDispatcher),
          classOf[RoutedActorCell.RouterActorCreator],
          Vector(p.routerConfig))
        val routeeProps = p.withRouter(NoRouter)

        try {
          val routerDispatcher = system.dispatchers.lookup(p.routerConfig.routerDispatcher)
          val routerMailbox = system.mailboxes.getMailboxType(routerProps, routerDispatcher.configurator.config)

          // routers use context.actorOf() to create the routees, which does not allow us to pass
          // these through, but obtain them here for early verification
          val routeeDispatcher = system.dispatchers.lookup(p.dispatcher)
          system.mailboxes.getMailboxType(routeeProps, routeeDispatcher.configurator.config)

          new RoutedActorRef(system, routerProps, routerDispatcher, routerMailbox, routeeProps, supervisor, path)
            .initialize(async)
        } catch {
          case NonFatal(e) =>
            throw new ConfigurationException(
              s"configuration problem while creating [$path] with router dispatcher [${routerProps.dispatcher}] and mailbox [${routerProps.mailbox}] " +
              s"and routee dispatcher [${routeeProps.dispatcher}] and mailbox [${routeeProps.mailbox}]",
              e)
        }
    }
  }

  def getExternalAddressFor(addr: Address): Option[Address] = if (addr == rootPath.address) Some(addr) else None

  def getDefaultAddress: Address = rootPath.address

  // no need for volatile, only intended as cached value, not necessarily a singleton value
  private var serializationInformationCache: OptionVal[Serialization.Information] = OptionVal.None
  @InternalApi override private[akka] def serializationInformation: Serialization.Information = {
    Serialization.Information(getDefaultAddress, system)
    serializationInformationCache match {
      case OptionVal.Some(info) => info
      case _ =>
        if (system eq null)
          throw new IllegalStateException("Too early access of serializationInformation")
        else {
          val info = Serialization.Information(rootPath.address, system)
          serializationInformationCache = OptionVal.Some(info)
          info
        }
    }
  }

  // lazily initialized with fallback since it can depend on transport which is not initialized up front
  // worth caching since if it is used once in a system it will very likely be used many times
  @volatile private var _addressString: OptionVal[String] = OptionVal.None
  override private[akka] def addressString: String = {
    _addressString match {
      case OptionVal.Some(addr) => addr
      case _ =>
        val addr = getDefaultAddress.toString
        _addressString = OptionVal.Some(addr)
        addr
    }
  }

  override val systemUid: Long =
    ThreadLocalRandom.current.nextLong()
}
