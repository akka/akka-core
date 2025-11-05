/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.actor

import java.io.ObjectStreamException
import java.lang.reflect.{ InvocationHandler, InvocationTargetException, Method, Proxy }
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{ AtomicReference => AtomVar }

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import akka.dispatch._
import akka.japi.{ Creator, Option => JOption }
import akka.japi.Util.{ immutableSeq, immutableSingletonSeq }
import akka.pattern.AskTimeoutException
import akka.serialization.{ JavaSerializer, SerializationExtension, Serializers }
import akka.util.Reflect.instantiator
import akka.util.Timeout

/**
 * A TypedActorFactory is something that can created TypedActor instances.
 */
@deprecated("Use 'akka.actor.typed' API.", since = "2.6.0")
trait TypedActorFactory {

  /**
   * Underlying dependency is to be able to create normal Actors
   */
  protected def actorFactory: ActorRefFactory

  /**
   * Underlying dependency to a TypedActorExtension, which can either be contextual or ActorSystem "global"
   */
  protected def typedActor: TypedActorExtension

  /**
   * Stops the underlying ActorRef for the supplied TypedActor proxy,
   * if any, returns whether it could find the find the ActorRef or not
   */
  def stop(proxy: AnyRef): Boolean = getActorRefFor(proxy) match {
    case null => false
    case ref  => ref.asInstanceOf[InternalActorRef].stop(); true
  }

  /**
   * Sends a PoisonPill the underlying ActorRef for the supplied TypedActor proxy,
   * if any, returns whether it could find the find the ActorRef or not
   */
  def poisonPill(proxy: AnyRef): Boolean = getActorRefFor(proxy) match {
    case null => false
    case ref  => ref ! PoisonPill; true
  }

  /**
   * Returns whether the supplied AnyRef is a TypedActor proxy or not
   */
  def isTypedActor(proxyOrNot: AnyRef): Boolean

  /**
   * Retrieves the underlying ActorRef for the supplied TypedActor proxy, or null if none found
   */
  def getActorRefFor(proxy: AnyRef): ActorRef

  /**
   * Creates a new TypedActor with the specified properties
   */
  def typedActorOf[R <: AnyRef, T <: R](props: TypedProps[T]): R = {
    val proxyVar = new AtomVar[R] //Chicken'n'egg-resolver
    val c = props.creator //Cache this to avoid closing over the Props
    val i = props.interfaces //Cache this to avoid closing over the Props
    val ap = Props(new TypedActor.TypedActor[R, T](proxyVar, c(), i)).withDeploy(props.actorProps().deploy)
    typedActor.createActorRefProxy(props, proxyVar, actorFactory.actorOf(ap))
  }

  /**
   * Creates a new TypedActor with the specified properties
   */
  def typedActorOf[R <: AnyRef, T <: R](props: TypedProps[T], name: String): R = {
    val proxyVar = new AtomVar[R] //Chicken'n'egg-resolver
    val c = props.creator //Cache this to avoid closing over the Props
    val i = props.interfaces //Cache this to avoid closing over the Props
    val ap = Props(new akka.actor.TypedActor.TypedActor[R, T](proxyVar, c(), i)).withDeploy(props.actorProps().deploy)
    typedActor.createActorRefProxy(props, proxyVar, actorFactory.actorOf(ap, name))
  }

  /**
   * Creates a TypedActor that intercepts the calls and forwards them as [[akka.actor.TypedActor.MethodCall]]
   * to the provided ActorRef.
   */
  def typedActorOf[R <: AnyRef, T <: R](props: TypedProps[T], actorRef: ActorRef): R =
    typedActor.createActorRefProxy(props, null: AtomVar[R], actorRef)

}

/**
 * This represents the TypedActor Akka Extension, access to the functionality is done through a given ActorSystem.
 */
@deprecated("Use 'akka.actor.typed' API.", since = "2.6.0")
object TypedActor extends ExtensionId[TypedActorExtension] with ExtensionIdProvider {
  override def get(system: ActorSystem): TypedActorExtension = super.get(system)
  override def get(system: ClassicActorSystemProvider): TypedActorExtension = super.get(system)

  def lookup = this
  def createExtension(system: ExtendedActorSystem): TypedActorExtension = new TypedActorExtension(system)

  /**
   * Returns a contextual TypedActorFactory of this extension, this means that any TypedActors created by this TypedActorExtension
   * will be children to the specified context, this allows for creating hierarchies of TypedActors.
   * Do _not_ let this instance escape the TypedActor since that will not be thread-safe.
   */
  @deprecated("Use 'akka.actor.typed' API.", since = "2.6.0")
  def apply(context: ActorContext): TypedActorFactory = ContextualTypedActorFactory(apply(context.system), context)

  /**
   * Returns a contextual TypedActorFactory of this extension, this means that any TypedActors created by this TypedActorExtension
   * will be children to the specified context, this allows for creating hierarchies of TypedActors.
   * Do _not_ let this instance escape the TypedActor since that will not be thread-safe.
   *
   * Java API
   */
  @nowarn("msg=deprecated")
  def get(context: ActorContext): TypedActorFactory = apply(context)

  /**
   * This class represents a Method call, and has a reference to the Method to be called and the parameters to supply
   * It's sent to the ActorRef backing the TypedActor and can be serialized and deserialized
   */
  final case class MethodCall(method: Method, parameters: Array[AnyRef]) {

    def isOneWay = method.getReturnType == java.lang.Void.TYPE
    def returnsFuture = classOf[Future[_]].isAssignableFrom(method.getReturnType)
    def returnsJOption = classOf[akka.japi.Option[_]].isAssignableFrom(method.getReturnType)
    def returnsOption = classOf[scala.Option[_]].isAssignableFrom(method.getReturnType)

    /**
     * Invokes the Method on the supplied instance
     *
     * Throws the underlying exception if there's an InvocationTargetException thrown on the invocation.
     */
    def apply(instance: AnyRef): AnyRef =
      try {
        parameters match {
          case null                     => method.invoke(instance)
          case args if args.length == 0 => method.invoke(instance)
          case args                     => method.invoke(instance, args: _*)
        }
      } catch { case i: InvocationTargetException => throw i.getTargetException }

    @throws(classOf[ObjectStreamException]) private def writeReplace(): AnyRef = parameters match {
      case null => SerializedMethodCall(method.getDeclaringClass, method.getName, method.getParameterTypes, null)
      case ps if ps.length == 0 =>
        SerializedMethodCall(method.getDeclaringClass, method.getName, method.getParameterTypes, Array())
      case ps =>
        val serialization = SerializationExtension(akka.serialization.JavaSerializer.currentSystem.value)
        val serializedParameters = new Array[(Int, String, Array[Byte])](ps.length)
        for (i <- 0 until ps.length) {
          val p = ps(i)
          val s = serialization.findSerializerFor(p)
          val m = Serializers.manifestFor(s, p)
          serializedParameters(i) = (s.identifier, m, s.toBinary(parameters(i))) //Mutable for the sake of sanity
        }

        SerializedMethodCall(method.getDeclaringClass, method.getName, method.getParameterTypes, serializedParameters)
    }
  }

  /**
   * INTERNAL API
   *
   * Represents the serialized form of a MethodCall, uses readResolve and writeReplace to marshall the call
   */
  private[akka] final case class SerializedMethodCall(
      ownerType: Class[_],
      methodName: String,
      parameterTypes: Array[Class[_]],
      serializedParameters: Array[(Int, String, Array[Byte])]) {

    //TODO implement writeObject and readObject to serialize
    //TODO Possible optimization is to special encode the parameter-types to conserve space
    @throws(classOf[ObjectStreamException]) private def readResolve(): AnyRef = {
      val system = akka.serialization.JavaSerializer.currentSystem.value
      if (system eq null)
        throw new IllegalStateException(
          "Trying to deserialize a SerializedMethodCall without an ActorSystem in scope." +
          " Use akka.serialization.JavaSerializer.currentSystem.withValue(system) { ... }")
      val serialization = SerializationExtension(system)
      MethodCall(
        ownerType.getDeclaredMethod(methodName, parameterTypes: _*),
        serializedParameters match {
          case null               => null
          case a if a.length == 0 => Array[AnyRef]()
          case a =>
            val deserializedParameters: Array[AnyRef] = new Array[AnyRef](a.length) //Mutable for the sake of sanity
            for (i <- 0 until a.length) {
              val (sId, manifest, bytes) = a(i)
              deserializedParameters(i) = serialization.deserialize(bytes, sId, manifest).get
            }

            deserializedParameters
        })
    }
  }

  private val selfReference = new ThreadLocal[AnyRef]
  private val currentContext = new ThreadLocal[ActorContext]

  @SerialVersionUID(1L)
  private case object NullResponse

  /**
   * Returns the reference to the proxy when called inside a method call in a TypedActor
   *
   * Example:
   * <p/>
   * class FooImpl extends Foo {
   *   def doFoo {
   *     val myself = TypedActor.self[Foo]
   *   }
   * }
   *
   * Useful when you want to send a reference to this TypedActor to someone else.
   *
   * NEVER EXPOSE "this" to someone else, always use "self[TypeOfInterface(s)]"
   *
   * Throws IllegalStateException if called outside of the scope of a method on this TypedActor.
   *
   * Throws ClassCastException if the supplied type T isn't the type of the proxy associated with this TypedActor.
   */
  def self[T <: AnyRef] = selfReference.get.asInstanceOf[T] match {
    case null =>
      throw new IllegalStateException("Calling TypedActor.self outside of a TypedActor implementation method!")
    case some => some
  }

  /**
   * Returns the ActorContext (for a TypedActor) when inside a method call in a TypedActor.
   */
  def context: ActorContext = currentContext.get match {
    case null =>
      throw new IllegalStateException("Calling TypedActor.context outside of a TypedActor implementation method!")
    case some => some
  }

  /**
   * Returns the default dispatcher (for a TypedActor) when inside a method call in a TypedActor.
   */
  implicit def dispatcher: ExecutionContextExecutor = context.dispatcher

  /**
   * INTERNAL API
   *
   * Implementation of TypedActor as an Actor
   */
  private[akka] class TypedActor[R <: AnyRef, T <: R](
      val proxyVar: AtomVar[R],
      createInstance: => T,
      interfaces: immutable.Seq[Class[_]])
      extends Actor {
    self =>
    // if we were remote deployed we need to create a local proxy
    if (!self.context.parent.asInstanceOf[InternalActorRef].isLocal)
      akka.actor.TypedActor
        .get(self.context.system)
        .createActorRefProxy(TypedProps(interfaces, createInstance), proxyVar, self.context.self)

    private val me = withContext[T](createInstance)

    override def supervisorStrategy: SupervisorStrategy = me match {
      case l: Supervisor => l.supervisorStrategy()
      case _             => super.supervisorStrategy
    }

    override def preStart(): Unit = withContext {
      me match {
        case l: PreStart => l.preStart()
        case _           => super.preStart()
      }
    }

    @nowarn("msg=deprecated")
    override def postStop(): Unit =
      try {
        withContext {
          me match {
            case l: PostStop => l.postStop()
            case _           => super.postStop()
          }
        }
      } finally {
        akka.actor.TypedActor(self.context.system).invocationHandlerFor(proxyVar.get) match {
          case null =>
          case some =>
            some.actorVar.set(self.context.system.deadLetters) //Point it to the DLQ
            proxyVar.set(null.asInstanceOf[R])
        }
      }

    override def preRestart(reason: Throwable, message: Option[Any]): Unit = withContext {
      me match {
        case l: PreRestart => l.preRestart(reason, message)
        case _ =>
          self.context.children
            .foreach(self.context.stop) //Can't be super.preRestart(reason, message) since that would invoke postStop which would set the actorVar to DL and proxyVar to null
      }
    }

    override def postRestart(reason: Throwable): Unit = withContext {
      me match {
        case l: PostRestart => l.postRestart(reason)
        case _              => super.postRestart(reason)
      }
    }

    protected def withContext[U](unitOfWork: => U): U = {
      akka.actor.TypedActor.selfReference.set(proxyVar.get)
      akka.actor.TypedActor.currentContext.set(self.context)
      try unitOfWork
      finally {
        akka.actor.TypedActor.selfReference.set(null)
        akka.actor.TypedActor.currentContext.set(null)
      }
    }

    def receive = {
      case m: MethodCall =>
        withContext {
          if (m.isOneWay) m(me)
          else {
            try {
              val s = sender()
              m(me) match {
                case f: Future[_] if m.returnsFuture =>
                  implicit val dispatcher = this.context.dispatcher
                  f.onComplete {
                    case Success(result) =>
                      if (result == null) {
                        s ! NullResponse
                      } else {
                        s ! result
                      }
                    case Failure(f) => s ! Status.Failure(f)
                  }
                case null   => s ! NullResponse
                case result => s ! result
              }
            } catch {
              case NonFatal(e) =>
                sender() ! Status.Failure(e)
                throw e
            }
          }
        }

      case msg if me.isInstanceOf[Receiver] =>
        withContext {
          me.asInstanceOf[Receiver].onReceive(msg, sender())
        }
    }
  }

  /**
   * Mix this into your TypedActor to be able to define supervisor strategy
   */
  trait Supervisor {

    /**
     * User overridable definition the strategy to use for supervising
     * child actors.
     */
    def supervisorStrategy(): SupervisorStrategy
  }

  /**
   * Mix this into your TypedActor to be able to intercept Terminated messages
   */
  trait Receiver {
    def onReceive(message: Any, sender: ActorRef): Unit
  }

  /**
   * Mix this into your TypedActor to be able to hook into its lifecycle
   */
  trait PreStart {

    /**
     * User overridable callback.
     * <p/>
     * Is called when an Actor is started by invoking 'actor'.
     */
    def preStart(): Unit
  }

  /**
   * Mix this into your TypedActor to be able to hook into its lifecycle
   */
  trait PostStop {

    /**
     * User overridable callback.
     * <p/>
     * Is called when 'actor.stop()' is invoked.
     */
    def postStop(): Unit
  }

  /**
   * Mix this into your TypedActor to be able to hook into its lifecycle
   */
  trait PreRestart {

    /**
     * User overridable callback: '''By default it disposes of all children and then calls `postStop()`.'''
     * @param reason the Throwable that caused the restart to happen
     * @param message optionally the current message the actor processed when failing, if applicable
     * <p/>
     * Is called on a crashed Actor right BEFORE it is restarted to allow clean
     * up of resources before Actor is terminated.
     * By default it terminates all children and calls postStop()
     */
    def preRestart(reason: Throwable, message: Option[Any]): Unit
  }

  trait PostRestart {

    /**
     * User overridable callback: By default it calls `preStart()`.
     * @param reason the Throwable that caused the restart to happen
     * <p/>
     * Is called right AFTER restart on the newly created Actor to allow reinitialization after an Actor crash.
     */
    def postRestart(reason: Throwable): Unit
  }

  /**
   * INTERNAL API
   */
  private[akka] class TypedActorInvocationHandler(
      @transient val extension: TypedActorExtension,
      @transient val actorVar: AtomVar[ActorRef],
      @transient val timeout: Timeout)
      extends InvocationHandler
      with Serializable {

    def actor = actorVar.get
    @throws(classOf[Throwable])
    def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
      case "toString" => actor.toString
      case "equals" =>
        (args.length == 1 && (proxy eq args(0)) || actor == extension.getActorRefFor(args(0)))
          .asInstanceOf[AnyRef] //Force boxing of the boolean
      case "hashCode" => actor.hashCode.asInstanceOf[AnyRef]
      case _ =>
        implicit val dispatcher = extension.system.dispatcher
        import akka.pattern.ask
        MethodCall(method, args) match {
          case m if m.isOneWay =>
            actor ! m; null //Null return value
          case m if m.returnsFuture =>
            ask(actor, m)(timeout).map {
              case NullResponse => null
              case other        => other
            }
          case m if m.returnsJOption || m.returnsOption =>
            val f = ask(actor, m)(timeout)
            (try {
              Await.ready(f, timeout.duration).value
            } catch { case _: TimeoutException => None }) match {
              case None | Some(Success(NullResponse)) | Some(Failure(_: AskTimeoutException)) =>
                if (m.returnsJOption) JOption.none[Any] else None
              case Some(t: Try[_]) =>
                t.get.asInstanceOf[AnyRef]
            }
          case m =>
            Await.result(ask(actor, m)(timeout), timeout.duration) match {
              case NullResponse => null
              case other        => other.asInstanceOf[AnyRef]
            }
        }
    }
    @throws(classOf[ObjectStreamException]) private def writeReplace(): AnyRef =
      SerializedTypedActorInvocationHandler(actor, timeout.duration)
  }

  /**
   * INTERNAL API
   */
  private[akka] final case class SerializedTypedActorInvocationHandler(
      val actor: ActorRef,
      val timeout: FiniteDuration) {
    @throws(classOf[ObjectStreamException]) private def readResolve(): AnyRef =
      JavaSerializer.currentSystem.value match {
        case null =>
          throw new IllegalStateException(
            "SerializedTypedActorInvocationHandler.readResolve requires that " +
            "JavaSerializer.currentSystem.value is set to a non-null value")
        case some => toTypedActorInvocationHandler(some)
      }

    @nowarn("msg=deprecated")
    def toTypedActorInvocationHandler(system: ActorSystem): TypedActorInvocationHandler =
      new TypedActorInvocationHandler(akka.actor.TypedActor(system), new AtomVar[ActorRef](actor), new Timeout(timeout))
  }
}

/**
 * TypedProps is a TypedActor configuration object, that is thread safe and fully sharable.
 * It's used in TypedActorFactory.typedActorOf to configure a TypedActor instance.
 */
object TypedProps {

  val defaultDispatcherId: String = Dispatchers.DefaultDispatcherId
  val defaultTimeout: Option[Timeout] = None
  val defaultLoader: Option[ClassLoader] = None

  /**
   * @return a sequence of interfaces that the specified class implements,
   * or a sequence containing only itself, if itself is an interface.
   */
  def extractInterfaces(clazz: Class[_]): immutable.Seq[Class[_]] =
    if (clazz.isInterface) immutableSingletonSeq(clazz) else immutableSeq(clazz.getInterfaces)

  /**
   * Uses the supplied class as the factory for the TypedActor implementation,
   * proxying all the interfaces it implements.
   *
   * Scala API
   */
  def apply[T <: AnyRef](implementation: Class[T]): TypedProps[T] =
    new TypedProps[T](implementation)

  /**
   * Uses the supplied class as the factory for the TypedActor implementation,
   * and that has the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements,
   * appended in the sequence of interfaces.
   *
   * Scala API
   */
  def apply[T <: AnyRef](interface: Class[_ >: T], implementation: Class[T]): TypedProps[T] =
    new TypedProps[T](extractInterfaces(interface), instantiator(implementation))

  /**
   * Uses the supplied thunk as the factory for the TypedActor implementation,
   * and that has the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements,
   * appended in the sequence of interfaces.
   *
   * Scala API
   */
  def apply[T <: AnyRef](interface: Class[_ >: T], creator: => T): TypedProps[T] =
    new TypedProps[T](extractInterfaces(interface), () => creator)

  /**
   * Uses the supplied class as the factory for the TypedActor implementation,
   * proxying all the interfaces it implements.
   *
   * Scala API
   */
  def apply[T <: AnyRef: ClassTag](): TypedProps[T] =
    new TypedProps[T](implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])

  /**
   * INTERNAL API
   */
  private[akka] def apply[T <: AnyRef](interfaces: immutable.Seq[Class[_]], creator: => T): TypedProps[T] =
    new TypedProps[T](interfaces, () => creator)
}

/**
 * TypedProps is a TypedActor configuration object, that is thread safe and fully sharable.
 * It's used in TypedActorFactory.typedActorOf to configure a TypedActor instance.
 */
@SerialVersionUID(1L)
final case class TypedProps[T <: AnyRef] protected[TypedProps] (
    interfaces: immutable.Seq[Class[_]],
    creator: () => T,
    dispatcher: String = TypedProps.defaultDispatcherId,
    deploy: Deploy = Props.defaultDeploy,
    timeout: Option[Timeout] = TypedProps.defaultTimeout,
    loader: Option[ClassLoader] = TypedProps.defaultLoader) {

  /**
   * Uses the supplied class as the factory for the TypedActor implementation,
   * and that has the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements,
   * appended in the sequence of interfaces.
   */
  def this(implementation: Class[T]) =
    this(interfaces = TypedProps.extractInterfaces(implementation), creator = instantiator(implementation))

  /**
   * Java API: Uses the supplied Creator as the factory for the TypedActor implementation,
   * and that has the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements,
   * appended in the sequence of interfaces.
   */
  def this(interface: Class[_ >: T], implementation: Creator[T]) =
    this(interfaces = TypedProps.extractInterfaces(interface), creator = implementation.create _)

  /**
   * Java API: Uses the supplied class as the factory for the TypedActor implementation,
   * and that has the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements,
   * appended in the sequence of interfaces.
   */
  def this(interface: Class[_ >: T], implementation: Class[T]) =
    this(interfaces = TypedProps.extractInterfaces(interface), creator = instantiator(implementation))

  /**
   * Returns a new TypedProps with the specified dispatcher set.
   */
  def withDispatcher(d: String): TypedProps[T] = copy(dispatcher = d)

  /**
   * Returns a new TypedProps with the specified deployment configuration.
   */
  def withDeploy(d: Deploy): TypedProps[T] = copy(deploy = d)

  /**
   * Java API: return a new TypedProps that will use the specified ClassLoader to create its proxy class in
   * If loader is null, it will use the bootstrap classloader.
   */
  def withLoader(loader: ClassLoader): TypedProps[T] = withLoader(Option(loader))

  /**
   * Scala API: return a new TypedProps that will use the specified ClassLoader to create its proxy class in
   * If loader is null, it will use the bootstrap classloader.
   *
   * Scala API
   */
  def withLoader(loader: Option[ClassLoader]): TypedProps[T] = this.copy(loader = loader)

  /**
   * Java API: return a new TypedProps that will use the specified Timeout for its non-void-returning methods,
   * if null is specified, it will use the default timeout as specified in the configuration.
   */
  def withTimeout(timeout: Timeout): TypedProps[T] = this.copy(timeout = Option(timeout))

  /**
   * Scala API: return a new TypedProps that will use the specified Timeout for its non-void-returning methods,
   * if None is specified, it will use the default timeout as specified in the configuration.
   *
   *
   */
  def withTimeout(timeout: Option[Timeout]): TypedProps[T] = this.copy(timeout = timeout)

  /**
   * Returns a new TypedProps that has the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements,
   * appended in the sequence of interfaces.
   */
  def withInterface(interface: Class[_ >: T]): TypedProps[T] =
    this.copy(interfaces = interfaces ++ TypedProps.extractInterfaces(interface))

  /**
   * Returns a new TypedProps without the specified interface,
   * or if the interface class is not an interface, all the interfaces it implements.
   */
  def withoutInterface(interface: Class[_ >: T]): TypedProps[T] =
    this.copy(interfaces = interfaces.diff(TypedProps.extractInterfaces(interface)))

  /**
   * Returns the akka.actor.Props representation of this TypedProps
   */
  def actorProps(): Props =
    if (dispatcher == Props.default.dispatcher)
      Props.default.withDeploy(deploy)
    else Props.default.withDispatcher(dispatcher).withDeploy(deploy)
}

/**
 * ContextualTypedActorFactory allows TypedActors to create children, effectively forming the same Actor Supervision Hierarchies
 * as normal Actors can.
 */
@nowarn("msg=deprecated")
final case class ContextualTypedActorFactory(typedActor: TypedActorExtension, actorFactory: ActorContext)
    extends TypedActorFactory {
  override def getActorRefFor(proxy: AnyRef): ActorRef = typedActor.getActorRefFor(proxy)
  override def isTypedActor(proxyOrNot: AnyRef): Boolean = typedActor.isTypedActor(proxyOrNot)
}

@nowarn("msg=deprecated")
class TypedActorExtension(val system: ExtendedActorSystem) extends TypedActorFactory with Extension {
  import TypedActor._ //Import the goodies from the companion object
  protected def actorFactory: ActorRefFactory = system
  protected def typedActor = this

  import system.settings

  import akka.util.Helpers.ConfigOps

  /**
   * Default timeout for typed actor methods with non-void return type
   */
  final val DefaultReturnTimeout = Timeout(settings.config.getMillisDuration("akka.actor.typed.timeout"))

  /**
   * Retrieves the underlying ActorRef for the supplied TypedActor proxy, or null if none found
   */
  def getActorRefFor(proxy: AnyRef): ActorRef = invocationHandlerFor(proxy) match {
    case null    => null
    case handler => handler.actor
  }

  /**
   * Returns whether the supplied AnyRef is a TypedActor proxy or not
   */
  def isTypedActor(proxyOrNot: AnyRef): Boolean = invocationHandlerFor(proxyOrNot) ne null

  // Private API
  /**
   * INTERNAL API
   */
  private[akka] def createActorRefProxy[R <: AnyRef, T <: R](
      props: TypedProps[T],
      proxyVar: AtomVar[R],
      actorRef: => ActorRef): R = {
    //Warning, do not change order of the following statements, it's some elaborate chicken-n-egg handling
    val actorVar = new AtomVar[ActorRef](null)
    val proxy = Proxy
      .newProxyInstance(
        props.loader
          .orElse(props.interfaces.collectFirst { case any => any.getClassLoader })
          .orNull, //If we have no loader, we arbitrarily take the loader of the first interface
        props.interfaces.toArray,
        new TypedActorInvocationHandler(this, actorVar, props.timeout.getOrElse(DefaultReturnTimeout)))
      .asInstanceOf[R]

    if (proxyVar eq null) {
      actorVar.set(actorRef)
      proxy
    } else {
      proxyVar.set(proxy) // Chicken and egg situation we needed to solve, set the proxy so that we can set the self-reference inside each receive
      actorVar.set(actorRef) //Make sure the InvocationHandler gets a hold of the actor reference, this is not a problem since the proxy hasn't escaped this method yet
      proxyVar.get
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] def invocationHandlerFor(typedActor: AnyRef): TypedActorInvocationHandler =
    if ((typedActor ne null) && classOf[Proxy].isAssignableFrom(typedActor.getClass) && Proxy.isProxyClass(
          typedActor.getClass)) typedActor match {
      case null => null
      case other =>
        Proxy.getInvocationHandler(other) match {
          case null                                 => null
          case handler: TypedActorInvocationHandler => handler
          case _                                    => null
        }
    } else null
}
