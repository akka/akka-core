/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.annotation.tailrec

import com.typesafe.config._

import akka.annotation.InternalApi
import akka.routing._
import akka.util.WildcardIndex

object Deploy {
  final val NoDispatcherGiven = ""
  final val NoMailboxGiven = ""
  val local = Deploy(scope = LocalScope)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final val DispatcherSameAsParent = ".."

  def apply(
      path: String = "",
      config: Config = ConfigFactory.empty,
      routerConfig: RouterConfig = NoRouter,
      scope: Scope = NoScopeGiven,
      dispatcher: String = Deploy.NoDispatcherGiven,
      mailbox: String = Deploy.NoMailboxGiven): Deploy =
    new Deploy(path, config, routerConfig, scope, dispatcher, mailbox, Set.empty)

  // for bincomp, pre 2.6 was case class
  def unapply(deploy: Deploy): Option[(String, Config, RouterConfig, Scope, String, String)] =
    Some((deploy.path, deploy.config, deploy.routerConfig, deploy.scope, deploy.dispatcher, deploy.mailbox))
}

/**
 * This class represents deployment configuration for a given actor path. It is
 * marked final in order to guarantee stable merge semantics (i.e. what
 * overrides what in case multiple configuration sources are available) and is
 * fully extensible via its Scope argument, and by the fact that an arbitrary
 * Config section can be passed along with it (which will be merged when merging
 * two Deploys).
 *
 * The path field is used only when inserting the Deploy into a deployer and
 * not needed when just doing deploy-as-you-go:
 *
 * {{{
 * val remoteProps = someProps.withDeploy(Deploy(scope = RemoteScope("someOtherNodeName")))
 * }}}
 */
@SerialVersionUID(3L)
final class Deploy(
    val path: String = "",
    val config: Config = ConfigFactory.empty,
    val routerConfig: RouterConfig = NoRouter,
    val scope: Scope = NoScopeGiven,
    val dispatcher: String = Deploy.NoDispatcherGiven,
    val mailbox: String = Deploy.NoMailboxGiven,
    val tags: Set[String] = Set.empty)
    extends Serializable
    with Product
    with Equals {

  // for bincomp, pre 2.6 did not have tags
  def this(
      path: String,
      config: Config,
      routerConfig: RouterConfig,
      scope: Scope,
      dispatcher: String,
      mailbox: String) = this(path, config, routerConfig, scope, dispatcher, mailbox, Set.empty)

  /**
   * Java API to create a Deploy with the given RouterConfig
   */
  def this(routing: RouterConfig) = this("", ConfigFactory.empty, routing)

  /**
   * Java API to create a Deploy with the given RouterConfig with Scope
   */
  def this(routing: RouterConfig, scope: Scope) = this("", ConfigFactory.empty, routing, scope)

  /**
   * Java API to create a Deploy with the given Scope
   */
  def this(scope: Scope) = this("", ConfigFactory.empty, NoRouter, scope)

  /**
   * Do a merge between this and the other Deploy, where values from “this” take
   * precedence. The “path” of the other Deploy is not taken into account. All
   * other members are merged using `X.withFallback(other.X)`.
   */
  def withFallback(other: Deploy): Deploy = {
    Deploy(
      path,
      config.withFallback(other.config),
      routerConfig.withFallback(other.routerConfig),
      scope.withFallback(other.scope),
      if (dispatcher == Deploy.NoDispatcherGiven) other.dispatcher else dispatcher,
      if (mailbox == Deploy.NoMailboxGiven) other.mailbox else mailbox)
  }

  def withTags(tags: Set[String]): Deploy =
    new Deploy(path, config, routerConfig, scope, dispatcher, mailbox, tags)

  // below are for bincomp, pre 2.6 was case class
  def copy(
      path: String = path,
      config: Config = config,
      routerConfig: RouterConfig = routerConfig,
      scope: Scope = scope,
      dispatcher: String = dispatcher,
      mailbox: String = mailbox): Deploy =
    new Deploy(path, config, routerConfig, scope, dispatcher, mailbox, tags)

  override def productElement(n: Int): Any = n match {
    case 0 => path
    case 1 => config
    case 2 => routerConfig
    case 3 => scope
    case 4 => dispatcher
    case 5 => mailbox
    case 6 => tags
  }

  override def productArity: Int = 7

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Deploy]

  override def equals(other: Any): Boolean = other match {
    case that: Deploy =>
      path == that.path &&
      config == that.config &&
      routerConfig == that.routerConfig &&
      scope == that.scope &&
      dispatcher == that.dispatcher &&
      mailbox == that.mailbox &&
      tags == that.tags
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq[AnyRef](path, config, routerConfig, scope, dispatcher, mailbox, tags)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Deploy($path, $config, $routerConfig, $scope, $dispatcher, $mailbox, $tags)"
}

/**
 * The scope of a [[akka.actor.Deploy]] serves two purposes: as a marker for
 * pattern matching the “scope” (i.e. local/remote/cluster) as well as for
 * extending the information carried by the final Deploy class. Scopes can be
 * used in conjunction with a custom [[akka.actor.ActorRefProvider]], making
 * Akka actors fully extensible.
 */
trait Scope {

  /**
   * When merging [[akka.actor.Deploy]] instances using ``withFallback()`` on
   * the left one, this is propagated to “merging” scopes in the same way.
   * The setup is biased towards preferring the callee over the argument, i.e.
   * ``a.withFallback(b)`` is called expecting that ``a`` should in general take
   * precedence.
   */
  def withFallback(other: Scope): Scope
}

@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
abstract class LocalScope extends Scope

/**
 * The Local Scope is the default one, which is assumed on all deployments
 * which do not set a different scope. It is also the only scope handled by
 * the LocalActorRefProvider.
 */
@SerialVersionUID(1L)
case object LocalScope extends LocalScope {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this

  def withFallback(other: Scope): Scope = this
}

/**
 * This is the default value and as such allows overrides.
 */
@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
abstract class NoScopeGiven extends Scope
@SerialVersionUID(1L)
case object NoScopeGiven extends NoScopeGiven {
  def withFallback(other: Scope): Scope = other

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Deployer maps actor paths to actor deployments.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] class Deployer(val settings: ActorSystem.Settings, val dynamicAccess: DynamicAccess) {

  import scala.jdk.CollectionConverters._

  private val resizerEnabled: Config = ConfigFactory.parseString("resizer.enabled=on")
  private val deployments = new AtomicReference(WildcardIndex[Deploy]())
  private val config = settings.config.getConfig("akka.actor.deployment")
  protected val default = config.getConfig("default")
  val routerTypeMapping: Map[String, String] =
    settings.config
      .getConfig("akka.actor.router.type-mapping")
      .root
      .unwrapped
      .asScala
      .collect {
        case (key, value: String) => (key -> value)
      }
      .toMap

  config.root.asScala
    .flatMap {
      case ("default", _)             => None
      case (key, value: ConfigObject) => parseConfig(key, value.toConfig)
      case _                          => None
    }
    .foreach(deploy)

  def lookup(path: ActorPath): Option[Deploy] = lookup(path.elements.drop(1))

  def lookup(path: Iterable[String]): Option[Deploy] = deployments.get().find(path)

  def deploy(d: Deploy): Unit = {
    @tailrec def add(path: Array[String], d: Deploy): Unit = {
      val w: WildcardIndex[Deploy] = deployments.get
      for (i <- path.indices) path(i) match {
        case "" => throw InvalidActorNameException(s"Actor name in deployment [${d.path}] must not be empty")
        case el => ActorPath.validatePathElement(el, fullPath = d.path)
      }

      if (!deployments.compareAndSet(w, w.insert(path, d))) add(path, d)
    }

    add(d.path.split("/").drop(1), d)
  }

  def parseConfig(key: String, config: Config): Option[Deploy] = {
    val deployment = config.withFallback(default)
    val router = createRouterConfig(deployment.getString("router"), key, config, deployment)
    val dispatcher = deployment.getString("dispatcher")
    val mailbox = deployment.getString("mailbox")
    Some(Deploy(key, deployment, router, NoScopeGiven, dispatcher, mailbox))
  }

  /**
   * Factory method for creating `RouterConfig`
   * @param routerType the configured name of the router, or FQCN
   * @param key the full configuration key of the deployment section
   * @param config the user defined config of the deployment, without defaults
   * @param deployment the deployment config, with defaults
   */
  protected def createRouterConfig(routerType: String, key: String, config: Config, deployment: Config): RouterConfig =
    if (routerType == "from-code") NoRouter
    else {
      // need this for backwards compatibility, resizer enabled when including (parts of) resizer section in the deployment
      val deployment2 =
        if (config.hasPath("resizer") && !deployment.getBoolean("resizer.enabled"))
          resizerEnabled.withFallback(deployment)
        else deployment

      val fqn = routerTypeMapping.getOrElse(routerType, routerType)

      def throwCannotInstantiateRouter(args: Seq[(Class[_], AnyRef)], cause: Throwable) =
        throw new IllegalArgumentException(
          s"Cannot instantiate router [$fqn], defined in [$key], " +
          s"make sure it extends [${classOf[RouterConfig]}] and has constructor with " +
          s"[${args(0)._1.getName}] and optional [${args(1)._1.getName}] parameter",
          cause)

      // first try with Config param, and then with Config and DynamicAccess parameters
      val args1 = List(classOf[Config] -> deployment2)
      val args2 = List(classOf[Config] -> deployment2, classOf[DynamicAccess] -> dynamicAccess)
      dynamicAccess
        .createInstanceFor[RouterConfig](fqn, args1)
        .recover {
          case e @ (_: IllegalArgumentException | _: ConfigException) => throw e
          case e: NoSuchMethodException =>
            dynamicAccess
              .createInstanceFor[RouterConfig](fqn, args2)
              .recover {
                case e2 @ (_: IllegalArgumentException | _: ConfigException) => throw e2
                case _                                                       => throwCannotInstantiateRouter(args2, e)
              }
              .get
          case e => throwCannotInstantiateRouter(args2, e)
        }
        .get
    }

}
