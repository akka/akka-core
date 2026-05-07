/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

/**
 * The basic ActorSystem covers all that is needed for locally running actors,
 * using futures and so on. In addition, more features can hook into it and
 * thus become visible to actors et al by registering themselves as extensions.
 * This is accomplished by providing an extension—which is an object
 * implementing this trait—to `ActorSystem.registerExtension(...)` or by
 * specifying the corresponding option in the configuration passed to
 * ActorSystem, which will then instantiate (without arguments) each FQCN and
 * register the result.
 *
 * The extension itself can be created in any way desired and has full access
 * to the ActorSystem implementation.
 *
 * This trait is only a marker interface to signify an Akka Extension. This is
 * how an extension is normally constructed.
 *
 * Scala API:
 *
 * {{{
 * object MyExt extends ExtensionId[Ext] with ExtensionIdProvider {
 *
 *   override def lookup = MyExt
 *
 *   override def createExtension(system: ExtendedActorSystem): Ext = new Ext(system)
 *
 *   // Java API: retrieve the extension for the given system.
 *   override def get(system: ActorSystem): UdpExt = super.get(system)
 *   override def get(system: ClassicActorSystemProvider): UdpExt = super.get(system)
 * }
 *
 * class Ext(system: ExtendedActorSystem) extends Extension {
 *   ...
 * }
 * }}}
 *
 * Java API:
 *
 * {{{
 * public class MyExt extends AbstractExtensionId<MyExtImpl>
 *   implements ExtensionIdProvider {
 *   public final static MyExt MyExtProvider = new MyExt();
 *
 *   private MyExt() {}
 *
 *   public MyExt lookup() {
 *     return MyExt.MyExtProvider;
 *   }
 *
 *   public MyExtImpl createExtension(ExtendedActorSystem system) {
 *     return new MyExtImpl();
 *   }
 * }
 *
 * public class MyExtImpl implements Extension {
 *    ...
 * }
 * }}}
 */
trait Extension

/**
 * Identifies an Extension
 * Lookup of Extensions is done by object identity, so the Id must be the same wherever it's used,
 * otherwise you'll get the same extension loaded multiple times.
 */
trait ExtensionId[T <: Extension] {

  /**
   * Returns an instance of the extension identified by this ExtensionId instance.
   */
  def apply(system: ActorSystem): T = {
    java.util.Objects.requireNonNull(system, "system must not be null!").registerExtension(this)
  }

  /**
   * Returns an instance of the extension identified by this ExtensionId instance.
   */
  def apply(system: ClassicActorSystemProvider): T = apply(system.classicSystem)

  /**
   * Returns an instance of the extension identified by this ExtensionId instance.
   * Java API
   * For extensions written in Scala that are to be used from Java also,
   * this method should be overridden to get correct return type.
   * {{{
   * override def get(system: ActorSystem): TheExtension = super.get(system)
   * }}}
   *
   */
  def get(system: ActorSystem): T = apply(system)

  /**
   * Returns an instance of the extension identified by this ExtensionId instance.
   * Java API
   * For extensions written in Scala that are to be used from Java also,
   * this method should be overridden to get correct return type.
   * {{{
   * override def get(system: ClassicActorSystemProvider): TheExtension = super.get(system)
   * }}}
   *
   */
  def get(system: ClassicActorSystemProvider): T = apply(system)

  /**
   * Is used by Akka to instantiate the Extension identified by this ExtensionId,
   * internal use only.
   */
  def createExtension(system: ExtendedActorSystem): T

  override final def hashCode: Int = System.identityHashCode(this)
  override final def equals(other: Any): Boolean = this eq other.asInstanceOf[AnyRef]
}

/**
 * Java API for ExtensionId
 */
abstract class AbstractExtensionId[T <: Extension] extends ExtensionId[T]

/**
 * To be able to load an ExtensionId from the configuration,
 * a class that implements ExtensionIdProvider must be specified.
 * The lookup method should return the canonical reference to the extension.
 */
trait ExtensionIdProvider {

  /**
   * Returns the canonical ExtensionId for this Extension
   */
  def lookup: ExtensionId[_ <: Extension]
}
