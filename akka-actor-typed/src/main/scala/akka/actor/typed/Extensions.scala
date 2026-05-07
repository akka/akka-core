/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import akka.actor.setup.Setup
import akka.annotation.DoNotInherit

/**
 * Marker trait/interface for extensions. An extension can be registered in the ActorSystem and is guaranteed to only
 * have one instance per [[ActorSystem]] instance per [[ExtensionId]]. The extension internals must be thread safe.
 * For mutable state it should be preferred to use an `Actor` rather than extensions as first choice.
 *
 * @see [[ExtensionId]]
 */
trait Extension

/**
 * Identifier and factory for an extension. Is used to look up an extension from the `ActorSystem`, and possibly create
 * an instance if no instance was already registered. The extension can also be listed in the actor system configuration
 * to have it eagerly loaded and registered on actor system startup.
 *
 * *Scala API*
 *
 * The `ExtensionId` for an extension written in Scala is best done by letting it be the companion object of the
 * extension. If the extension will be used from Java special care needs to be taken to provide a `get` method with the
 * concrete extension type as return (as this will not be inferred correctly by the Java compiler with the default
 * implementation)
 *
 * Example:
 *
 * {{{
 * object MyExt extends ExtensionId[Ext] {
 *
 *   override def createExtension(system: ActorSystem[_]): MyExt = new MyExt(system)
 *
 *   // Java API: retrieve the extension instance for the given system.
 *   def get(system: ActorSystem[_]): MyExt = apply(system)
 * }
 *
 * class MyExt(system: ActorSystem[_]) extends Extension {
 *   ...
 * }
 *
 * // can be loaded eagerly on system startup through configuration
 * // note that the name is the JVM/Java class name, with a dollar sign in the end
 * // and not the Scala object name
 * akka.actor.typed.extensions = ["com.example.MyExt$"]
 *
 * // Allows access like this from Scala
 * MyExt().someMethodOnTheExtension()
 * // and from Java
 * MyExt.get(system).someMethodOnTheExtension()
 * }}}
 *
 * *Java API*
 *
 * To implement an extension in Java you should first create an `ExtensionId` singleton by implementing a static method
 * called `getInstance`, this is needed to be able to list the extension among the `akka.actor.typed.extensions` in the configuration
 * and have it loaded when the actor system starts up.
 *
 * {{{
 *
 * public class MyExt extends AbstractExtensionId<MyExtImpl> {
 *   // single instance of the identifier
 *   private final static MyExt instance = new MyExt();
 *
 *   // protect against other instances than the singleton
 *   private MyExt() {}
 *
 *   // This static method singleton accessor is needed to be able to enable the extension through config when
 *   // implementing extensions in Java.
 *   public static MyExt getInstance() {
 *     return instance;
 *   }
 *
 *   public MyExtImpl createExtension(ActorSystem<?> system) {
 *     return new MyExtImpl();
 *   }
 *
 *   // convenience accessor
 *   public static MyExtImpl get(ActorSystem<?> system) {
 *      return instance.apply(system);
 *   }
 * }
 *
 * public class MyExtImpl implements Extension {
 *    ...
 * }
 *
 * // can be loaded eagerly on system startup through configuration
 * akka.actor.typed.extensions = ["com.example.MyExt"]
 *
 * // Allows access like this from Scala
 * MyExt.someMethodOnTheExtension()
 * // and from Java
 * MyExt.get(system).someMethodOnTheExtension()
 * }}}
 *
 * For testing purposes extensions typically provide a concrete [[ExtensionSetup]]
 * that can be used in [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]]
 * to replace the default implementation of the extension.
 *
 * @tparam T The concrete extension type
 * @see [[ExtensionSetup]]
 */
abstract class ExtensionId[T <: Extension] {

  /**
   * Create the extension, will be invoked at most one time per actor system where the extension is registered.
   */
  def createExtension(system: ActorSystem[_]): T

  /**
   * Lookup or create an instance of the extension identified by this id.
   */
  final def apply(system: ActorSystem[_]): T = system.registerExtension(this)

  override final def hashCode: Int = System.identityHashCode(this)
  override final def equals(other: Any): Boolean = this eq other.asInstanceOf[AnyRef]

  /**
   * Java API: The identifier of the extension
   */
  def id: ExtensionId[T] = this
}

/**
 * API for registering and looking up extensions.
 *
 * Not for user extension.
 */
@DoNotInherit
trait Extensions {

  /**
   * Registers the provided extension and creates its payload, if this extension isn't already registered
   * This method has putIfAbsent-semantics, this method can potentially block, waiting for the initialization
   * of the payload, if is in the process of registration from another Thread of execution
   */
  def registerExtension[T <: Extension](ext: ExtensionId[T]): T

  /**
   * Returns the payload that is associated with the provided extension
   * throws an IllegalStateException if it is not registered.
   * This method can potentially block, waiting for the initialization
   * of the payload, if is in the process of registration from another Thread of execution
   */
  def extension[T <: Extension](ext: ExtensionId[T]): T

  /**
   * Returns whether the specified extension is already registered, this method can potentially block, waiting for the initialization
   * of the payload, if is in the process of registration from another Thread of execution
   */
  def hasExtension(ext: ExtensionId[_ <: Extension]): Boolean
}

/**
 * Each extension typically provide a concrete `ExtensionSetup` that can be used in
 * [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]] to replace the default
 * implementation of the extension. Intended for tests that need to replace
 * extension with stub/mock implementations.
 */
abstract class ExtensionSetup[T <: Extension](
    val extId: ExtensionId[T],
    val createExtension: java.util.function.Function[ActorSystem[_], T])
    extends Setup

/**
 * Scala 2.11 API: Each extension typically provide a concrete `ExtensionSetup` that can be used in
 * [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]] to replace the default
 * implementation of the extension. Intended for tests that need to replace
 * extension with stub/mock implementations.
 */
abstract class AbstractExtensionSetup[T <: Extension](extId: ExtensionId[T], createExtension: ActorSystem[_] => T)
    extends ExtensionSetup[T](extId, createExtension.apply)
