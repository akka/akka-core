/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.japi

import scala.util.control.NonFatal

/**
 * Helper class for determining whether a `Throwable` is fatal or not.
 * User should only catch the non-fatal one,and keep rethrow the fatal one.
 *
 * Fatal errors are errors like `VirtualMachineError`
 * (for example, `OutOfMemoryError` and `StackOverflowError`, subclasses of `VirtualMachineError`), `ThreadDeath`,
 * `LinkageError`, `InterruptedException`, `ControlThrowable`.
 *
 * Note. this helper keep the same semantic with `NonFatal` in Scala.
 * For example, all harmless `Throwable`s can be caught by:
 * {{{
 *   try {
 *     // dangerous stuff
 *   } catch(Throwable e) {
 *     if (Throwables.isNonFatal(e)){
 *       log.error(e, "Something not that bad.");
 *     } else {
 *       throw e;
 *     }
 * }}}
 */
object Throwables {

  /**
   * Returns true if the provided `Throwable` is to be considered non-fatal,
   * or false if it is to be considered fatal
   */
  def isNonFatal(throwable: Throwable): Boolean = NonFatal(throwable)

  /**
   * Returns true if the provided `Throwable` is to be considered fatal,
   * or false if it is to be considered non-fatal
   */
  def isFatal(throwable: Throwable): Boolean = !isNonFatal(throwable)
}
