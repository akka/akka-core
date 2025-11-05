/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.scalatest

import org.scalactic.source.Position
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.verbs.StringVerbBlockRegistration

trait MayVerb {
  import MayVerb._

  /**
   * Configurable number of frames to be shown when a MAY test fails (is canceled).
   *
   * Defaults to `3`.
   * Must be greater than `0`.
   */
  def mayVerbStacktraceContextFrames = 3

  def optional(whenSkippedMessage: String)(body: => Unit): Unit =
    try body
    catch {
      case cause: Throwable =>
        val shortTrace = cause.getStackTrace.take(mayVerbStacktraceContextFrames)
        throw new TestCanceledByFailure(whenSkippedMessage, shortTrace)
    }

  trait StringMayWrapperForVerb {
    val leftSideString: String

    /**
     * Block of tests which MAY pass, and if not should be ignored.
     * Such as rules which may be optionally implemented by Journals.
     *
     * MUST be used in conjunction with [[MayVerb#optional]] to provide explanation as to why it may be ok to fail this spec.
     *
     * The word `MAY` is to be understood as defined in RFC 2119
     *
     * @see <a href="https://www.rfc-editor.org/rfc/rfc2119.txt">RFC 2119</a>
     */
    def may(right: => Unit)(implicit fun: StringVerbBlockRegistration, pos: Position): Unit = {
      fun(leftSideString, "may", pos, () => right)
    }
  }

  import scala.language.implicitConversions

  /**
   * Implicitly converts an object of type <code>String</code> to a <code>StringMayWrapper</code>,
   * to enable <code>may</code> methods to be invokable on that object.
   */
  implicit def convertToStringMayWrapper(o: String): StringMayWrapperForVerb =
    new StringMayWrapperForVerb {
      val leftSideString = o.trim
    }

}

object MayVerb {
  case class TestCanceledByFailure(msg: String, specialStackTrace: Array[StackTraceElement])
      extends TestCanceledException(Some(msg), None, 2) {
    override def getStackTrace = specialStackTrace
  }
}
